import csv
import json
import time
import logging
import urllib.request
import urllib.error
from pathlib import Path
from typing import Any, Iterator

from core.base import Extractor

logger = logging.getLogger(__name__)


class CSVExtractor(Extractor):
    def extract(self) -> Iterator[dict]:
        path = Path(self.config["path"])
        encoding = self.config.get("encoding", "utf-8")
        delimiter = self.config.get("delimiter", ",")
        skip_blank = self.config.get("skip_blank", True)

        logger.info("CSVExtractor: reading %s", path)
        with path.open(encoding=encoding, newline="") as fh:
            reader = csv.DictReader(fh, delimiter=delimiter)
            for i, row in enumerate(reader):
                if skip_blank and not any(row.values()):
                    continue
                yield dict(row)
        logger.info("CSVExtractor: finished %s", path)


class HTTPExtractor(Extractor):
    def extract(self) -> Iterator[dict]:
        url: str = self.config["url"]
        headers: dict = self.config.get("headers", {})
        pagination: str = self.config.get("pagination", "none")
        page_size: int = self.config.get("page_size", 100)
        results_key: str | None = self.config.get("results_key")
        next_key: str = self.config.get("next_key", "next")
        timeout: int = self.config.get("timeout_s", 10)

        if pagination == "offset":
            yield from self._paginate_offset(
                url, headers, page_size, results_key, timeout
            )
        elif pagination == "cursor":
            yield from self._paginate_cursor(
                url, headers, results_key, next_key, timeout
            )
        else:
            data = self._get(url, headers, timeout)
            yield from self._unpack(data, results_key)

    # ------------------------------------------------------------------
    # Pagination strategies
    # ------------------------------------------------------------------

    def _paginate_offset(
        self,
        base_url: str,
        headers: dict,
        page_size: int,
        results_key: str | None,
        timeout: int,
    ) -> Iterator[dict]:
        offset = 0
        while True:
            sep = "&" if "?" in base_url else "?"
            url = f"{base_url}{sep}offset={offset}&limit={page_size}"
            data = self._get(url, headers, timeout)
            records = list(self._unpack(data, results_key))
            if not records:
                break
            yield from records
            if len(records) < page_size:
                break  # last page
            offset += page_size

    def _paginate_cursor(
        self,
        start_url: str,
        headers: dict,
        results_key: str | None,
        next_key: str,
        timeout: int,
    ) -> Iterator[dict]:
        url: str | None = start_url
        while url:
            data = self._get(url, headers, timeout)
            yield from self._unpack(data, results_key)
            # 'next' may be a full URL or null/absent
            url = data.get(next_key) if isinstance(data, dict) else None

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _get(self, url: str, headers: dict, timeout: int) -> Any:
        max_retries = self.config.get("max_retries", 3)
        delay = self.config.get("retry_delay_s", 1.0)

        for attempt in range(1, max_retries + 1):
            try:
                logger.debug("HTTPExtractor GET %s (attempt %d)", url, attempt)
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    return json.loads(resp.read().decode())
            except urllib.error.HTTPError as e:
                if e.code in (429, 500, 502, 503, 504) and attempt < max_retries:
                    wait = delay * (2 ** (attempt - 1))
                    logger.warning(
                        "HTTPExtractor: HTTP %d, retrying in %.1fs", e.code, wait
                    )
                    time.sleep(wait)
                else:
                    raise
            except urllib.error.URLError as e:
                if attempt < max_retries:
                    wait = delay * (2 ** (attempt - 1))
                    logger.warning(
                        "HTTPExtractor: network error %s, retrying in %.1fs", e, wait
                    )
                    time.sleep(wait)
                else:
                    raise

    @staticmethod
    def _unpack(data: Any, results_key: str | None) -> Iterator[dict]:
        """Extract the record list from the response envelope."""
        if results_key:
            data = data[results_key]
        if isinstance(data, list):
            yield from data
        elif isinstance(data, dict):
            yield data
        else:
            raise ValueError(
                f"Expected list or dict from API, got {type(data).__name__}"
            )


class JSONFileExtractor(Extractor):
    def extract(self) -> Iterator[dict]:
        path = Path(self.config["path"])
        records_key = self.config.get("records_key")

        logger.info("JSONFileExtractor: reading %s", path)
        with path.open() as fh:
            data = json.load(fh)

        if records_key:
            data = data[records_key]

        if isinstance(data, list):
            yield from data
        else:
            yield data