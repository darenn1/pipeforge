import time
import logging
import traceback
from dataclasses import dataclass, field
from typing import Optional

from .base import Extractor, Transformer, Loader
from .history import RunHistory

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Result value object
# ------------------------------------------------------------------

@dataclass
class RunResult:
    pipeline: str
    status: str          # 'success' | 'failed'
    records: int = 0
    duration_s: float = 0.0
    attempts: int = 1
    error: Optional[str] = None
    run_ids: list[int] = field(default_factory=list)  # one per attempt

    @property
    def succeeded(self) -> bool:
        return self.status == "success"


# ------------------------------------------------------------------
# Pipeline
# ------------------------------------------------------------------

class Pipeline:
    def __init__(
        self,
        name: str,
        extractor: Extractor,
        transformer: Transformer,
        loader: Loader,
        *,
        max_attempts: int = 3,
        base_delay_s: float = 2.0,
        backoff_factor: float = 2.0,
        history: Optional[RunHistory] = None,
    ):
        self.name = name
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.max_attempts = max_attempts
        self.base_delay_s = base_delay_s
        self.backoff_factor = backoff_factor
        self.history = history or RunHistory()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> RunResult:
        result = RunResult(pipeline=self.name, status="failed")

        for attempt in range(1, self.max_attempts + 1):
            result.attempts = attempt
            run_id = self.history.start_run(self.name, attempt=attempt)
            result.run_ids.append(run_id)

            start = time.monotonic()
            try:
                logger.info(
                    "[%s] attempt %d/%d starting", self.name, attempt, self.max_attempts
                )
                records = self._execute()
                duration = time.monotonic() - start

                self.history.finish_run(
                    run_id,
                    status="success",
                    duration_s=duration,
                    records=records,
                )
                result.status = "success"
                result.records = records
                result.duration_s = round(duration, 3)
                result.error = None
                logger.info(
                    "[%s] succeeded in %.2fs (%d records)", self.name, duration, records
                )
                return result

            except Exception as exc:
                duration = time.monotonic() - start
                error_msg = f"{type(exc).__name__}: {exc}"
                tb = traceback.format_exc()

                self.history.finish_run(
                    run_id,
                    status="failed",
                    duration_s=duration,
                    error=error_msg,
                )
                result.error = error_msg
                result.duration_s = round(duration, 3)

                logger.warning(
                    "[%s] attempt %d failed: %s", self.name, attempt, error_msg
                )
                logger.debug(tb)

                if attempt < self.max_attempts:
                    delay = self._backoff_delay(attempt)
                    logger.info("[%s] retrying in %.1fs...", self.name, delay)
                    time.sleep(delay)
                else:
                    logger.error(
                        "[%s] all %d attempts exhausted", self.name, self.max_attempts
                    )

        return result

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _execute(self) -> int:
        raw = self.extractor.extract()
        transformed = self.transformer.transform(raw)
        return self.loader.load(transformed)

    def _backoff_delay(self, attempt: int) -> float:
        return self.base_delay_s * (self.backoff_factor ** (attempt - 1))

    def __repr__(self) -> str:
        return (
            f"Pipeline(name={self.name!r}, "
            f"extractor={self.extractor}, "
            f"transformer={self.transformer}, "
            f"loader={self.loader})"
        )