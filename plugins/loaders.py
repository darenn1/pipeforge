import csv
import json
import logging
import sqlite3
from io import StringIO
from pathlib import Path
from typing import Any, Iterator

from core.base import Loader

logger = logging.getLogger(__name__)


class JSONFileLoader(Loader):
    def load(self, records: Iterator[Any]) -> int:
        path = Path(self.config["path"])
        mode = self.config.get("mode", "array")
        indent = self.config.get("indent")
        append = self.config.get("append", False)

        count = 0
        if mode == "ndjson":
            file_mode = "a" if append else "w"
            with path.open(file_mode) as fh:
                for record in records:
                    fh.write(json.dumps(record) + "\n")
                    count += 1
        else:
            all_records = list(records)
            count = len(all_records)
            with path.open("w") as fh:
                json.dump(all_records, fh, indent=indent)

        logger.info("JSONFileLoader: wrote %d records to %s", count, path)
        return count


class SQLiteLoader(Loader):
    def load(self, records: Iterator[Any]) -> int:
        path = self.config["path"]
        table = self.config["table"]
        batch_size = self.config.get("batch_size", 500)
        conflict = self.config.get("conflict", "replace").upper()
        primary_key = self.config.get("primary_key")

        conn = sqlite3.connect(path)
        try:
            count = 0
            columns: list[str] = []
            batch: list[tuple] = []

            for record in records:
                if not columns:
                    columns = list(record.keys())
                    self._ensure_table(conn, table, columns, primary_key)
                    insert_sql = self._build_insert(table, columns, conflict)

                row = tuple(record.get(c) for c in columns)
                batch.append(row)
                count += 1

                if len(batch) >= batch_size:
                    conn.executemany(insert_sql, batch)
                    conn.commit()
                    batch.clear()

            if batch:
                conn.executemany(insert_sql, batch)
                conn.commit()

        finally:
            conn.close()

        logger.info("SQLiteLoader: upserted %d records into %s::%s", count, path, table)
        return count

    # ------------------------------------------------------------------
    # Schema helpers
    # ------------------------------------------------------------------

    def _ensure_table(
        self,
        conn: sqlite3.Connection,
        table: str,
        columns: list[str],
        primary_key: str | None,
    ) -> None:
        col_defs = []
        for col in columns:
            if col == primary_key:
                col_defs.append(f'"{col}" TEXT PRIMARY KEY')
            else:
                col_defs.append(f'"{col}" TEXT')
        ddl = f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(col_defs)})'
        conn.execute(ddl)
        conn.commit()

    @staticmethod
    def _build_insert(table: str, columns: list[str], conflict: str) -> str:
        cols = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join("?" * len(columns))
        return (
            f'INSERT OR {conflict} INTO "{table}" ({cols}) VALUES ({placeholders})'
        )


class CSVLoader(Loader):
    def load(self, records: Iterator[Any]) -> int:
        path = Path(self.config["path"])
        delimiter = self.config.get("delimiter", ",")
        append = self.config.get("append", False)

        count = 0
        fieldnames: list[str] = []
        buffer: list[dict] = []

        for record in records:
            if not fieldnames:
                fieldnames = list(record.keys())
            buffer.append(record)
            count += 1

        if not buffer:
            return 0

        file_mode = "a" if append else "w"
        with path.open(file_mode, newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter=delimiter)
            if not append:
                writer.writeheader()
            writer.writerows(buffer)

        logger.info("CSVLoader: wrote %d records to %s", count, path)
        return count


class StdoutLoader(Loader):
    def load(self, records: Iterator[Any]) -> int:
        indent = self.config.get("indent")
        limit = self.config.get("limit")
        count = 0
        for record in records:
            if limit and count >= limit:
                break
            print(json.dumps(record, indent=indent, default=str))
            count += 1
        return count