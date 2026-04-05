import sqlite3
import contextlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

_SENTINEL = object()

DEFAULT_DB_PATH = Path("pipeforge.db")


class RunHistory:
    def __init__(self, db_path: Path = DEFAULT_DB_PATH, persistent: bool = False):
        self.db_path = db_path
        self._memory_conn: Optional[sqlite3.Connection] = None
        if str(db_path) == ":memory:" or persistent:
            self._memory_conn = sqlite3.connect(str(db_path))
        self._init_db()
    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS runs (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    pipeline    TEXT    NOT NULL,
                    started_at  TEXT    NOT NULL,
                    finished_at TEXT,
                    status      TEXT    NOT NULL,
                    duration_s  REAL,
                    records     INTEGER,
                    error       TEXT,
                    attempt     INTEGER NOT NULL DEFAULT 1
                )
            """)

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def start_run(self, pipeline: str, attempt: int = 1) -> int:
        """Insert a new run row and return its ID."""
        now = _utcnow()
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO runs (pipeline, started_at, status, attempt)
                VALUES (?, ?, 'running', ?)
                """,
                (pipeline, now, attempt),
            )
            return cur.lastrowid

    def finish_run(
        self,
        run_id: int,
        *,
        status: str,
        duration_s: float,
        records: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        """Update an existing run row with its final state."""
        now = _utcnow()
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE runs
                SET finished_at = ?,
                    status      = ?,
                    duration_s  = ?,
                    records     = ?,
                    error       = ?
                WHERE id = ?
                """,
                (now, status, round(duration_s, 3), records, error, run_id),
            )

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def recent(self, pipeline: Optional[str] = None, limit: int = 20) -> list[dict]:
        """Return a list of recent runs, optionally filtered by pipeline."""
        query = "SELECT * FROM runs"
        params: list = []
        if pipeline:
            query += " WHERE pipeline = ?"
            params.append(pipeline)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)

        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, params).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @contextlib.contextmanager
    def _conn(self):
        if self._memory_conn is not None:
            try:
                yield self._memory_conn
                self._memory_conn.commit()
            except Exception:
                self._memory_conn.rollback()
                raise
        else:
            conn = sqlite3.connect(self.db_path)
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")