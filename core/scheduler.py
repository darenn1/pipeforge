import time
import signal
import logging
import schedule
from pathlib import Path

from core.history import RunHistory
from core.config import load_pipeline

logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(
        self,
        pipeline_path: str | Path,
        history: RunHistory | None = None,
        every: int = 1,
        unit: str = "hours",
    ):
        self.pipeline_path = Path(pipeline_path)
        self.history = history or RunHistory()
        self.every = every
        self.unit = unit
        self._running = False

    def run_once(self) -> None:
        """Load and execute the pipeline a single time."""
        logger.info("[scheduler] tick — loading %s", self.pipeline_path.name)
        try:
            pipeline = load_pipeline(self.pipeline_path, history=self.history)
            result = pipeline.run()
            if result.succeeded:
                logger.info(
                    "[scheduler] %s succeeded — %d records in %.2fs",
                    pipeline.name, result.records, result.duration_s,
                )
            else:
                logger.error(
                    "[scheduler] %s failed after %d attempt(s): %s",
                    pipeline.name, result.attempts, result.error,
                )
        except Exception as exc:
            logger.exception("[scheduler] failed to load pipeline: %s", exc)

    def run_forever(self) -> None:
        self._running = True
        signal.signal(signal.SIGINT,  self._handle_stop)
        signal.signal(signal.SIGTERM, self._handle_stop)

        interval = getattr(schedule.every(self.every), self.unit)
        interval.do(self.run_once)

        logger.info(
            "[scheduler] running every %d %s — press Ctrl-C to stop",
            self.every, self.unit,
        )

        self.run_once()

        while self._running:
            schedule.run_pending()
            time.sleep(1)

    # ------------------------------------------------------------------
    # Signal handling
    # ------------------------------------------------------------------

    def _handle_stop(self, signum, frame) -> None:
        logger.info("[scheduler] received signal %d — shutting down", signum)
        self._running = False