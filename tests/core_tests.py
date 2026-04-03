import sys
import time
import tempfile
from pathlib import Path
from typing import Any, Iterator


sys.path.insert(0, str(Path(__file__).parent.parent))

from core.base import Extractor, Transformer, Loader
from core.history import RunHistory
from core.pipeline import Pipeline

PASS = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"


def check(label: str, condition: bool) -> None:
    print(f"  {PASS if condition else FAIL}  {label}")
    if not condition:
        raise AssertionError(label)


class NumberExtractor(Extractor):
    """Yields integers 0..n-1."""
    def extract(self) -> Iterator[int]:
        for i in range(self.config.get("count", 5)):
            yield i


class DoubleTransformer(Transformer):
    """Doubles each number."""
    def transform(self, records: Iterator[Any]) -> Iterator[Any]:
        for r in records:
            yield r * 2


class ListLoader(Loader):
    """Collects records into self.results."""
    def __init__(self, config: dict):
        super().__init__(config)
        self.results: list = []

    def load(self, records: Iterator[Any]) -> int:
        self.results = list(records)
        return len(self.results)


class BrokenExtractor(Extractor):
    """Always raises — used to test retry logic."""
    def extract(self) -> Iterator[Any]:
        raise RuntimeError("simulated source failure")
        yield  


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------

def test_abc_contracts():
    print("\n[1] ABC contracts")
    try:
        Extractor({}) 
        check("Extractor cannot be instantiated directly", False)
    except TypeError:
        check("Extractor cannot be instantiated directly", True)

    try:
        Transformer({})  
        check("Transformer cannot be instantiated directly", False)
    except TypeError:
        check("Transformer cannot be instantiated directly", True)

    try:
        Loader({})  
        check("Loader cannot be instantiated directly", False)
    except TypeError:
        check("Loader cannot be instantiated directly", True)


def test_happy_path(db_path: Path):
    print("\n[2] Happy-path pipeline run")
    history = RunHistory(db_path)
    loader = ListLoader({})
    pipeline = Pipeline(
        name="test_happy",
        extractor=NumberExtractor({"count": 6}),
        transformer=DoubleTransformer({}),
        loader=loader,
        max_attempts=1,
        history=history,
    )
    result = pipeline.run()

    check("status is success",         result.succeeded)
    check("records count is 6",        result.records == 6)
    check("loader received [0,2,4,6,8,10]", loader.results == [0, 2, 4, 6, 8, 10])
    check("duration recorded",         result.duration_s >= 0)
    check("no error",                  result.error is None)

    runs = history.recent("test_happy")
    check("one row in history",        len(runs) == 1)
    check("history status is success", runs[0]["status"] == "success")
    check("history records is 6",      runs[0]["records"] == 6)


def test_failure_and_retry(db_path: Path):
    print("\n[3] Failure + retry (3 attempts)")
    history = RunHistory(db_path)
    pipeline = Pipeline(
        name="test_retry",
        extractor=BrokenExtractor({}),
        transformer=DoubleTransformer({}),
        loader=ListLoader({}),
        max_attempts=3,
        base_delay_s=0.05,   
        backoff_factor=2.0,
        history=history,
    )

    t0 = time.monotonic()
    result = pipeline.run()
    elapsed = time.monotonic() - t0

    check("status is failed",          not result.succeeded)
    check("attempts == 3",             result.attempts == 3)
    check("error message captured",    "RuntimeError" in (result.error or ""))
    check("3 run_ids logged",          len(result.run_ids) == 3)
    check("wall-clock >= 2 delays",    elapsed >= 0.05 + 0.10)  

    runs = history.recent("test_retry")
    check("3 rows in history",         len(runs) == 3)
    check("all rows status=failed",    all(r["status"] == "failed" for r in runs))
    check("attempt numbers logged",    {r["attempt"] for r in runs} == {1, 2, 3})


def test_generator_laziness():
    print("\n[4] Generator chain is lazy")
    evaluated = []

    class TrackingExtractor(Extractor):
        def extract(self) -> Iterator[Any]:
            for i in range(3):
                evaluated.append(("extract", i))
                yield i

    class TrackingTransformer(Transformer):
        def transform(self, records: Iterator[Any]) -> Iterator[Any]:
            for r in records:
                evaluated.append(("transform", r))
                yield r

    class CountLoader(Loader):
        def load(self, records: Iterator[Any]) -> int:
            return sum(1 for _ in records)

    pipeline = Pipeline(
        "lazy_test",
        TrackingExtractor({}),
        TrackingTransformer({}),
        CountLoader({}),
        max_attempts=1,
        history=RunHistory(Path(":memory:")), 
    )

    check("nothing evaluated before run()", len(evaluated) == 0)
    pipeline.run()
    check("all 3 items extracted and transformed", len(evaluated) == 6)
    kinds = [e[0] for e in evaluated]
    check("extract/transform interleaved (lazy)", kinds == ["extract","transform"] * 3)


# ------------------------------------------------------------------
# Runner
# ------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 50)
    print("pipeforge — Day 1 smoke tests")
    print("=" * 50)

    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "test.db"
        try:
            test_abc_contracts()
            test_happy_path(db)
            test_failure_and_retry(db)
            test_generator_laziness()
            print(f"\n{'='*50}")
            print(f"{PASS}  All tests passed")
            print(f"{'='*50}\n")
        except AssertionError as e:
            print(f"\n{FAIL}  Test failed: {e}\n")
            sys.exit(1)