import sys
import argparse
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from core.config import load_pipeline
from core.history import RunHistory

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("pipeforge")


# ====================================================================
# Command handlers
# ====================================================================

def cmd_run(args) -> int:
    path = Path(args.pipeline)
    if not path.exists():
        logger.error("Pipeline file not found: %s", path)
        return 1

    history = RunHistory(Path(args.db))

    pipeline = load_pipeline(path, history=history)
    if args.attempts:
        pipeline.max_attempts = args.attempts

    logger.info("Starting pipeline: %s", pipeline.name)
    result = pipeline.run()

    _print_result(result)
    return 0 if result.succeeded else 1


def cmd_schedule(args) -> int:
    path = Path(args.pipeline)
    if not path.exists():
        logger.error("Pipeline file not found: %s", path)
        return 1

    from core.scheduler import Scheduler
    history = RunHistory(Path(args.db))

    scheduler = Scheduler(
        pipeline_path=path,
        history=history,
        every=args.every,
        unit=args.unit,
    )
    logger.info(
        "Scheduling %s every %d %s — press Ctrl-C to stop",
        path.name, args.every, args.unit,
    )
    scheduler.run_forever()
    return 0


def cmd_list(args) -> int:
    search_dir = Path(args.dir)
    if not search_dir.exists():
        logger.error("Directory not found: %s", search_dir)
        return 1

    yamls = sorted(search_dir.rglob("*.yaml")) + sorted(search_dir.rglob("*.yml"))
    if not yamls:
        print(f"No pipeline files found in {search_dir}/")
        return 0

    print(f"\n{'PIPELINE FILE':<45}  {'SIZE':>8}")
    print("─" * 56)
    for p in yamls:
        size = p.stat().st_size
        print(f"  {str(p):<43}  {size:>6} B")
    print()
    return 0


def cmd_history(args) -> int:
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"No history found (db not created yet: {db_path})")
        return 0

    history = RunHistory(db_path)
    runs = history.recent(pipeline=args.pipeline or None, limit=args.limit)

    if not runs:
        msg = f"pipeline '{args.pipeline}'" if args.pipeline else "any pipeline"
        print(f"No runs found for {msg}")
        return 0

    # ── header ───────────────────────────────────────────────────────
    print(f"\n  {'ID':>4}  {'PIPELINE':<24}  {'STATUS':<8}  {'RECORDS':>7}  "
          f"{'DURATION':>9}  {'ATTEMPT':>7}  STARTED")
    print("  " + "─" * 82)

    for r in runs:
        status_color = _color(r["status"])
        duration = f"{r['duration_s']:.2f}s" if r["duration_s"] is not None else "—"
        records  = str(r["records"]) if r["records"] is not None else "—"
        attempt  = str(r["attempt"])
        started  = (r["started_at"] or "")[:19].replace("T", " ")

        print(
            f"  {r['id']:>4}  {r['pipeline']:<24}  {status_color:<8}  "
            f"{records:>7}  {duration:>9}  {attempt:>7}  {started}"
        )
        if r["status"] == "failed" and r["error"]:
            print(f"         \033[91m↳ {r['error'][:72]}\033[0m")

    print()
    return 0


# ====================================================================
# Helpers
# ====================================================================

def _print_result(result) -> None:
    status = _color(result.status)
    print(f"\n  {'─'*44}")
    print(f"  pipeline : {result.pipeline}")
    print(f"  status   : {status}")
    print(f"  records  : {result.records}")
    print(f"  duration : {result.duration_s}s")
    print(f"  attempts : {result.attempts}")
    if result.error:
        print(f"  error    : \033[91m{result.error}\033[0m")
    print(f"  {'─'*44}\n")


def _color(status: str) -> str:
    if status == "success":
        return f"\033[92m{status}\033[0m"
    if status == "failed":
        return f"\033[91m{status}\033[0m"
    return f"\033[93m{status}\033[0m"


# ====================================================================
# Argument parser
# ====================================================================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipeforge",
        description="Lightweight modular ETL pipeline runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python cli.py run pipelines/example.yaml
  python cli.py run pipelines/example.yaml --attempts 5
  python cli.py schedule pipelines/example.yaml --every 10 --unit minutes
  python cli.py list --dir pipelines/
  python cli.py history
  python cli.py history user_ingest --limit 5
  python cli.py --db /tmp/custom.db run pipelines/example.yaml
        """,
    )
    parser.add_argument(
        "--db",
        default="pipeforge.db",
        metavar="PATH",
        help="SQLite history database path (default: pipeforge.db)",
    )

    sub = parser.add_subparsers(dest="command", metavar="command")
    sub.required = True

    # ── run ──────────────────────────────────────────────────────────
    p_run = sub.add_parser("run", help="Execute a pipeline once")
    p_run.add_argument("pipeline", help="Path to pipeline YAML file")
    p_run.add_argument(
        "--attempts", type=int, default=None,
        metavar="N", help="Override max retry attempts"
    )
    p_run.set_defaults(func=cmd_run)

    # ── schedule ─────────────────────────────────────────────────────
    p_sched = sub.add_parser("schedule", help="Run a pipeline on a schedule")
    p_sched.add_argument("pipeline", help="Path to pipeline YAML file")
    p_sched.add_argument(
        "--every", type=int, default=1,
        metavar="N", help="Run every N units (default: 1)"
    )
    p_sched.add_argument(
        "--unit", default="hours",
        choices=["seconds", "minutes", "hours", "days"],
        help="Time unit (default: hours)"
    )
    p_sched.set_defaults(func=cmd_schedule)

    # ── list ─────────────────────────────────────────────────────────
    p_list = sub.add_parser("list", help="List pipeline YAML files")
    p_list.add_argument(
        "--dir", default="pipelines/",
        metavar="DIR", help="Directory to search (default: pipelines/)"
    )
    p_list.set_defaults(func=cmd_list)

    # ── history ──────────────────────────────────────────────────────
    p_hist = sub.add_parser("history", help="Show run history")
    p_hist.add_argument(
        "pipeline", nargs="?", default=None,
        help="Filter by pipeline name (optional)"
    )
    p_hist.add_argument(
        "--limit", type=int, default=10,
        metavar="N", help="Max rows to show (default: 10)"
    )
    p_hist.set_defaults(func=cmd_history)

    return parser

if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    sys.exit(args.func(args))