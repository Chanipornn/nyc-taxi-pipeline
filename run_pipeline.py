#!/usr/bin/env python3
"""Entry point: ingest -> transform -> validate -> analyze
ใช้: python run_pipeline.py [--config config.yaml] [--offline]
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src import analyze, ingest, transform, validate  # noqa: E402
from src.utils import DataQualityError, get_logger, load_config, log_step, new_run_id, now_iso  # noqa: E402


def run_pipeline(cfg, offline=None) -> dict:
    log = get_logger(cfg)
    run_id = new_run_id()
    log.info("=== Run %s started ===", run_id)
    steps = [
        ("ingest", lambda: ingest.run(cfg, run_id, offline)),
        ("transform", lambda: transform.run(cfg, run_id)),
        ("validate", lambda: validate.run(cfg, run_id)),
        ("analyze", lambda: analyze.run(cfg, run_id)),
    ]
    summary = {"run_id": run_id}
    for name, fn in steps:
        started = now_iso()
        try:
            info = fn()
        except Exception as e:  # log แล้ว raise ต่อ เพื่อหยุด pipeline
            status = "FAILED_DQ" if isinstance(e, DataQualityError) else "FAILED"
            log_step(cfg, run_id, name, status, started, message=str(e))
            log.error("Step %s %s: %s", name, status, e)
            raise
        log_step(cfg, run_id, name, "SUCCESS", started, info)
        summary[name] = info
        log.info("Step %s SUCCESS %s", name, info)
    log.info("=== Run %s finished: most used = %s ===", run_id, summary["analyze"]["most_used"])
    return summary


def main():
    ap = argparse.ArgumentParser(description="NYC Taxi reproducible pipeline")
    ap.add_argument("--config", default=str(Path(__file__).parent / "config.yaml"))
    ap.add_argument("--offline", action="store_true", help="ไม่ใช้เน็ต ใช้ไฟล์ใน data/raw")
    args = ap.parse_args()
    cfg = load_config(args.config)
    try:
        run_pipeline(cfg, offline=True if args.offline else None)
    except DataQualityError:
        sys.exit(2)
    except Exception:
        sys.exit(1)


if __name__ == "__main__":
    main()
