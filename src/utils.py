"""Helper กลาง: config, รายการไฟล์, logging, run log"""
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml


class DataQualityError(Exception):
    """ถูก raise เมื่อมี data quality check ระดับ FAIL"""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def new_run_id() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:6]


def load_config(path) -> dict:
    path = Path(path).resolve()
    cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
    cfg["_root"] = path.parent
    cfg["paths"] = {k: (path.parent / v) for k, v in cfg["paths"].items()}
    return cfg


def period_months(cfg) -> list:
    """คืน list ของ (year, month) ตาม period.start..period.end"""
    sy, sm = map(int, cfg["period"]["start"].split("-"))
    ey, em = map(int, cfg["period"]["end"].split("-"))
    out, y, m = [], sy, sm
    while (y, m) <= (ey, em):
        out.append((y, m))
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def window(cfg):
    """ช่วงเวลา valid: [start, end) เป็นสตริงวันที่"""
    months = period_months(cfg)
    (sy, sm), (ey, em) = months[0], months[-1]
    ny, nm = (ey, em + 1) if em < 12 else (ey + 1, 1)
    return f"{sy}-{sm:02d}-01", f"{ny}-{nm:02d}-01"


def slug(taxi_type_spec) -> str:
    return taxi_type_spec["prefix"].split("_")[0]


def build_file_list(cfg) -> list:
    files = []
    for taxi_type, spec in cfg["taxi_types"].items():
        for y, m in period_months(cfg):
            fn = f"{spec['prefix']}_{y}-{m:02d}.parquet"
            files.append({
                "taxi_type": taxi_type,
                "month": f"{y}-{m:02d}",
                "filename": fn,
                "url": f"{cfg['base_url']}/{fn}",
            })
    return files


def get_logger(cfg) -> logging.Logger:
    reports = cfg["paths"]["reports"]
    reports.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    for h in (logging.StreamHandler(), logging.FileHandler(reports / "pipeline.log", encoding="utf-8")):
        h.setFormatter(fmt)
        logger.addHandler(h)
    return logger


def log_step(cfg, run_id, step, status, started_at, info=None, message=""):
    """เพิ่ม 1 บรรทัดใน reports/run_log.jsonl (หลักฐานสถานะการทำงาน)"""
    rec = {
        "run_id": run_id, "step": step, "status": status,
        "started_at": started_at, "finished_at": now_iso(),
        "info": info or {}, "message": message,
    }
    reports = cfg["paths"]["reports"]
    reports.mkdir(parents=True, exist_ok=True)
    with open(reports / "run_log.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
    return rec


def sql_path(p) -> str:
    return str(Path(p).as_posix()).replace("'", "''")
