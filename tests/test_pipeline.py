"""ทดสอบ pipeline ด้วยข้อมูลจำลอง (offline) — ไม่ต้องใช้เน็ต
รัน: pytest -q
"""
import csv
import json
from pathlib import Path

import duckdb
import pytest
import yaml

from run_pipeline import run_pipeline
from src.utils import DataQualityError, load_config, build_file_list

ROOT = Path(__file__).resolve().parents[1]
ROWS = {"Yellow Taxi": 100, "Green Taxi": 10, "FHV": 80, "HVFHV": 500}  # ต่อเดือน


def make_fake_raw(cfg, rename_pickup_for=None):
    raw = cfg["paths"]["raw"]
    raw.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    for f in build_file_list(cfg):
        spec = cfg["taxi_types"][f["taxi_type"]]
        n = ROWS[f["taxi_type"]]
        pick, drop = spec["pickup"], spec["dropoff"]
        if rename_pickup_for == f["filename"]:
            pick = "some_other_col"
        ym = f["month"]
        # 2 แถว null, 3 แถวอยู่นอก period (ปี 2002), 1 แถว dropoff < pickup
        con.execute(f"""
            COPY (
              SELECT
                CASE WHEN i < 2 THEN NULL
                     WHEN i < 5 THEN TIMESTAMP '2002-12-31 10:00:00'
                     ELSE TIMESTAMP '{ym}-15 10:00:00' + INTERVAL (i) MINUTE END AS "{pick}",
                CASE WHEN i = 7 THEN TIMESTAMP '{ym}-01 00:00:00'
                     ELSE TIMESTAMP '{ym}-15 11:00:00' END AS "{drop}",
                CAST(i % 200 AS INTEGER) AS "{spec['pu_location']}",
                CAST(i % 100 AS INTEGER) AS "{spec['do_location']}"
              FROM range({n}) t(i)
            ) TO '{(raw / f['filename']).as_posix()}' (FORMAT PARQUET)
        """)


@pytest.fixture
def cfg(tmp_path):
    base = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
    base["offline"] = True
    base["paths"] = {k: str(tmp_path / v) for k, v in
                     {"raw": "raw", "curated": "curated", "reports": "reports", "sample": "sample"}.items()}
    # path แบบ absolute -> load_config ใช้ root/abs = abs
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(base, sort_keys=False), encoding="utf-8")
    c = load_config(p)
    make_fake_raw(c)
    return c


def test_end_to_end_answer(cfg):
    s = run_pipeline(cfg)
    assert s["analyze"]["most_used"] == "HVFHV"
    rep = cfg["paths"]["reports"]
    for name in ("manifest.csv", "dq_report.csv", "dq_report.json", "run_log.jsonl",
                 "monthly_summary.csv", "answer.md", "trips_by_type.png", "taxi_type_ranking.csv"):
        assert (rep / name).exists(), name
    assert (cfg["paths"]["sample"] / "trips_sample.csv").exists()
    # valid = 500 - 2 null - 3 out_of_range = 495 ต่อเดือน x 6
    rows = list(csv.DictReader(open(rep / "taxi_type_ranking.csv")))
    hv = next(r for r in rows if r["taxi_type"] == "HVFHV")
    assert int(hv["valid_records"]) == 495 * 6


def test_dq_has_no_fail_and_reconciles(cfg):
    run_pipeline(cfg)
    rep = json.loads((cfg["paths"]["reports"] / "dq_report.json").read_text())
    assert rep["summary"]["FAIL"] == 0
    assert any(r["check"] == "row_reconciliation" and r["status"] == "PASS" for r in rep["results"])


def test_rerun_is_idempotent(cfg):
    s1 = run_pipeline(cfg)
    r1 = (cfg["paths"]["reports"] / "monthly_summary.csv").read_text()
    s2 = run_pipeline(cfg)
    assert s1["transform"]["processed"] == 24
    assert s2["transform"]["processed"] == 0 and s2["transform"]["skipped"] == 24
    assert (cfg["paths"]["reports"] / "monthly_summary.csv").read_text() == r1


def test_changed_file_reprocessed_only_that_file(cfg):
    run_pipeline(cfg)
    f = build_file_list(cfg)[0]["filename"]  # Yellow 2024-01 -> เขียนใหม่ให้เนื้อหาเปลี่ยน
    p = cfg["paths"]["raw"] / f
    duckdb.execute(f"COPY (SELECT TIMESTAMP '2024-01-10 10:00:00' AS tpep_pickup_datetime, "
                   f"TIMESTAMP '2024-01-10 10:30:00' AS tpep_dropoff_datetime, 1 AS PULocationID, 2 AS DOLocationID) "
                   f"TO '{p.as_posix()}.new' (FORMAT PARQUET)")
    p.unlink()
    Path(str(p) + ".new").rename(p)
    s = run_pipeline(cfg)
    assert s["transform"]["processed"] == 1 and s["transform"]["skipped"] == 23


def test_missing_file_fails_dq(cfg):
    f = build_file_list(cfg)[5]["filename"]
    (cfg["paths"]["raw"] / f).unlink()
    with pytest.raises(DataQualityError):
        run_pipeline(cfg)
    last = [json.loads(l) for l in open(cfg["paths"]["reports"] / "run_log.jsonl")][-1]
    assert last["step"] == "validate" and last["status"] == "FAILED_DQ"
    assert not (cfg["paths"]["reports"] / "answer.md").exists()  # fail-fast: ไม่ถึง analyze


def test_schema_drift_fails_dq(cfg):
    f = build_file_list(cfg)[0]["filename"]
    (cfg["paths"]["raw"] / f).unlink()
    make_fake_raw(cfg, rename_pickup_for=f)
    with pytest.raises(DataQualityError):
        run_pipeline(cfg)
