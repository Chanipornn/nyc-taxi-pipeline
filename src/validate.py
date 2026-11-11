"""Step 3: Validate — ตรวจคุณภาพข้อมูล สร้าง reports/dq_report.csv/.json
ระดับผล: PASS / WARN / FAIL  (FAIL = หยุด pipeline ก่อนวิเคราะห์)
"""
import csv
import json
import logging

import duckdb

from .ingest import USABLE_STATUSES, read_manifest
from .transform import curated_glob, load_state
from .utils import DataQualityError, build_file_list, now_iso, sql_path, window

log = logging.getLogger("pipeline")


def run(cfg, run_id) -> dict:
    start, end = window(cfg)
    dq = cfg["dq"]
    results = []

    def add(check, scope, expected, actual, status, detail=""):
        results.append({"check": check, "scope": scope, "expected": expected,
                        "actual": actual, "status": status, "detail": detail,
                        "run_id": run_id, "checked_at": now_iso()})

    expected_files = build_file_list(cfg)
    manifest = {r["filename"]: r for r in read_manifest(cfg)}
    state = load_state(cfg)["files"]
    con = duckdb.connect()

    # สถิติ curated ต่อไฟล์ (query ครั้งเดียว)
    stats = {}
    try:
        df = con.execute(f"""
            SELECT source_file,
                   COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE is_valid) AS valid,
                   COUNT(*) FILTER (WHERE invalid_reason = 'null_pickup') AS null_pickup,
                   COUNT(*) FILTER (WHERE invalid_reason = 'out_of_range') AS out_of_range,
                   COUNT(*) FILTER (WHERE dropoff_before_pickup) AS reversed
            FROM read_parquet('{curated_glob(cfg)}', hive_partitioning=false)
            GROUP BY source_file
        """).df()
        stats = {r.source_file: r for r in df.itertuples()}
    except duckdb.IOException:
        pass

    # 1) completeness
    present = [f for f in expected_files if manifest.get(f["filename"], {}).get("status") in USABLE_STATUSES]
    add("file_completeness", "all", len(expected_files), len(present),
        "PASS" if len(present) == len(expected_files) else "FAIL",
        "จำนวนไฟล์ที่ต้องมี = taxi types x เดือนใน period")

    for f in expected_files:
        fn = f["filename"]
        m = manifest.get(fn, {})
        # 2) source availability
        ok = m.get("status") in USABLE_STATUSES
        add("source_available", fn, "DOWNLOADED/SKIPPED/LOCAL", m.get("status", "NOT_IN_MANIFEST"),
            "PASS" if ok else "FAIL")
        if not ok:
            continue
        st = state.get(fn, {})
        # 3) schema
        pick = st.get("resolved", {}).get("pickup")
        add("schema_pickup_column", fn, cfg["taxi_types"][f["taxi_type"]]["pickup"], pick,
            "PASS" if pick else "FAIL")
        if not pick:
            continue
        s = stats.get(fn)
        if s is None:
            add("curated_exists", fn, "curated partition", "missing", "FAIL")
            continue
        total = int(s.total)
        # 4) row count
        add("row_count_positive", fn, "> 0", total, "PASS" if total > 0 else "FAIL")
        # 5) reconciliation raw = curated = valid + null + out_of_range
        raw_rows = st.get("raw_rows")
        recon = int(s.valid) + int(s.null_pickup) + int(s.out_of_range)
        add("row_reconciliation", fn, raw_rows, f"curated={total}, valid+invalid={recon}",
            "PASS" if raw_rows == total == recon else "FAIL", "ไม่มีแถวหายระหว่าง transform")
        # 6-8) ratios
        for name, val, thr in (
            ("null_pickup_ratio", int(s.null_pickup), dq["null_pickup_warn_ratio"]),
            ("out_of_range_ratio", int(s.out_of_range), dq["out_of_range_warn_ratio"]),
            ("dropoff_before_pickup_ratio", int(s.reversed), dq["dropoff_before_pickup_warn_ratio"]),
        ):
            ratio = val / total if total else 0
            add(name, fn, f"<= {thr}", f"{ratio:.6f} ({val:,} rows)",
                "PASS" if ratio <= thr else "WARN",
                "WARN เท่านั้น ไม่ลบข้อมูล" if ratio > thr else "")
        # 9) independent recount จาก raw ด้วยกฎเดียวกัน ต้องเท่ากับ curated
        src = sql_path(cfg["paths"]["raw"] / fn)
        recount = con.execute(f"""
            SELECT COUNT(*) FILTER (WHERE TRY_CAST("{pick}" AS TIMESTAMP) >= TIMESTAMP '{start}'
                                      AND TRY_CAST("{pick}" AS TIMESTAMP) <  TIMESTAMP '{end}')
            FROM read_parquet('{src}')""").fetchone()[0]
        add("valid_recount_from_raw", fn, recount, int(s.valid),
            "PASS" if recount == int(s.valid) else "FAIL", "นับ valid จาก raw ตรง ๆ เทียบกับ curated")
        # 10) duplicates (optional)
        if dq.get("check_duplicates"):
            d = con.execute(f"""
                SELECT COUNT(*) - COUNT(DISTINCT (pickup_datetime, dropoff_datetime, pu_location_id, do_location_id))
                FROM read_parquet('{sql_path(cfg['paths']['curated'] / 'trips_unified' / ('taxi=' + f['filename'].split('_')[0]) / ('month=' + f['month']) / 'data.parquet')}')
                """).fetchone()[0]
            add("duplicate_rows", fn, 0, int(d), "PASS" if d == 0 else "WARN",
                "ข้อมูล taxi มี trip ที่ key ซ้ำได้ตามธรรมชาติ จึงเป็น WARN")

    counts = {k: sum(r["status"] == k for r in results) for k in ("PASS", "WARN", "FAIL")}
    reports = cfg["paths"]["reports"]
    reports.mkdir(parents=True, exist_ok=True)
    with open(reports / "dq_report.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(results[0].keys()))
        w.writeheader()
        w.writerows(results)
    (reports / "dq_report.json").write_text(
        json.dumps({"run_id": run_id, "summary": counts, "results": results},
                   indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    log.info("DQ: %s", counts)

    if counts["FAIL"]:
        failed = [f"{r['check']}[{r['scope']}]" for r in results if r["status"] == "FAIL"]
        raise DataQualityError(f"{counts['FAIL']} FAIL checks: {failed[:5]}")
    return {"checks": len(results), **counts}
