"""Step 4: Analyze — คำนวณ metric 'most used' จาก curated (ไม่อ่าน raw)

Most used = taxi type ที่มีจำนวน valid trip records มากที่สุดใน period
"""
import logging

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from .transform import curated_glob  # noqa: E402
from .utils import sql_path, window  # noqa: E402

log = logging.getLogger("pipeline")


def run(cfg, run_id) -> dict:
    reports = cfg["paths"]["reports"]
    reports.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    g = curated_glob(cfg)
    start, end = window(cfg)

    monthly = con.execute(f"""
        SELECT taxi_type, file_month AS month,
               COUNT(*) AS raw_records,
               COUNT(*) FILTER (WHERE is_valid) AS valid_records,
               COUNT(*) FILTER (WHERE NOT is_valid) AS invalid_records
        FROM read_parquet('{g}', hive_partitioning=false)
        GROUP BY taxi_type, file_month ORDER BY taxi_type, file_month
    """).df()
    monthly.to_csv(reports / "monthly_summary.csv", index=False)

    ranking = (monthly.groupby("taxi_type", as_index=False)[["raw_records", "valid_records"]].sum()
               .sort_values("valid_records", ascending=False).reset_index(drop=True))
    total_valid = int(ranking["valid_records"].sum())
    ranking["share_pct"] = (ranking["valid_records"] / total_valid * 100).round(2)
    ranking.to_csv(reports / "taxi_type_ranking.csv", index=False)

    top, second = ranking.iloc[0], ranking.iloc[1]
    ratio = top.valid_records / second.valid_records if second.valid_records else float("inf")

    # chart
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(ranking["taxi_type"], ranking["valid_records"] / 1e6)
    ax.set_xlabel("Taxi type")
    ax.set_ylabel("Valid trips (millions)")
    ax.set_title(f"NYC Taxi usage {start} to {end} (exclusive)")
    for i, v in enumerate(ranking["valid_records"]):
        ax.text(i, v / 1e6, f"{v:,}", ha="center", va="bottom", fontsize=8)
    fig.tight_layout()
    fig.savefig(reports / "trips_by_type.png", dpi=150)
    plt.close(fig)

    # answer.md
    table = "\n".join(
        f"| {r.taxi_type} | {int(r.valid_records):,} | {r.share_pct:.2f}% |"
        for r in ranking.itertuples())
    (reports / "answer.md").write_text(f"""# คำตอบ: Which taxi type was the most used in the first half of 2024?

**คำตอบ: {top.taxi_type}** — {int(top.valid_records):,} valid trip records
(ช่วงเวลา {start} ถึงก่อน {end}) คิดเป็น {top.share_pct:.2f}% ของทุกประเภท
และมากกว่าอันดับ 2 ({second.taxi_type}) ประมาณ {ratio:.1f} เท่า

| Taxi type | Valid trips | Share |
|---|---:|---:|
{table}

**นิยาม:** most used = จำนวน valid trip records มากที่สุด (ไม่ใช่รายได้/ระยะทาง/ผู้โดยสาร)
**ที่มา:** `data/curated/trips_unified` -> `reports/monthly_summary.csv` -> ตารางนี้
(run_id: `{run_id}`)
""", encoding="utf-8")

    # sample
    sample_dir = cfg["paths"]["sample"]
    sample_dir.mkdir(parents=True, exist_ok=True)
    n = int(cfg.get("sample_rows_per_type", 2500))
    parts = []
    for t in ranking["taxi_type"]:
        parts.append(con.execute(f"""
            SELECT * FROM read_parquet('{g}', hive_partitioning=false)
            WHERE taxi_type = ? USING SAMPLE reservoir({n} ROWS) REPEATABLE (42)
        """, [t]).df())
    import pandas as pd
    pd.concat(parts).to_csv(sample_dir / "trips_sample.csv", index=False)

    log.info("Analyze: most used = %s (%s)", top.taxi_type, f"{int(top.valid_records):,}")
    return {"most_used": top.taxi_type, "valid_trips": int(top.valid_records),
            "ratio_vs_second": round(float(ratio), 2)}
