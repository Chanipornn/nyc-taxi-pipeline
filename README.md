# NYC Taxi — Reproducible Data Pipeline (Submission 2)

**คำถาม:** Which taxi type was the most used in the first half of 2024?

**ชื่อ–รหัส:** ชนิภรณ์ คิมประเสริฐ 6709650243

**คำตอบ:** **HVFHV** — 120,864,668 valid trips (มากกว่า Yellow Taxi อันดับ 2 ประมาณ 5.94 เท่า) ตรงกับผล Baseline (Submission 1)

## 1. ข้อมูลที่ใช้
- แหล่ง: NYC TLC Trip Record Data — https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- 4 ประเภท (Yellow, Green, FHV, HVFHV) x มกราคม–มิถุนายน 2024 = **24 ไฟล์ Parquet** (รวม 149,655,796 แถว)
- นิยาม **most used** = taxi type ที่มี *valid trip records* มากที่สุดใน period

## 2. โครงสร้าง Pipeline
ดูแผนภาพที่ [`docs/pipeline_diagram.md`](docs/pipeline_diagram.md)

| Step | ไฟล์ | หน้าที่ | Output |
|---|---|---|---|
| 1 Ingest | `src/ingest.py` | ตรวจ/ดาวน์โหลด, ข้ามไฟล์ที่ไม่เปลี่ยน (ETag/size/Last-Modified) | `data/raw/`, `reports/manifest.csv` |
| 2 Transform | `src/transform.py` | map 4 schemas เป็น schema เดียว, ติดธงแถวไม่ valid, เขียน Parquet แบบ partition | `data/curated/trips_unified/` |
| 3 Validate | `src/validate.py` | data quality checks (PASS/WARN/FAIL), FAIL = หยุด | `reports/dq_report.csv/.json` |
| 4 Analyze | `src/analyze.py` | นับ valid trips จาก curated, สรุปคำตอบ, chart, sample | `reports/answer.md` ฯลฯ, `data/sample/` |
| — | `run_pipeline.py`, `src/utils.py` | orchestration + run log | `reports/run_log.jsonl`, `reports/pipeline.log` |

## 3. วิธีติดตั้งและรัน
```bash
git clone https://github.com/Chanipornn/nyc-taxi-pipeline.git
cd nyc-taxi-pipeline
python3 -m venv .venv
source .venv/bin/activate              # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python run_pipeline.py                 # ต้องมีอินเทอร์เน็ต (โหลด ~2-3 GB ครั้งแรก)
python run_pipeline.py --offline       # ใช้ไฟล์ที่มีใน data/raw เท่านั้น
pytest -q                              # ทดสอบด้วยข้อมูลจำลอง ไม่ใช้เน็ต
```
Exit code: `0` สำเร็จ, `2` data quality FAIL, `1` error อื่น

## 4. รันซ้ำ / อัปเดตข้อมูลใหม่
| สถานการณ์ | พฤติกรรม |
|---|---|
| รันซ้ำโดยไม่มีอะไรเปลี่ยน | ingest = `SKIPPED` ทั้งหมด, transform `processed=0` |
| TLC แก้ไฟล์ / ลบไฟล์ใน `data/raw` | โหลดเฉพาะไฟล์นั้น (sha256 เปลี่ยน) และ transform เฉพาะ partition นั้น |
| เพิ่มเดือนใหม่ (แก้ `period.end` ใน `config.yaml`) | โหลดเฉพาะไฟล์ใหม่ แต่ *transform ทำใหม่ทั้งหมด* เพราะ validity window เปลี่ยน (ดูข้อจำกัด) |

## 5. หลักฐานการรัน
รันจริงเมื่อ 11 พ.ย. 2569 บันทึก log ไว้ที่ `docs/evidence/`

| รอบ | ไฟล์ log | run_id | ingest | transform | DQ |
|---|---|---|---|---|---|
| 1 (ครั้งแรก) | [`run1_first.txt`](docs/evidence/run1_first.txt) | `20261111-162657-f2d15e` | DOWNLOADED 24 | processed 24, rows_written 149,655,796 | PASS 193 / WARN 0 / FAIL 0 |
| 2 (รันซ้ำ) | [`run2_rerun.txt`](docs/evidence/run2_rerun.txt) | `20261111-163036-259e31` | **SKIPPED 24** | **processed 0, skipped 24** | PASS 193 / WARN 0 / FAIL 0 |

- ผลลัพธ์ทั้งสองรอบเท่ากัน: HVFHV = 120,864,668 trips (แสดงว่ารันซ้ำได้ผลเดิม)
- Row reconciliation ผ่านทุกไฟล์: raw = curated (เช่น `fhvhv_tripdata_2024-01`: 19,663,930 → 19,663,930)
- สถานะแต่ละ step: `reports/run_log.jsonl`
- สถานะไฟล์ต้นทาง (url, ขนาด, sha256): `reports/manifest.csv`
- ผล DQ ทั้ง 193 checks: `reports/dq_report.csv` / `.json`
- จำนวนแถวที่ถูกติดธง invalid ทั้งหมดมีเพียง 45 แถว จาก 149,655,796 แถว (ตรวจได้จากคอลัมน์ `invalid_records` ใน `reports/monthly_summary.csv`) จึงไม่เกินเกณฑ์ WARN

## 6. ตัวอย่างข้อมูลและผลลัพธ์
- `data/sample/trips_sample.csv` — ตัวอย่าง curated ~2,500 แถวต่อ type
- `reports/monthly_summary.csv`, `reports/taxi_type_ranking.csv`, `reports/answer.md`, `reports/trips_by_type.png`
- Trace กลับ: `answer.md` → `monthly_summary.csv` → `data/curated` (คอลัมน์ `source_file`) → `manifest.csv` (url, sha256) → TLC

| Taxi type | Valid trips |
|---|---:|
| HVFHV | 120,864,668 |
| Yellow Taxi | 20,332,054 |
| FHV | 8,119,228 |
| Green Taxi | 339,801 |

## 7. Data Quality Checks
file completeness • source available • pickup column exists • row count > 0 •
**row reconciliation** (raw = curated = valid + null + out_of_range) • null pickup ratio •
out-of-range ratio • dropoff < pickup ratio • **valid recount จาก raw เทียบ curated** • duplicates (optional)
- `FAIL` → หยุดก่อน analyze  |  `WARN` → บันทึกแต่ไปต่อ

## 8. Design Decisions
1. **ติดธงแทนการลบ (`invalid_reason`, `is_valid`)** — ตรวจย้อนกลับได้ว่าตัดแถวใดเพราะอะไร และ reconcile ให้ครบ (แลกกับไฟล์ curated ใหญ่ขึ้นเล็กน้อย)
2. **เก็บ curated เฉพาะ column ที่จำเป็น + partition ตาม taxi/month แล้ว overwrite ทีละ partition** — ลดขนาดข้อมูล ทำให้ transform รันซ้ำแล้วได้ผลเดิม ไม่เกิดแถวซ้ำ
3. **ตรวจ ETag/size/Last-Modified + sha256 ใน manifest เพื่อข้ามไฟล์เดิม** — ไม่โหลดซ้ำหลาย GB และรู้ว่าไฟล์ใดเปลี่ยน
4. **Fail-fast เมื่อ DQ FAIL** — ไม่ตอบคำถามจากข้อมูลที่พิสูจน์ไม่ได้ว่าครบ
5. **หา column แบบไม่สนตัวพิมพ์ + mapping ใน config** — FHV ใช้ชื่อ `PUlocationID`/`dropOff_datetime` ต่างจากประเภทอื่น

## 9. Submission 2 พัฒนาจาก Submission 1 อย่างไร
| | Submission 1 | Submission 2 |
|---|---|---|
| รูปแบบ | notebook เดียว | repo แยก module + config + tests |
| การจัดเก็บ | query URL ตรง ไม่เก็บ | raw + curated Parquet + manifest |
| Curated output | ไม่มี | `trips_unified` schema กลาง |
| Data quality | null/ช่วงเวลา ตรวจด้วยตา | 193 checks, PASS/WARN/FAIL, fail-fast |
| รันซ้ำ/อัปเดต | ทำใหม่ทั้งหมด | ข้ามไฟล์ที่ไม่เปลี่ยน, idempotent |
| หลักฐานสถานะ | output ใน notebook | manifest + run_log + dq_report |

ผลลัพธ์ตรงกับ Submission 1 ทุกตัวเลข (HVFHV = 120,864,668) แสดงว่าการย้ายมาเป็น pipeline ไม่ทำให้คำตอบเปลี่ยน

## 10. Learning Reflection
จากงานนี้เข้าใจว่า pipeline ต่างจาก notebook ตรงที่ต้องคิดถึงการรันซ้ำและการตรวจสอบตั้งแต่ออกแบบ ไม่ใช่แค่ทำให้ได้คำตอบครั้งเดียว เช่น การเก็บ manifest และ sha256 ทำให้รันรอบสองข้ามไฟล์ทั้ง 24 ไฟล์ได้และได้ผลเท่าเดิม ข้อมูลแต่ละ taxi type มีชื่อ column ต่างกัน (เช่น FHV ใช้ `PUlocationID`) จึงต้องทำ mapping ใน config แทนการเขียนฝังในโค้ด และการแยก WARN ออกจาก FAIL ช่วยให้ pipeline หยุดเฉพาะกรณีที่ข้อมูลไม่น่าเชื่อถือจริง ๆ ส่วนที่ยังต้องปรับปรุงคือการเปลี่ยน period ที่ทำให้ transform ใหม่ทั้งหมด และการตั้ง scheduler ให้รันอัตโนมัติ

## 11. ข้อจำกัดของ Pipeline ปัจจุบัน
- ถ้าเปลี่ยน `period` ต้อง transform ใหม่ทั้งหมด เพราะกำหนด valid จากช่วงเวลารวม
- ตรวจการเปลี่ยนของไฟล์จาก header ของ TLC; ถ้า TLC แก้ไฟล์โดย header ไม่เปลี่ยนจะตรวจไม่พบ
- นับ trip records ไม่ใช่ "ผู้ใช้จริง"; HVFHV (Uber/Lyft ฯลฯ) และ Yellow อาจนิยามหนึ่ง record ต่างกัน
- FHV มี column น้อย จึงตรวจ quality ได้จำกัด
- ยังรันด้วยมือ ไม่มี scheduler (ต่อยอดด้วย cron / GitHub Actions ได้)
- ทดสอบอัตโนมัติใช้ข้อมูลจำลอง ไม่ได้ครอบคลุมความผิดปกติทุกแบบของข้อมูลจริง
