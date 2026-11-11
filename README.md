# NYC Taxi — Reproducible Data Pipeline (Submission 2)

**คำถาม:** Which taxi type was the most used in the first half of 2024?
**ชื่อ–รหัส:** ชนิภรณ์ คิมประเสริฐ 6709650243

> **คำตอบ:** _(ใส่หลังรันจริง — ดู `reports/answer.md`)_  ผล Baseline (Submission 1) คือ **HVFHV = 120,864,668 trips**

## 1. ข้อมูลที่ใช้
- แหล่ง: NYC TLC Trip Record Data — https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- 4 ประเภท (Yellow, Green, FHV, HVFHV) x มกราคม–มิถุนายน 2024 = **24 ไฟล์ Parquet**
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
git clone <repo-url> && cd nyc-taxi-pipeline
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
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

## 5. หลักฐานการรัน (ต้องเติมหลังรันจริง)
เก็บ log ไว้ที่ `docs/evidence/`:
```bash
python run_pipeline.py 2>&1 | tee docs/evidence/run1_first.txt     # DOWNLOADED 24
python run_pipeline.py 2>&1 | tee docs/evidence/run2_rerun.txt     # SKIPPED 24, transform processed=0
```
- สถานะแต่ละ step: `reports/run_log.jsonl`
- สถานะไฟล์ต้นทาง: `reports/manifest.csv`
- ผล DQ: `reports/dq_report.csv` — _สรุปผลจริง: PASS=__ WARN=__ FAIL=__ (เติม)_

## 6. ตัวอย่างข้อมูลและผลลัพธ์
- `data/sample/trips_sample.csv` — ตัวอย่าง curated ~2,500 แถวต่อ type
- `reports/monthly_summary.csv`, `reports/taxi_type_ranking.csv`, `reports/answer.md`, `reports/trips_by_type.png`
- Trace กลับ: `answer.md` → `monthly_summary.csv` → `data/curated` (คอลัมน์ `source_file`) → `manifest.csv` (url, sha256) → TLC

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
| Data quality | null/ช่วงเวลา ตรวจด้วยตา | ~10 checks, PASS/WARN/FAIL, fail-fast |
| รันซ้ำ/อัปเดต | ทำใหม่ทั้งหมด | ข้ามไฟล์ที่ไม่เปลี่ยน, idempotent |
| หลักฐานสถานะ | output ใน notebook | manifest + run_log + dq_report |

## 10. Learning Reflection
_(เขียนเองด้วยคำของตัวเอง 3–5 ประโยค เช่น: สิ่งที่เรียนรู้เรื่องความต่างของ schema, การทำ idempotent, การแยก WARN/FAIL, และสิ่งที่จะปรับปรุง)_

## 11. ข้อจำกัดของ Pipeline ปัจจุบัน
- ถ้าเปลี่ยน `period` ต้อง transform ใหม่ทั้งหมด เพราะกำหนด valid จากช่วงเวลารวม
- ตรวจการเปลี่ยนของไฟล์จาก header ของ TLC; ถ้า TLC แก้ไฟล์โดย header ไม่เปลี่ยนจะตรวจไม่พบ
- นับ trip records ไม่ใช่ "ผู้ใช้จริง"; HVFHV (Uber/Lyft ฯลฯ) และ Yellow อาจนิยามหนึ่ง record ต่างกัน
- FHV มี column น้อย จึงตรวจ quality ได้จำกัด
- ยังรันด้วยมือ ไม่มี scheduler (ต่อยอดด้วย cron / GitHub Actions ได้)
- ทดสอบอัตโนมัติใช้ข้อมูลจำลอง ไม่ได้ครอบคลุมความผิดปกติทุกแบบของข้อมูลจริง
