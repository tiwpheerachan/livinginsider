# LivingInsider Scraper + Web UI (Excel/CSV)

โปรเจกต์นี้ทำให้พี่สามารถ:
- ใส่ **Start URL** ของ LivingInsider (หน้า search)
- เลือก filter เบื้องต้น: ประเภทประกาศ (ขาย/ให้เช่า/เซ้ง), ประเภทอสังหา, keyword, ช่วงราคา
- กด **Run Scrape** เพื่อดึงข้อมูล
- Export ออกเป็น **CSV** หรือ **XLSX (Excel)** ได้ทันที
- Copy สูตร `IMPORTDATA()` เพื่อใช้ใน Google Sheets

> หมายเหตุ: เว็บอาจมีการเปลี่ยน DOM/คลาสได้ตลอดเวลา
> หากบางฟิลด์ไม่ขึ้น ให้ส่งตัวอย่างหน้า detail มา 1 ลิงก์ แล้วจะปรับ selector ให้ตรง 100%

## 1) ติดตั้ง

ต้องมี Node.js 18+ (แนะนำ 20+)

```bash
cd livinginsider-scraper
npm install
```

Playwright จะติดตั้ง Chromium ให้อัตโนมัติ (ผ่าน postinstall)

## 2) รัน

```bash
npm start
```

เปิดเว็บ: http://localhost:3000

## 3) ฟิลด์ที่ดึง (ตามที่ขอ)

- ชื่อโครงการ + URL โครงการ (ถ้าหาเจอ)
- ชื่อประกาศ
- ราคา (text + ตัวเลข)
- ประกาศเมื่อ / active เมื่อ (ข้อความ)
- URL ประกาศ
- ยอดดู / ยอดคลิก (ถ้าหาเจอจาก header/ไอคอน)
- ชื่อคนประกาศ + URL คนประกาศ (ถ้าหาเจอ)
- สเปคอสังหา: พื้นที่ใช้สอย, ชั้น, ห้องนอน, ห้องน้ำ, ที่จอด ฯลฯ (ดึงจากตาราง/kv แล้ว normalize)
- รายละเอียด/คำอธิบาย (best-effort)
- จุดเด่น/nearby/location links (best-effort)

## 4) Environment Variables (ปรับความเร็ว/ความทน)

```bash
PORT=3000
SCRAPE_HEADLESS=true
SCRAPE_NAV_TIMEOUT=45000
SCRAPE_LIST_SCROLL_ROUNDS=10
SCRAPE_LIST_SCROLL_STEP=1200
SCRAPE_DETAIL_TIMEOUT_MS=60000
SCRAPE_DETAIL_RETRIES=2
SCRAPE_MAX_CONCURRENCY=5
CACHE_TTL_MS=600000
```

## 5) Export

- CSV: ปุ่ม Export CSV (server จะดาวน์โหลดจาก `/api/export.csv?jobId=...`)
- XLSX: ปุ่ม Export XLSX (server จะดาวน์โหลดจาก `/api/export.xlsx?jobId=...`)

## 6) ข้อควรระวัง

- การ scrape ควรทำด้วยความถี่เหมาะสม
- บางประกาศต้องกด “ดูเบอร์/Contact” เพื่อแสดงข้อมูลติดต่อ (สคริปต์พยายามกดให้แล้ว)
- ถ้าเว็บไซต์เริ่ม block ให้ลด concurrency หรือเพิ่ม delay (สามารถเพิ่มได้ใน src/scraper.js)
