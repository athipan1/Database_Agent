# Database Gateway Boundary

ระบบนี้ใช้ `Database_Agent` เป็น service เดียวที่เชื่อมต่อ Railway PostgreSQL โดยตรง

## Correct Architecture

```text
Manager_Agent
Learning_Agent
Execution_Agent
Risk_Agent
Technical_Agent
Fundamental_Agent
Scanner_Agent
        ↓ HTTP API
Database_Agent
        ↓ DATABASE_URL
Railway PostgreSQL
```

## Secret Placement

### Database_Agent only

Database_Agent ต้องมี:

```env
DATABASE_URL=postgresql://...
DATABASE_AGENT_API_KEY=...
DATABASE_DEV_MODE=false
TRADING_MODE=PAPER
```

`DATABASE_URL` ควรเป็น Railway private connection URL เมื่อ Database_Agent และ PostgreSQL อยู่ใน project/environment เดียวกัน

### Other Agents

Agent อื่นไม่ควรมี `DATABASE_URL`

ให้ใช้:

```env
DATABASE_AGENT_URL=https://your-database-agent-service
DATABASE_AGENT_API_KEY=your-api-key
```

## Why

- ลดการกระจาย database password
- ทำให้ Database_Agent เป็น single source of truth
- audit ง่ายกว่า
- ป้องกัน service อื่นยิง SQL ตรงเข้า production database

## Railway Readiness

Database_Agent มี endpoint สองระดับ:

```text
GET /health  process และสถานะโดยรวม ตอบ HTTP 200 พร้อมรายละเอียดสถานะ
GET /ready   PostgreSQL readiness ตอบ HTTP 503 จนกว่าจะเชื่อมฐานข้อมูลได้
```

Railway หรือ container healthcheck ควรใช้ `/ready` เพื่อไม่ปล่อย instance ที่เปิด Uvicorn ได้ แต่ PostgreSQL ยังไม่พร้อม เข้ารับ traffic

ผลที่พร้อมใช้งานต้องมี:

```json
{
  "data": {
    "database_connection": "connected"
  }
}
```

## Secret-safe Write/Read Smoke Test

Repository มี workflow `Railway Database API Smoke` สำหรับทดสอบ production โดยไม่ส่ง `DATABASE_URL` ออกนอก Database_Agent

ตั้งค่าที่ GitHub repository:

```text
Secret: DATABASE_AGENT_API_KEY
Variable หรือ Secret: DATABASE_AGENT_URL
```

หรือใส่ public Database_Agent URL เป็น input ตอนกด `Run workflow`

Workflow จะทำตามลำดับ:

1. รอ `GET /ready` จน PostgreSQL connected
2. เขียน synthetic signal ที่ symbol `ZZTEST`
3. อ่านแถวเดิมกลับจาก `/history/signals`
4. ตรวจ `metadata.synthetic=true` และ `safe_for_trading=false`
5. แสดงเฉพาะผลที่กรอง secret แล้วใน Job Summary

แถว diagnostic ไม่สร้าง order, fill, position หรือคำสั่งเทรด

## Current Persistence Target

Database_Agent should persist these tables in PostgreSQL:

- `accounts`
- `positions`
- `orders`
- `ledger`
- `prices`
- `signal_history`
- `performance_metrics`

`signal_history` and `performance_metrics` are handled by `history_repository.py`.
