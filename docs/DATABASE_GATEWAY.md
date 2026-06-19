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
```

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
