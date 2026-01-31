# Database Agent

Database Agent เป็นบริการหลักในระบบเทรด (Trading System) ที่พัฒนาด้วย FastAPI โดยทำหน้าที่จัดการการโต้ตอบกับฐานข้อมูลทั้งหมด เป็นแหล่งเก็บข้อมูลกลาง (Single Source of Truth) ที่มีความปลอดภัยสูงและรองรับการทำ Transaction ที่ซับซ้อน เพื่อให้มั่นใจในความถูกต้องและตรวจสอบได้ของข้อมูลการเทรดทั้งหมด

## ความรับผิดชอบหลัก

1. **การบันทึกการตัดสินใจ (Decision Logging):** บันทึกทุกคำสั่งซื้อขายและสถานะ เพื่อใช้ในการวิเคราะห์ย้อนหลังและรักษาระดับ Traceability
2. **การติดตามผลลัพธ์ (Outcome Tracking):** จัดเก็บผลการดำเนินงานจริงจากการเทรด เพื่อนำไปคำนวณกำไร/ขาดทุน และ Drawdown
3. **แหล่งข้อมูลสำหรับการเรียนรู้ (Data Source for Learning):** ให้บริการข้อมูลราคาประวัติศาสตร์และผลการเทรดแก่ Learning Agent เพื่อใช้ประเมินประสิทธิภาพและปรับปรุงกลยุทธ์
4. **การตรวจสอบระบบ (System Auditing):** สร้าง Audit Trail ที่สมบูรณ์ผ่านระบบ Ledger เพื่อติดตามทุกความเคลื่อนไหวของเงินและสินทรัพย์ในระดับ Transaction

---

## รายละเอียดการทำงานเชิงลึก

### 1. ระบบ Traceability และ Security
* **Correlation ID Middleware:** ระบบจะแนบ `X-Correlation-ID` ไปกับทุก Log และ Response Header ทำให้สามารถติดตามเส้นทางการทำงานของแต่ละ Request ได้อย่างแม่นยำแม้ในระบบที่มีการทำงานแบบขนานหรือ Distributed
* **API Key Protection:** ปกป้องข้อมูลสำคัญด้วยการตรวจสอบ API Key ผ่าน Header `X-API-KEY` เพื่ออนุญาตเฉพาะเอเจนต์ที่ได้รับสิทธิ์เท่านั้น โดยมีการตั้งค่าผ่านสภาพแวดล้อม (Environment Variable)

### 2. การจัดการ Transaction (ACID Properties)
* **Atomic Order Execution:** ในขั้นตอนการรันคำสั่งซื้อขาย (`execute_order`) ระบบจะทำงานภายใน Database Transaction ชุดเดียวแบบ Atomic โดยจะครอบคลุมขั้นตอน:
    - ตรวจสอบยอดเงินหรือจำนวนสินทรัพย์ที่พร้อมเทรด
    - หักเงินหรือสินทรัพย์จากบัญชี
    - อัปเดตพอร์ตการลงทุน (Positions)
    - บันทึกประวัติการเปลี่ยนแปลงลงใน Ledger
    - อัปเดตสถานะคำสั่งซื้อขายเป็น `executed`
  หากขั้นตอนใดขั้นตอนหนึ่งล้มเหลว ระบบจะทำการ Rollback ทั้งหมดเพื่อป้องกันข้อมูลผิดเพี้ยน
* **Concurrency Control:** ใช้ระบบ Row-level Locking (`FOR UPDATE` ใน PostgreSQL หรือ `BEGIN IMMEDIATE` ใน SQLite) เพื่อป้องกันปัญหา Race Condition เมื่อมีการรันหลายรายการพร้อมกันในบัญชีเดียว

### 3. ระบบนำเข้าข้อมูลอัตโนมัติ (Background Scheduler)
* **Historical Data Ingestion:** มีระบบ Scheduler ที่ทำงานเป็น Thread แยกอยู่เบื้องหลัง เพื่อดึงข้อมูลราคา (OHLCV) จาก Alpaca API โดยอัตโนมัติตามช่วงเวลาที่กำหนด (เช่น ทุกเที่ยงคืน)
* **Reliability:** ใช้ระบบ Retry แบบ Exponential Backoff (ผ่าน `tenacity`) เมื่อการเชื่อมต่อกับ API ภายนอกขัดข้อง และใช้กลยุทธ์ `UPSERT` (On Conflict Do Nothing) เพื่อป้องกันการบันทึกข้อมูลราคาซ้ำซ้อนในฐานข้อมูล

---

## รายละเอียด API Endpoint

### การจัดการบัญชีและสินทรัพย์
* **`GET /accounts/{account_id}/balance`**: ดึงยอดเงินสดคงเหลือปัจจุบันของบัญชี
* **`GET /accounts/{account_id}/positions`**: ดึงรายการสินทรัพย์ที่ถือครองอยู่ทั้งหมด พร้อมจำนวนและราคาต้นทุนเฉลี่ย
* **`GET /accounts/{account_id}/executions`**: ดึงประวัติรายการเทรดที่รันสำเร็จแล้ว (Executed Trades) สามารถระบุช่วงวันที่ (start_date, end_date) ได้

### การจัดการคำสั่งซื้อขาย
* **`GET /accounts/{account_id}/orders`**: ดูประวัติคำสั่งซื้อขายทั้งหมดของบัญชี (รวมถึงสถานะ pending, executed, failed)
* **`POST /accounts/{account_id}/orders`**: สร้างคำสั่งซื้อขายใหม่ในสถานะ 'pending' โดยรองรับการใช้ `client_order_id` เพื่อความปลอดภัยในการเรียกซ้ำ (Idempotency)
* **`POST /orders/{order_id}/execute`**: สั่งประมวลผลคำสั่งซื้อขายที่ค้างอยู่จริง เป็น Endpoint หลักที่ทำการหักลบยอดเงินและอัปเดตพอร์ตการลงทุน

### ข้อมูลตลาด
* **`GET /prices/{symbol}`**: ดึงข้อมูลราคาประวัติศาสตร์ (OHLCV) จากฐานข้อมูลกลาง

---

## โครงสร้างข้อมูล (Data Schemas)

### AccountBalance
| Field | Type | Description |
| :--- | :--- | :--- |
| `cash_balance` | Decimal | ยอดเงินสดคงเหลือในบัญชี |

### Position
| Field | Type | Description |
| :--- | :--- | :--- |
| `symbol` | str | ชื่อสัญลักษณ์สินทรัพย์ (Ticker Symbol) |
| `quantity` | int | จำนวนหน่วยที่ถือครอง |
| `average_cost` | Decimal | ราคาต้นทุนเฉลี่ยต่อหน่วย (ใช้ในการคำนวณ PnL) |

### Order
| Field | Type | Description |
| :--- | :--- | :--- |
| `order_id` | int | ID ภายในระบบของคำสั่งซื้อขาย |
| `client_order_id` | UUID | ID ที่ทาง Client กำหนด เพื่อใช้อ้างอิงและป้องกันรายการซ้ำ |
| `symbol` | str | ชื่อสัญลักษณ์สินทรัพย์ |
| `order_type` | str | ประเภทการสั่ง: `BUY` (ซื้อ) หรือ `SELL` (ขาย) |
| `quantity` | int | จำนวนหน่วยที่ต้องการเทรด |
| `price` | Decimal | ราคาต่อหน่วยที่ระบุในคำสั่ง |
| `status` | str | สถานะปัจจุบัน: `pending`, `executed`, `cancelled`, `failed` |
| `failure_reason` | str | ระบุสาเหตุหากคำสั่งซื้อขายล้มเหลว (เช่น insufficient_funds) |
| `timestamp` | datetime | วันเวลาที่ระบบได้รับคำสั่งซื้อขาย |

### CreateOrderBody
| Field | Type | Description |
| :--- | :--- | :--- |
| `client_order_id` | UUID (optional) | ID สำหรับตรวจสอบรายการซ้ำ หากไม่ระบุระบบจะสร้างให้ใหม่ |
| `symbol` | str | ชื่อสัญลักษณ์สินทรัพย์ที่ต้องการเทรด |
| `order_type` | str | ประเภท: `BUY` หรือ `SELL` |
| `quantity` | int | จำนวนหน่วยที่ต้องการเทรด |
| `price` | Decimal | ราคาต่อหน่วยที่ต้องการ |

### ExecutionTrade
| Field | Type | Description |
| :--- | :--- | :--- |
| `trade_id` | int | ID ของรายการเทรดที่เกิดขึ้นจริง |
| `account_id` | int | ID ของบัญชีที่ทำรายการ |
| `symbol` | str | ชื่อสัญลักษณ์สินทรัพย์ |
| `side` | str | ด้านที่เทรด: `buy` หรือ `sell` |
| `quantity` | int | จำนวนที่เทรดได้จริง |
| `price` | Decimal | ราคาที่ใช้ในการเทรดจริง |
| `notional` | Decimal | มูลค่ารวมของรายการเทรด (price * quantity) |
| `executed_at` | str | วันและเวลาที่รายการสำเร็จในฐานข้อมูล |

### Price
| Field | Type | Description |
| :--- | :--- | :--- |
| `symbol` | str | ชื่อสัญลักษณ์สินทรัพย์ |
| `timestamp` | str | วันเวลาของแท่งราคา (ISO Format) |
| `open` | Decimal | ราคาเปิด |
| `high` | Decimal | ราคาสูงสุด |
| `low` | Decimal | ราคาต่ำสุด |
| `close` | Decimal | ราคาปิด |
| `volume` | int | ปริมาณการซื้อขายรวม |
