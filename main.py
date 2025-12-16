# In Database_Agent/main.py
from fastapi import FastAPI, HTTPException
from typing import List
import logging

from trading_db import TradingDB
from models import AccountBalance, Position, Order, CreateOrderBody, CreateOrderResponse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI(title="Database Agent")

# สร้าง instance ของ TradingDB เพื่อใช้ตลอดการทำงานของแอปพลิเคชัน
# ใช้ db_file="trading_persistent.db" เพื่อให้ข้อมูลถูกบันทึกถาวรข้าม session
db = TradingDB(db_file="trading_persistent.db")
db.setup_database() # ตรวจสอบและสร้างตารางถ้ายังไม่มี

@app.on_event("startup")
async def startup_event():
    logging.info("Database Agent API starting up.")
    # ไม่ต้องทำอะไรเป็นพิเศษ เพราะ db ถูกสร้างแล้ว

@app.on_event("shutdown")
async def shutdown_event():
    logging.info("Database Agent API shutting down.")
    # การเชื่อมต่อ db จะถูกปิดโดยอัตโนมัติเมื่อ object ถูกทำลาย

# --- API Endpoints ---

@app.get("/accounts/{account_id}/balance", response_model=AccountBalance)
async def get_balance(account_id: int):
    """ดึงข้อมูลยอดเงินคงเหลือ"""
    balance = db.get_account_balance(account_id)
    if balance is None:
        raise HTTPException(status_code=404, detail="Account not found")
    return AccountBalance(cash_balance=balance)

@app.get("/accounts/{account_id}/positions", response_model=List[Position])
async def get_positions_for_account(account_id: int):
    """ดึงข้อมูลหุ้นทั้งหมดในพอร์ต"""
    positions = db.get_positions(account_id)
    return positions

@app.get("/accounts/{account_id}/orders", response_model=List[Order])
async def get_order_history_for_account(account_id: int):
    """ดึงประวัติคำสั่งซื้อขายทั้งหมด"""
    orders = db.get_order_history(account_id)
    return orders

@app.post("/accounts/{account_id}/orders", response_model=CreateOrderResponse)
async def create_new_order(account_id: int, order_body: CreateOrderBody):
    """สร้างคำสั่งซื้อขายใหม่ (สถานะเริ่มต้นคือ 'pending')"""
    order_id = db.create_order(
        account_id=account_id,
        symbol=order_body.symbol,
        order_type=order_body.order_type,
        quantity=order_body.quantity,
        price=order_body.price
    )
    if order_id is None:
        raise HTTPException(status_code=500, detail="Failed to create order")
    return CreateOrderResponse(order_id=order_id, status="pending")

@app.post("/orders/{order_id}/execute", response_model=Order)
async def execute_existing_order(order_id: int):
    """ยืนยันการซื้อขาย (execute) จาก order ที่เป็น pending"""
    # ต้องดึงข้อมูล order ก่อน execute เพื่อดูผลลัพธ์
    cursor = db.get_cursor()
    cursor.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,))
    order_before = cursor.fetchone()

    if not order_before or order_before['status'] != 'pending':
        raise HTTPException(status_code=404, detail=f"Pending order with ID {order_id} not found.")

    db.execute_order(order_id)

    # ดึงข้อมูล order อีกครั้งหลัง execute เพื่อดูสถานะล่าสุด
    cursor.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,))
    order_after = cursor.fetchone()

    if not order_after:
         raise HTTPException(status_code=500, detail="Order disappeared after execution attempt.")

    return order_after
