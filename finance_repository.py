from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from fastapi import HTTPException

from finance_models import (
    CreateFinanceEntryBody,
    FinanceBudgets,
    FinanceEntry,
    PersonalFinanceState,
    UpsertFinanceBudgetsBody,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _row_get(row: Any, key: str, index: int = 0) -> Any:
    try:
        return row[key]
    except Exception:
        return row[index]


def setup_finance_tables(db) -> None:
    timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
    money_type = "TEXT" if db.db_type == "sqlite" else "NUMERIC(18, 2)"
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS personal_finance_entries (
                    entry_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    entry_type TEXT NOT NULL,
                    amount {money_type} NOT NULL,
                    currency TEXT NOT NULL DEFAULT 'THB',
                    category TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    occurred_at {timestamp_type} NOT NULL,
                    created_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP,
                    updated_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS personal_finance_budgets (
                    account_id TEXT PRIMARY KEY,
                    personal_investment_budget_thb {money_type} NOT NULL DEFAULT 0,
                    trade_plan_limit_usd {money_type} NOT NULL DEFAULT 0,
                    updated_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_personal_finance_entries_account_occurred "
                "ON personal_finance_entries(account_id, occurred_at)"
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def _format_entry(row: Any) -> FinanceEntry:
    return FinanceEntry(
        entry_id=_row_get(row, "entry_id", 0),
        account_id=str(_row_get(row, "account_id", 1)),
        entry_type=str(_row_get(row, "entry_type", 2)).lower(),
        amount=Decimal(str(_row_get(row, "amount", 3))),
        currency=str(_row_get(row, "currency", 4)).upper(),
        category=_row_get(row, "category", 5),
        description=_row_get(row, "description", 6) or "",
        occurred_at=_parse_dt(_row_get(row, "occurred_at", 7)),
        created_at=_parse_dt(_row_get(row, "created_at", 8)),
        updated_at=_parse_dt(_row_get(row, "updated_at", 9)),
    )


def _format_budgets(row: Any, account_id: str) -> FinanceBudgets:
    if not row:
        return FinanceBudgets(account_id=account_id)
    return FinanceBudgets(
        account_id=str(_row_get(row, "account_id", 0)),
        personal_investment_budget_thb=Decimal(str(_row_get(row, "personal_investment_budget_thb", 1))),
        trade_plan_limit_usd=Decimal(str(_row_get(row, "trade_plan_limit_usd", 2))),
        updated_at=_parse_dt(_row_get(row, "updated_at", 3)),
    )


def create_finance_entry(db, body: CreateFinanceEntryBody) -> FinanceEntry:
    setup_finance_tables(db)
    now = _now_iso()
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                INSERT INTO personal_finance_entries
                    (entry_id, account_id, entry_type, amount, currency, category, description,
                     occurred_at, created_at, updated_at)
                VALUES ({db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style})
                """,
                (
                    body.entry_id,
                    str(body.account_id),
                    body.entry_type,
                    str(body.amount),
                    body.currency,
                    body.category,
                    body.description,
                    body.occurred_at.isoformat(),
                    now,
                    now,
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    return get_finance_entry(db, body.entry_id)


def get_finance_entry(db, entry_id: str) -> FinanceEntry:
    setup_finance_tables(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"SELECT * FROM personal_finance_entries WHERE entry_id = {db.param_style}",
                (entry_id,),
            )
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"Finance entry {entry_id} not found")
            return _format_entry(row)
        finally:
            cursor.close()


def delete_finance_entry(db, entry_id: str, account_id: str) -> None:
    setup_finance_tables(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"DELETE FROM personal_finance_entries "
                f"WHERE entry_id = {db.param_style} AND account_id = {db.param_style}",
                (entry_id, str(account_id)),
            )
            if cursor.rowcount != 1:
                raise HTTPException(status_code=404, detail=f"Finance entry {entry_id} not found")
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def list_finance_entries(db, account_id: str, limit: int = 2000) -> list[FinanceEntry]:
    setup_finance_tables(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"SELECT * FROM personal_finance_entries "
                f"WHERE account_id = {db.param_style} "
                f"ORDER BY occurred_at DESC, created_at DESC LIMIT {db.param_style}",
                (str(account_id), limit),
            )
            return [_format_entry(row) for row in cursor.fetchall()]
        finally:
            cursor.close()


def get_finance_budgets(db, account_id: str) -> FinanceBudgets:
    setup_finance_tables(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"SELECT * FROM personal_finance_budgets WHERE account_id = {db.param_style}",
                (str(account_id),),
            )
            return _format_budgets(cursor.fetchone(), str(account_id))
        finally:
            cursor.close()


def upsert_finance_budgets(db, account_id: str, body: UpsertFinanceBudgetsBody) -> FinanceBudgets:
    setup_finance_tables(db)
    now = _now_iso()
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                INSERT INTO personal_finance_budgets
                    (account_id, personal_investment_budget_thb, trade_plan_limit_usd, updated_at)
                VALUES ({db.param_style}, {db.param_style}, {db.param_style}, {db.param_style})
                ON CONFLICT(account_id) DO UPDATE SET
                    personal_investment_budget_thb = excluded.personal_investment_budget_thb,
                    trade_plan_limit_usd = excluded.trade_plan_limit_usd,
                    updated_at = excluded.updated_at
                """,
                (
                    str(account_id),
                    str(body.personal_investment_budget_thb),
                    str(body.trade_plan_limit_usd),
                    now,
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    return get_finance_budgets(db, str(account_id))


def get_personal_finance_state(db, account_id: str, limit: int = 2000) -> PersonalFinanceState:
    return PersonalFinanceState(
        account_id=str(account_id),
        entries=list_finance_entries(db, str(account_id), limit=limit),
        budgets=get_finance_budgets(db, str(account_id)),
    )
