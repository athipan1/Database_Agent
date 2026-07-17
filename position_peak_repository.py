from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional


def _decimal_or_none(value: Any) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def highest_price_since_entry_for_sync(
    *,
    entry_price: Any,
    current_price: Any,
    existing_position: Optional[Dict[str, Any]] = None,
) -> Decimal:
    """Return the durable peak for one still-open position lifecycle."""
    entry = _decimal_or_none(entry_price) or Decimal("0")
    current = _decimal_or_none(current_price) or entry
    existing_peak = _decimal_or_none(
        (existing_position or {}).get("highest_price_since_entry")
    )
    return max(value for value in (entry, current, existing_peak) if value is not None)


def _install_sqlite_triggers(cursor) -> None:
    cursor.executescript(
        """
        CREATE TRIGGER IF NOT EXISTS trg_positions_highest_price_insert
        AFTER INSERT ON positions
        BEGIN
            UPDATE positions
            SET highest_price_since_entry = CAST(
                CASE
                    WHEN NEW.highest_price_since_entry IS NOT NULL
                         AND TRIM(CAST(NEW.highest_price_since_entry AS TEXT)) <> ''
                         AND CAST(NEW.highest_price_since_entry AS NUMERIC) >= CAST(NEW.average_cost AS NUMERIC)
                         AND (
                             NEW.current_market_price IS NULL
                             OR TRIM(CAST(NEW.current_market_price AS TEXT)) = ''
                             OR CAST(NEW.highest_price_since_entry AS NUMERIC) >= CAST(NEW.current_market_price AS NUMERIC)
                         )
                    THEN CAST(NEW.highest_price_since_entry AS NUMERIC)
                    WHEN NEW.current_market_price IS NOT NULL
                         AND TRIM(CAST(NEW.current_market_price AS TEXT)) <> ''
                         AND CAST(NEW.current_market_price AS NUMERIC) > CAST(NEW.average_cost AS NUMERIC)
                    THEN CAST(NEW.current_market_price AS NUMERIC)
                    ELSE CAST(NEW.average_cost AS NUMERIC)
                END AS TEXT
            )
            WHERE position_id = NEW.position_id;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_positions_highest_price_update
        AFTER UPDATE OF average_cost, current_market_price ON positions
        BEGIN
            UPDATE positions
            SET highest_price_since_entry = CAST(
                CASE
                    WHEN NEW.current_market_price IS NOT NULL
                         AND TRIM(CAST(NEW.current_market_price AS TEXT)) <> ''
                         AND CAST(NEW.current_market_price AS NUMERIC) >
                             CAST(COALESCE(NULLIF(OLD.highest_price_since_entry, ''), OLD.average_cost) AS NUMERIC)
                         AND CAST(NEW.current_market_price AS NUMERIC) >= CAST(NEW.average_cost AS NUMERIC)
                    THEN CAST(NEW.current_market_price AS NUMERIC)
                    WHEN CAST(NEW.average_cost AS NUMERIC) >
                         CAST(COALESCE(NULLIF(OLD.highest_price_since_entry, ''), OLD.average_cost) AS NUMERIC)
                    THEN CAST(NEW.average_cost AS NUMERIC)
                    ELSE CAST(COALESCE(NULLIF(OLD.highest_price_since_entry, ''), OLD.average_cost) AS NUMERIC)
                END AS TEXT
            )
            WHERE position_id = NEW.position_id;
        END;
        """
    )


def _install_postgres_trigger(cursor) -> None:
    cursor.execute(
        """
        CREATE OR REPLACE FUNCTION maintain_position_highest_price_since_entry()
        RETURNS TRIGGER AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                NEW.highest_price_since_entry := GREATEST(
                    NEW.average_cost,
                    COALESCE(NEW.current_market_price, NEW.average_cost),
                    COALESCE(NEW.highest_price_since_entry, NEW.average_cost)
                );
            ELSE
                NEW.highest_price_since_entry := GREATEST(
                    COALESCE(OLD.highest_price_since_entry, OLD.average_cost),
                    NEW.average_cost,
                    COALESCE(NEW.current_market_price, NEW.average_cost)
                );
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """
    )
    cursor.execute("DROP TRIGGER IF EXISTS trg_positions_highest_price ON positions")
    cursor.execute(
        """
        CREATE TRIGGER trg_positions_highest_price
        BEFORE INSERT OR UPDATE OF average_cost, current_market_price
        ON positions
        FOR EACH ROW
        EXECUTE FUNCTION maintain_position_highest_price_since_entry()
        """
    )


def setup_position_peak_tracking(db) -> None:
    """Apply the additive position peak migration and MAX-semantics guards."""
    numeric_type = "TEXT" if db.db_type == "sqlite" else "NUMERIC(18, 5)"
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            db._add_column_if_not_exists(
                cursor,
                "positions",
                "highest_price_since_entry",
                numeric_type,
            )
            if db.db_type == "sqlite":
                cursor.execute(
                    """
                    UPDATE positions
                    SET highest_price_since_entry = CAST(
                        CASE
                            WHEN current_market_price IS NOT NULL
                                 AND TRIM(CAST(current_market_price AS TEXT)) <> ''
                                 AND CAST(current_market_price AS NUMERIC) > CAST(average_cost AS NUMERIC)
                            THEN CAST(current_market_price AS NUMERIC)
                            ELSE CAST(average_cost AS NUMERIC)
                        END AS TEXT
                    )
                    WHERE highest_price_since_entry IS NULL
                       OR TRIM(CAST(highest_price_since_entry AS TEXT)) = ''
                    """
                )
                _install_sqlite_triggers(cursor)
            else:
                cursor.execute(
                    """
                    UPDATE positions
                    SET highest_price_since_entry = GREATEST(
                        average_cost,
                        COALESCE(current_market_price, average_cost)
                    )
                    WHERE highest_price_since_entry IS NULL
                    """
                )
                _install_postgres_trigger(cursor)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
