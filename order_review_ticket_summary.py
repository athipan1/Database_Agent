from collections import Counter
from typing import Any, Dict, Optional

from order_review_ticket_models import ListOrderReviewTicketsQuery
from order_review_ticket_repository import get_order_review_ticket_audit, list_order_review_ticket_audits


def get_latest_order_review_ticket_audit(db, account_id: Optional[str] = None, source: Optional[str] = None):
    records = list_order_review_ticket_audits(
        db,
        ListOrderReviewTicketsQuery(
            account_id=account_id,
            source=source,
            limit=1,
            offset=0,
            sort="updated_at",
            order="desc",
        ),
    )
    return records[0] if records else None


def get_order_review_ticket_summary(
    db,
    account_id: Optional[str] = None,
    source: Optional[str] = None,
    latest_ticket_id: Optional[str] = None,
) -> Dict[str, Any]:
    records = list_order_review_ticket_audits(
        db,
        ListOrderReviewTicketsQuery(
            account_id=account_id,
            source=source,
            limit=500,
            offset=0,
            sort="updated_at",
            order="desc",
        ),
    )
    status_counts = Counter(record.status for record in records)
    latest_ticket = get_order_review_ticket_audit(db, latest_ticket_id) if latest_ticket_id else (records[0] if records else None)

    return {
        "total_count": len(records),
        "ready_ticket_count": status_counts.get("ready_for_manual_approval", 0),
        "blocked_ticket_count": status_counts.get("blocked", 0),
        "created_ticket_count": status_counts.get("created", 0),
        "executed_ticket_count": status_counts.get("executed", 0),
        "rejected_ticket_count": status_counts.get("rejected", 0),
        "approval_required_count": sum(1 for record in records if record.approval_required),
        "execution_enabled_count": sum(1 for record in records if record.execution_enabled),
        "total_ready_items": sum(record.ready_count for record in records),
        "total_blocked_items": sum(record.blocked_count for record in records),
        "status_counts": [
            {"status": status, "count": count}
            for status, count in sorted(status_counts.items())
        ],
        "latest_ticket": latest_ticket,
    }
