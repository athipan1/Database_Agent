from review_history_repository import (
    build_review_history_summary,
    create_review_history,
    get_latest_review_history_summary,
    get_review_history,
    list_review_history,
)


class FakeCursor:
    def __init__(self):
        self.statements = []
        self.runs = {}
        self.decisions = []
        self.last_result = None
        self.last_results = []

    def execute(self, sql, params=()):
        self.statements.append((sql, params))
        normalized = " ".join(sql.lower().split())
        if normalized.startswith("insert into review_runs"):
            self.runs[params[0]] = {
                "review_run_id": params[0],
                "account_id": params[1],
                "bucket": params[2],
                "mode": params[3],
                "source": params[4],
                "status": params[5],
                "generated_at": params[6],
                "correlation_id": params[7],
                "summary": params[8],
                "safety": params[9],
                "raw_report": params[10],
                "created_at": "now",
                "updated_at": params[11],
            }
        elif normalized.startswith("insert into review_decisions"):
            self.decisions.append({
                "decision_id": params[0],
                "review_run_id": params[1],
                "account_id": params[2],
                "bucket": params[3],
                "symbol": params[4],
                "profit_action": params[5],
                "risk_status": params[6],
                "preview_status": params[7],
                "final_decision": params[8],
                "reason": params[9],
                "position_snapshot": params[10],
                "profit_plan": params[11],
                "risk_result": params[12],
                "preview_result": params[13],
                "metadata": params[14],
                "created_at": "now",
            })
        elif "from review_runs where review_run_id" in normalized:
            self.last_result = self.runs.get(params[0])
        elif "from review_decisions where review_run_id" in normalized:
            self.last_results = [row for row in self.decisions if row["review_run_id"] == params[0]]
        elif normalized.startswith("select * from review_runs"):
            results = list(self.runs.values())
            if "where" in normalized:
                filters = list(params[:-1])
                for value in filters:
                    results = [row for row in results if value in (row["account_id"], row["bucket"])]
            self.last_results = results[: int(params[-1])] if params else results

    def fetchone(self):
        return self.last_result

    def fetchall(self):
        return self.last_results

    def close(self):
        pass


class FakeConnection:
    def __init__(self, cursor):
        self.cursor = cursor

    def commit(self):
        pass

    def rollback(self):
        pass


class FakeScope:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeDB:
    db_type = "sqlite"
    param_style = "?"

    def __init__(self):
        self.cursor = FakeCursor()
        self.conn = FakeConnection(self.cursor)

    def connection_scope(self):
        return FakeScope(self.conn)

    def get_cursor(self, conn):
        return conn.cursor


def sample_report():
    return {
        "generated_at": "2026-07-01T13:48:06Z",
        "bucket": "value_rebound",
        "mode": "BUCKET_PROFIT_REVIEW_REPORT_ONLY",
        "summary": {
            "positions_seen": 4,
            "reviewed_positions": 2,
            "database_bucket_hints_applied": 3,
            "profit_agent_used": 2,
            "risk_submissions": 2,
            "risk_approved": 2,
            "risk_rejected": 0,
            "execution_preview_submissions": 2,
            "execution_preview_ready": 2,
            "execution_preview_blocked": 0,
            "execution_submissions": 0,
        },
        "safety": {"advisory_only": True, "orders_submitted": False},
        "reviewed_positions": [
            {
                "symbol": "ACGL",
                "bucket": "value_rebound",
                "quantity": 82,
                "entry_price": 96.79,
                "current_price": 97.06,
                "stop_loss": 92.94,
                "has_protective_stop": True,
                "bucket_source": "database_agent",
                "profit_source": "profit_agent",
                "profit_plan": {
                    "primary_action": "hold",
                    "actions": [{"reason": "No take-profit or exit condition is triggered"}],
                },
                "risk_status": "approved",
                "risk_result": {"approved": True},
                "execution_preview_status": "ready",
                "execution_preview_result": {"approved_for_execution": True},
            },
            {
                "symbol": "CINF",
                "bucket": "value_rebound",
                "profit_plan": {
                    "primary_action": "hold",
                    "actions": [{"reason": "No take-profit or exit condition is triggered"}],
                },
                "risk_status": "approved",
                "execution_preview_status": "ready",
            },
        ],
    }


def test_create_review_history_persists_run_and_decisions():
    db = FakeDB()
    record = create_review_history(
        db,
        {
            "account_id": 1,
            "review_run_id": "review-test-1",
            "bucket": "value_rebound",
            "report": sample_report(),
        },
        correlation_id="corr-1",
    )

    assert record["review_run_id"] == "review-test-1"
    assert record["bucket"] == "value_rebound"
    assert len(record["decisions"]) == 2
    assert record["decisions"][0]["final_decision"] == "HOLD"


def test_get_and_list_review_history():
    db = FakeDB()
    create_review_history(db, {"review_run_id": "review-test-1", "report": sample_report()})
    fetched = get_review_history(db, "review-test-1")
    listed = list_review_history(db, bucket="value_rebound")

    assert fetched["review_run_id"] == "review-test-1"
    assert len(listed) == 1


def test_build_review_history_summary_compacts_decisions():
    db = FakeDB()
    record = create_review_history(db, {"review_run_id": "review-test-1", "account_id": 1, "report": sample_report()})

    summary = build_review_history_summary(record)

    assert summary["latest_review_run_id"] == "review-test-1"
    assert summary["reviewed_positions"] == 2
    assert summary["orders_submitted"] is False
    assert summary["final_decisions"] == {"HOLD": 2}
    assert summary["profit_actions"] == {"hold": 2}
    assert summary["risk_statuses"] == {"approved": 2}
    assert summary["preview_statuses"] == {"ready": 2}
    assert summary["decisions"][0]["symbol"] == "ACGL"


def test_get_latest_review_history_summary():
    db = FakeDB()
    create_review_history(db, {"review_run_id": "review-test-1", "account_id": 1, "bucket": "value_rebound", "report": sample_report()})

    summary = get_latest_review_history_summary(db, account_id="1", bucket="value_rebound")

    assert summary["latest_review_run_id"] == "review-test-1"
    assert summary["bucket"] == "value_rebound"
    assert summary["risk_approved"] == 2
    assert summary["execution_preview_ready"] == 2
