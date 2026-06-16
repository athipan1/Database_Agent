from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from uuid import uuid4


DATA_DIR = Path("data")
SIGNAL_HISTORY_PATH = DATA_DIR / "signal_history.jsonl"
PERFORMANCE_METRICS_PATH = DATA_DIR / "performance_metrics.jsonl"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> Dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
    return payload


def _read_jsonl(path: Path, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    rows = list(reversed(rows))
    return rows[offset: offset + limit]


def save_signal_history(payload: Dict[str, Any]) -> Dict[str, Any]:
    record = dict(payload)
    record.setdefault("signal_id", str(uuid4()))
    record.setdefault("timestamp", _now_iso())
    record.setdefault("source_agent", "manager-agent")
    return _append_jsonl(SIGNAL_HISTORY_PATH, record)


def list_signal_history(limit: int = 100, offset: int = 0, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    rows = _read_jsonl(SIGNAL_HISTORY_PATH, limit=10_000, offset=0)
    if symbol:
        rows = [row for row in rows if str(row.get("symbol", "")).upper() == symbol.upper()]
    return rows[offset: offset + limit]


def save_performance_metric(payload: Dict[str, Any]) -> Dict[str, Any]:
    record = dict(payload)
    record.setdefault("metric_id", str(uuid4()))
    record.setdefault("timestamp", _now_iso())
    record.setdefault("source_agent", "learning-agent")
    return _append_jsonl(PERFORMANCE_METRICS_PATH, record)


def list_performance_metrics(limit: int = 100, offset: int = 0, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    rows = _read_jsonl(PERFORMANCE_METRICS_PATH, limit=10_000, offset=0)
    if symbol:
        rows = [row for row in rows if str(row.get("symbol", "")).upper() == symbol.upper()]
    return rows[offset: offset + limit]
