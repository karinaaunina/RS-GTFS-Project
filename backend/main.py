import os
import json
import io
import zipfile
import threading
import time
import math
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, Tuple, Set, List

import numpy as np
import pandas as pd
import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse, Response

from google.transit import gtfs_realtime_pb2 as gtfs
from history_store import analytics_overview, cleanup_old_history, db_status, fetch_history, fetch_stop_history, fetch_trace_history, fetch_vehicle_history, init_database, store_snapshot

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env", override=False)
# -----------------------------
# Config
# -----------------------------

STATIC_GTFS_URL = os.getenv("STATIC_GTFS_URL")
STATIC_GTFS_VERIFY_TLS = os.getenv("STATIC_GTFS_VERIFY_TLS", "true").lower() != "false"

STATIC_GTFS_AUTO_UPDATE = os.getenv("STATIC_GTFS_AUTO_UPDATE", "false").lower() == "true"
STATIC_GTFS_UPDATE_TIME = os.getenv("STATIC_GTFS_UPDATE_TIME", "03:10")  # HH:MM local

GTFS_RT_URL = os.getenv("GTFS_RT_URL", "https://mobis-web-01.rigassatiksme.lv/gtfs_realtime.pb")

GTFS_RT_SOURCES_RAW = os.getenv("GTFS_RT_SOURCES")
DEFAULT_SOURCES = {
    "mobis": GTFS_RT_URL,
    "saraksti": "https://saraksti.lv/gtfsdata.ashx?gtfsrealtime.4tD6jVzykWbSvp3",
}

try:
    SOURCES = json.loads(GTFS_RT_SOURCES_RAW) if GTFS_RT_SOURCES_RAW else DEFAULT_SOURCES
    if not isinstance(SOURCES, dict) or not SOURCES:
        SOURCES = DEFAULT_SOURCES
except Exception:
    SOURCES = DEFAULT_SOURCES

TZ_NAME = os.getenv("TZ_NAME", "Europe/Riga")
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "5"))

# Static GTFS (optional): path to a GTFS ZIP file or a folder that contains stops.txt, trips.txt, routes.txt, stop_times.txt
STATIC_GTFS_PATH = os.getenv("STATIC_GTFS_PATH")
if STATIC_GTFS_PATH:
    p = Path(STATIC_GTFS_PATH).expanduser()
    if not p.is_absolute():
        p = (BASE_DIR / p).resolve()
    STATIC_GTFS_PATH = str(p)

# Anomaly detection controls (tuned to catch "countdown disappeared" cases without too much noise)
ANOMALY_WINDOW_PAST_SEC = int(os.getenv("ANOMALY_WINDOW_PAST_SEC", "120"))
ANOMALY_WINDOW_FUTURE_SEC = int(os.getenv("ANOMALY_WINDOW_FUTURE_SEC", "7200"))
ANOMALY_NEXT_ARRIVAL_MAX_SEC = int(os.getenv("ANOMALY_NEXT_ARRIVAL_MAX_SEC", "1800"))  # focus on "next <= 30min"
ANOMALY_JUMP_THRESHOLD_SEC = int(os.getenv("ANOMALY_JUMP_THRESHOLD_SEC", "600"))        # 10 minutes
ANOMALIES_MAX_KEEP = int(os.getenv("ANOMALIES_MAX_KEEP", "5000"))
ANOMALIES_MAX_PER_REFRESH = int(os.getenv("ANOMALIES_MAX_PER_REFRESH", "300"))

# Debounce / confidence (reduce false positives)
ANOMALY_MISSING_CONSECUTIVE = int(os.getenv("ANOMALY_MISSING_CONSECUTIVE", "2"))
ANOMALY_MIN_FUTURE_SEC = int(os.getenv("ANOMALY_MIN_FUTURE_SEC", "30"))
ANOMALY_NEAR_STOP_METERS = int(os.getenv("ANOMALY_NEAR_STOP_METERS", "1200"))

# Background automation (optional)
AUTO_REFRESH = os.getenv("AUTO_REFRESH", "true").lower() == "true"
DB_CLEANUP_ENABLED = os.getenv("DB_CLEANUP_ENABLED", "true").lower() == "true"
DB_RETENTION_DAYS = int(os.getenv("DB_RETENTION_DAYS", "3"))
DB_CLEANUP_INTERVAL_MINUTES = int(os.getenv("DB_CLEANUP_INTERVAL_MINUTES", "1440"))

# Periodic archiving (optional)
# If enabled, the backend will periodically refresh the feeds and append snapshots to daily CSVs
# (and optionally also write a workbook snapshot).
ARCHIVE_ENABLED = os.getenv("ARCHIVE_ENABLED", "false").lower() == "true"
ARCHIVE_INTERVAL_MINUTES = int(os.getenv("ARCHIVE_INTERVAL_MINUTES", "30"))
ARCHIVE_APPEND_SNAPSHOTS = os.getenv("ARCHIVE_APPEND_SNAPSHOTS", "true").lower() == "true"
ARCHIVE_WRITE_WORKBOOK = os.getenv("ARCHIVE_WRITE_WORKBOOK", "true").lower() == "true"
ARCHIVE_FORCE_REFRESH = os.getenv("ARCHIVE_FORCE_REFRESH", "true").lower() == "true"

# Snapshot capture (optional)
SNAPSHOT_ALWAYS = os.getenv("SNAPSHOT_ALWAYS", "false").lower() == "true"
SNAPSHOT_ON_ANOMALY = os.getenv("SNAPSHOT_ON_ANOMALY", "false").lower() == "true"

AUTH_USER = os.getenv("GTFS_RT_USER")
AUTH_PASS = os.getenv("GTFS_RT_PASS")
VERIFY_TLS_DEFAULT = os.getenv("VERIFY_TLS", "true").lower() != "false"
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "15"))

DATA_DIR = Path(os.getenv("DATA_DIR", str(Path(__file__).parent / "data")))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Data quality monitor (missing / empty columns)
# -----------------------------
MONITOR_DIR = DATA_DIR / "monitor"
MONITOR_DIR.mkdir(parents=True, exist_ok=True)

# If a column has >= this fraction of missing values, we flag it
MONITOR_NULL_FRACTION = float(os.getenv("MONITOR_NULL_FRACTION", "1.0"))
# Datasets that are allowed to be empty (no alerts/penalties) - e.g. GTFS-RT Alerts are often empty by design
MONITOR_ALLOW_EMPTY_DATASETS = set(
    [s.strip() for s in os.getenv("MONITOR_ALLOW_EMPTY_DATASETS", "alerts").split(",") if s.strip()]
)

# Columns to ignore in monitoring (missing/empty), per dataset.
# Format (JSON): {"trip_updates":["colA","colB"], "stop_time_updates":["arrival_delay_sec"], "alerts":["*"]}
# Use "*" to ignore ALL columns for a dataset.
MONITOR_IGNORE_COLUMNS_RAW = os.getenv("MONITOR_IGNORE_COLUMNS", "") or os.getenv("MONITOR_IGNORE_COLUMNS_JSON", "")
try:
    MONITOR_IGNORE_COLUMNS: Dict[str, List[str]] = json.loads(MONITOR_IGNORE_COLUMNS_RAW) if MONITOR_IGNORE_COLUMNS_RAW else {}
    if not isinstance(MONITOR_IGNORE_COLUMNS, dict):
        MONITOR_IGNORE_COLUMNS = {}
except Exception:
    MONITOR_IGNORE_COLUMNS = {}

# Source-aware DQ profile.
# Columns with weight=0 are informational: they are monitored and shown in analytics,
# but they do not penalize DQI because the upstream PB feed often omits them and we only
# fill them best-effort from static GTFS.
DQ_MONITOR_PROFILES: Dict[str, Dict[str, Dict[str, float | int | str]]] = {
    "trip_updates": {
        "trip_id": {"max_missing_fraction": 0.0, "weight": 18},
        "trip_update_timestamp_local": {"max_missing_fraction": 0.05, "weight": 12},
        "vehicle_id": {"max_missing_fraction": 0.02, "weight": 8},
        "route_id": {"max_missing_fraction": 0.05, "weight": 0},
        "route_short_name": {"max_missing_fraction": 0.10, "weight": 0},
    },
    "stop_time_updates": {
        "trip_id": {"max_missing_fraction": 0.0, "weight": 18},
        "stop_id": {"max_missing_fraction": 0.0, "weight": 18},
        "arrival_time_local": {"max_missing_fraction": 0.0, "weight": 14},
        "vehicle_id": {"max_missing_fraction": 0.02, "weight": 6},
        "stop_sequence": {"max_missing_fraction": 0.05, "weight": 0},
        "route_id": {"max_missing_fraction": 0.05, "weight": 0},
    },
    "vehicle_positions": {
        "vehicle_id": {"max_missing_fraction": 0.0, "weight": 18},
        "trip_id": {"max_missing_fraction": 0.0, "weight": 14},
        "latitude": {"max_missing_fraction": 0.0, "weight": 16},
        "longitude": {"max_missing_fraction": 0.0, "weight": 16},
        "timestamp_local": {"max_missing_fraction": 0.0, "weight": 14},
        "stop_id": {"max_missing_fraction": 0.05, "weight": 6},
        "route_id": {"max_missing_fraction": 0.05, "weight": 0},
        "route_short_name": {"max_missing_fraction": 0.10, "weight": 0},
    },
    "alerts": {},
}

def _monitor_ignored_columns(dataset: str) -> Set[str]:
    cols = MONITOR_IGNORE_COLUMNS.get(str(dataset), []) if isinstance(MONITOR_IGNORE_COLUMNS, dict) else []
    if cols is None:
        return set()
    try:
        cols = list(cols)
    except Exception:
        cols = []
    if "*" in cols:
        return {"*"}
    return set(str(c) for c in cols if c is not None)

def _monitor_baseline_path(source: str, dataset: str) -> Path:
    return MONITOR_DIR / f"baseline_{source}_{dataset}.json"

def _monitor_events_path(source: str) -> Path:
    return MONITOR_DIR / f"events_{source}.jsonl"


def _monitor_refresh_path(source: str) -> Path:
    return MONITOR_DIR / f"refresh_{source}.jsonl"

def _normalize_missing_mask(s: pd.Series) -> pd.Series:
    """
    Treat None/NaN and empty strings as missing.
    """
    return s.isna() | (s.astype(str).str.strip() == "")


def _dq_assess_df(source: str, dataset: str, df: Optional[pd.DataFrame]) -> Dict[str, Any]:
    """Assess one dataframe against a source-aware quality profile.

    We only score fields that the upstream source is expected to provide reliably.
    Fields that are usually missing in the PB feed (for example route_id in this feed)
    may still be monitored as informational enrichment checks, but do not reduce DQI.
    """
    ignored = _monitor_ignored_columns(dataset)
    ignore_all = "*" in ignored
    profile_raw = DQ_MONITOR_PROFILES.get(str(dataset), {}) if isinstance(DQ_MONITOR_PROFILES, dict) else {}
    profile: Dict[str, Dict[str, Any]] = {}
    if not ignore_all:
        for col, cfg in profile_raw.items():
            if col in ignored:
                continue
            profile[str(col)] = dict(cfg)

    if df is None or df.empty:
        allowed = str(dataset) in MONITOR_ALLOW_EMPTY_DATASETS
        return {
            "dataset_empty": True,
            "allowed_empty": bool(allowed),
            "missing_columns": [],
            "empty_columns": {},
            "degraded_columns": {},
            "column_issues": {},
            "row_count": int(0),
            "col_count": int(0),
            "current_columns": [],
            "expected_columns": sorted(list(profile.keys())),
            "ignored_columns": sorted(list(ignored)) if ignored else [],
        }

    current_cols = [str(c) for c in df.columns]
    if ignore_all:
        current_cols = []

    missing_cols: List[str] = []
    empty_cols: Dict[str, float] = {}
    degraded_cols: Dict[str, float] = {}
    column_issues: Dict[str, Dict[str, Any]] = {}

    for col, cfg in profile.items():
        threshold = float(cfg.get("max_missing_fraction", 0.0) or 0.0)
        weight = int(cfg.get("weight", 0) or 0)
        if col not in df.columns:
            missing_cols.append(col)
            column_issues[col] = {
                "kind": "missing_column",
                "weight": weight,
                "max_missing_fraction": threshold,
                "missing_fraction": 1.0,
                "score_applied": weight > 0,
            }
            continue
        try:
            frac = float(_normalize_missing_mask(df[col]).mean())
        except Exception:
            frac = 1.0
        if frac >= 1.0:
            empty_cols[col] = round(frac, 4)
        if frac > threshold:
            if frac < 1.0:
                degraded_cols[col] = round(frac, 4)
            column_issues[col] = {
                "kind": "fully_empty" if frac >= 1.0 else "missing_fraction_exceeded",
                "weight": weight,
                "max_missing_fraction": threshold,
                "missing_fraction": round(frac, 4),
                "score_applied": weight > 0,
            }

    return {
        "dataset_empty": False,
        "allowed_empty": False,
        "missing_columns": sorted(missing_cols),
        "empty_columns": empty_cols,
        "degraded_columns": degraded_cols,
        "column_issues": column_issues,
        "row_count": int(len(df)),
        "col_count": int(len(df.columns)),
        "current_columns": sorted(current_cols),
        "expected_columns": sorted(list(profile.keys())),
        "ignored_columns": sorted(list(ignored)) if ignored else [],
    }



def _dq_monitor_df(source: str, dataset: str, df: Optional[pd.DataFrame], ts_local: Optional[str]):
    assess = _dq_assess_df(source, dataset, df)

    if assess.get("dataset_empty"):
        _dq_log_event(source, dataset, ts_local, {
            "event_type": "dataset_empty",
            "dataset_empty": True,
            "allowed_empty": bool(assess.get("allowed_empty")),
        })
        return assess

    missing_cols = assess.get("missing_columns") or []
    empty_cols = assess.get("empty_columns") or {}
    degraded_cols = assess.get("degraded_columns") or {}
    column_issues = assess.get("column_issues") or {}

    if missing_cols or empty_cols or degraded_cols:
        payload: Dict[str, Any] = {
            "event_type": "column_quality",
            "missing_columns": missing_cols,
            "empty_columns": empty_cols,
            "degraded_columns": degraded_cols,
            "column_issues": column_issues,
            "row_count": int(assess.get("row_count") or 0),
            "col_count": int(assess.get("col_count") or 0),
        }
        for key in ["vehicle_id", "trip_id", "route_id", "stop_id"]:
            if df is not None and key in df.columns:
                vals = [v for v in df[key].head(5).tolist() if v is not None and not _is_blank(v)]
                if vals:
                    payload[f"sample_{key}s"] = [str(v) for v in vals[:5]]
        _dq_log_event(source, dataset, ts_local, payload)

    return assess


def _dq_log_refresh(source: str, ts_local: Optional[str], payload: Dict[str, Any]):
    evt = {
        "ts_local": ts_local,
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "kind": "refresh",
        **payload,
    }
    try:
        with _monitor_refresh_path(source).open("a", encoding="utf-8") as f:
            f.write(json.dumps(evt, ensure_ascii=False) + "\n")
    except Exception:
        pass

def _dq_log_event(source: str, dataset: str, ts_local: Optional[str], payload: Dict[str, Any]):
    evt = {
        "ts_local": ts_local,
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "dataset": dataset,
        **payload,
    }
    try:
        with _monitor_events_path(source).open("a", encoding="utf-8") as f:
            f.write(json.dumps(evt, ensure_ascii=False) + "\n")
    except Exception:
        # last resort: don't break data refresh
        pass


app = FastAPI(title="GTFS-RT Decoder API", version="1.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/monitor/events")
def api_monitor_events(source: str = "mobis", limit: int = 200):
    """
    Return last N data-quality events (missing / empty columns).
    """
    p = _monitor_events_path(source)
    if not p.exists():
        return []
    try:
        lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
        lines = lines[-max(1, min(limit, 5000)) :]
        out = []
        for ln in lines:
            try:
                out.append(json.loads(ln))
            except Exception:
                continue
        return out
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read monitor events: {e}")



def _read_jsonl(path: Path, limit: int = 50000) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    out: List[Dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except Exception:
                    continue
    except Exception:
        return []
    # keep only last N
    if limit and len(out) > limit:
        out = out[-limit:]
    return out

def _day_key(ts_local: Optional[str]) -> str:
    if not ts_local:
        return ""
    # expects ISO string
    return ts_local[:10]

def _hour_key(ts_local: Optional[str]) -> int:
    if not ts_local or len(ts_local) < 13:
        return -1
    try:
        return int(ts_local[11:13])
    except Exception:
        return -1

@app.get("/api/monitor/stats")
def monitor_stats(source: str = "mobis", days: int = 30):
    """Aggregated DQI + incidents for the last N days."""
    days = max(1, min(365, int(days)))
    refresh_rows = _read_jsonl(_monitor_refresh_path(source), limit=200000)

    # restrict to last N days based on ts_local
    cutoff = datetime.now(timezone.utc) - timedelta(days=days + 2)
    def _keep(row):
        ts = row.get("ts_utc")
        if not ts:
            return True
        try:
            d = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return d >= cutoff
        except Exception:
            return True

    refresh_rows = [r for r in refresh_rows if _keep(r)]

    by_day: Dict[str, Dict[str, Any]] = {}
    for r in refresh_rows:
        day = _day_key(r.get("ts_local"))
        if not day:
            continue
        b = by_day.setdefault(day, {"day": day, "dqi_avg": 0.0, "refresh_count": 0, "penalty_sum": 0, "dqi_sum": 0})
        b["refresh_count"] += 1
        b["penalty_sum"] += float(r.get("penalty", 0) or 0)
        b["dqi_sum"] += float(r.get("dqi", 100) or 100)

    series = []
    for day in sorted(by_day.keys()):
        b = by_day[day]
        cnt = max(1, int(b["refresh_count"]))
        series.append({
            "day": day,
            "refresh_count": int(b["refresh_count"]),
            "dqi_avg": round(float(b["dqi_sum"]) / cnt, 2),
            "penalty_avg": round(float(b["penalty_sum"]) / cnt, 2),
        })

    # Incidents from events log
    events = _read_jsonl(_monitor_events_path(source), limit=200000)
    events = [e for e in events if _keep(e)]

    incidents_by_day: Dict[str, int] = {}
    by_dataset: Dict[str, int] = {}
    by_hour: Dict[int, int] = {}
    top_missing: Dict[str, int] = {}
    top_empty: Dict[str, int] = {}

    for e in events:
        day = _day_key(e.get("ts_local"))
        if day:
            incidents_by_day[day] = incidents_by_day.get(day, 0) + 1
        ds = str(e.get("dataset") or "")
        if ds:
            by_dataset[ds] = by_dataset.get(ds, 0) + 1
        hr = _hour_key(e.get("ts_local"))
        if hr >= 0:
            by_hour[hr] = by_hour.get(hr, 0) + 1
        for c in (e.get("missing_columns") or []):
            top_missing[str(c)] = top_missing.get(str(c), 0) + 1
        for c in (e.get("empty_columns") or {}).keys():
            top_empty[str(c)] = top_empty.get(str(c), 0) + 1

    return {
        "source": source,
        "days": days,
        "dqi_series": series,
        "incidents_by_day": [{"day": d, "count": incidents_by_day[d]} for d in sorted(incidents_by_day.keys())],
        "incidents_by_dataset": [{"dataset": k, "count": by_dataset[k]} for k in sorted(by_dataset.keys())],
        "incidents_by_hour": [{"hour": h, "count": by_hour.get(h, 0)} for h in range(24)],
        "top_missing_columns": [{"column": k, "count": top_missing[k]} for k in sorted(top_missing, key=lambda x: top_missing[x], reverse=True)[:20]],
        "top_empty_columns": [{"column": k, "count": top_empty[k]} for k in sorted(top_empty, key=lambda x: top_empty[x], reverse=True)[:20]],
    }

@app.get("/api/monitor/daily")
def monitor_daily(source: str = "mobis", date: Optional[str] = None):
    """Daily summary for a given YYYY-MM-DD (default: today in local TZ)."""
    tz = ZoneInfo(TZ_NAME)
    if not date:
        date = datetime.now(tz).date().isoformat()
    # read refresh rows + events rows and filter by day key
    refresh_rows = _read_jsonl(_monitor_refresh_path(source), limit=200000)
    events = _read_jsonl(_monitor_events_path(source), limit=200000)

    refresh_today = [r for r in refresh_rows if _day_key(r.get("ts_local")) == date]
    events_today = [e for e in events if _day_key(e.get("ts_local")) == date]

    # compute DQI
    if refresh_today:
        dqi_avg = round(sum(float(r.get("dqi", 100) or 100) for r in refresh_today) / len(refresh_today), 2)
    else:
        dqi_avg = None

    # group events
    by_dataset: Dict[str, int] = {}
    cols_missing: Dict[str, int] = {}
    cols_empty: Dict[str, int] = {}
    for e in events_today:
        ds = str(e.get("dataset") or "")
        if ds:
            by_dataset[ds] = by_dataset.get(ds, 0) + 1
        for c in (e.get("missing_columns") or []):
            cols_missing[str(c)] = cols_missing.get(str(c), 0) + 1
        for c in (e.get("empty_columns") or {}).keys():
            cols_empty[str(c)] = cols_empty.get(str(c), 0) + 1

    return {
        "source": source,
        "date": date,
        "refresh_count": len(refresh_today),
        "dqi_avg": dqi_avg,
        "incident_count": len(events_today),
        "incidents_by_dataset": [{"dataset": k, "count": by_dataset[k]} for k in sorted(by_dataset.keys())],
        "top_missing_columns": [{"column": k, "count": cols_missing[k]} for k in sorted(cols_missing, key=lambda x: cols_missing[x], reverse=True)[:20]],
        "top_empty_columns": [{"column": k, "count": cols_empty[k]} for k in sorted(cols_empty, key=lambda x: cols_empty[x], reverse=True)[:20]],
        "sample_events": events_today[-20:],
    }

_states: Dict[str, Dict[str, Any]] = {}
_background_threads_started = False

# Loaded once at startup (if STATIC_GTFS_PATH is set)
_static_gtfs: Dict[str, Any] = {
    "loaded": False,
    "path": None,
    "stops": pd.DataFrame(),
    "routes": pd.DataFrame(),
    "trips": pd.DataFrame(),
    "stop_times": pd.DataFrame(),
    "shapes": pd.DataFrame(),
    # Derived helper (trip_id+stop_id -> stop_sequence and scheduled times)
    "stop_times_key": None,
    # Optional fast lookup indexes (populated when static feed loads)
    "stops_idx": None,
    "routes_idx": None,
    "trips_idx": None,
}

# Expected columns: helps create non-erroring empty CSVs/Excel sheets
CSV_EXPECTED_COLUMNS: Dict[str, List[str]] = {
    "gtfs_trip_updates.csv": [
        "entity_id",
        "trip_id",
        "route_id",
        "route_short_name",
        "route_long_name",
        "start_date",
        "start_time",
        "direction_id",
        "trip_headsign",
        "shape_id",
        "schedule_relationship",
        "vehicle_id",
        "vehicle_label",
        "trip_update_timestamp_local",
        "feed_header_timestamp_local",
    ],
    "gtfs_stop_time_updates.csv": [
        "entity_id",
        "trip_id",
        "vehicle_id",
        "vehicle_label",
        "route_id",
        "route_short_name",
        "route_long_name",
        "direction_id",
        "trip_headsign",
        "stop_sequence",
        "stop_id",
        "stop_name",
        "stop_lat",
        "stop_lon",
        "arrival_time_local",
        "arrival_delay_sec",
        "departure_time_local",
        "departure_delay_sec",
        "scheduled_arrival_time",
        "scheduled_departure_time",
        "schedule_relationship",
        "feed_header_timestamp_local",
    ],
    "gtfs_vehicle_positions.csv": [
        "entity_id",
        "vehicle_id",
        "vehicle_label",
        "trip_id",
        "route_id",
        "route_short_name",
        "route_long_name",
        "direction_id",
        "trip_headsign",
        "latitude",
        "longitude",
        "bearing",
        "speed_mps",
        "current_status",
        "stop_id",
        "stop_name",
        "timestamp_local",
        "feed_header_timestamp_local",
    ],
    "gtfs_alerts.csv": [
        "entity_id",
        "header",
        "description",
        "cause",
        "effect",
        "start_local",
        "end_local",
        "feed_header_timestamp_local",
    ],
    "anomalies.csv": [
        "detection_time_local",
        "source",
        "kind",
        "confidence",
        "consecutive_misses",
        "trip_id",
        "route_id",
        "route_short_name",
        "stop_id",
        "stop_name",
        "stop_sequence",
        "stop_lat",
        "stop_lon",
        "vehicle_id",
        "vehicle_lat",
        "vehicle_lon",
        "distance_to_stop_m",
        "vehicle_timestamp_local",
        "prev_arrival_time_local",
        "prev_departure_time_local",
        "curr_arrival_time_local",
        "curr_departure_time_local",
        "prev_header_ts_local",
        "curr_header_ts_local",
        "note",
    ],
}


# -----------------------------
# Helpers
# -----------------------------
def json_safe(v: Any) -> Any:
    """Convert numpy / pandas scalars to JSON-safe Python values.

    Starlette's JSONResponse uses json.dumps(..., allow_nan=False) which raises
    ValueError for NaN/Inf. We normalize those to None.
    """
    if v is None:
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating, float)):
        fv = float(v)
        return fv if math.isfinite(fv) else None
    if isinstance(v, (np.bool_, bool)):
        return bool(v)
    # pandas Timestamp
    if isinstance(v, pd.Timestamp):
        return v.isoformat()
    return v


def sanitize_obj(obj: Any) -> Any:
    """Recursively sanitize a structure (dict/list/scalars) to be JSON-safe."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {k: sanitize_obj(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [sanitize_obj(v) for v in obj]
    return json_safe(obj)


def to_iso(ts: Optional[int], tz: ZoneInfo) -> Optional[str]:
    if not ts:
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone(tz).isoformat()


def parse_iso(s: Optional[str]) -> Optional[datetime]:
    """Best-effort ISO8601 parser (returns timezone-aware dt if the string contains tz info)."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s))
    except Exception:
        return None


def _read_gtfs_table_from_folder(folder: Path, filename: str, usecols: Optional[List[str]] = None) -> pd.DataFrame:
    p = folder / filename
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p, dtype=str, keep_default_na=False, encoding="utf-8-sig", usecols=usecols)


def _read_gtfs_table_from_zip(z: zipfile.ZipFile, filename: str, usecols: Optional[List[str]] = None) -> pd.DataFrame:
    try:
        with z.open(filename, "r") as f:
            return pd.read_csv(f, dtype=str, keep_default_na=False, encoding="utf-8-sig", usecols=usecols)
    except KeyError:
        return pd.DataFrame()


def load_static_gtfs(path: str) -> Dict[str, pd.DataFrame]:
    """Load a (static) GTFS feed from a folder or a .zip.

    Expected (optional but recommended) files:
      - stops.txt
      - routes.txt
      - trips.txt
      - stop_times.txt
    """
    p = Path(path)
    use_stops = ["stop_id", "stop_name", "stop_lat", "stop_lon"]
    use_routes = ["route_id", "route_short_name", "route_long_name", "route_type"]
    use_trips = ["route_id", "service_id", "trip_id", "trip_headsign", "direction_id", "shape_id"]
    use_stop_times = ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"]
    use_shapes = ["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"]

    if p.is_dir():
        stops = _read_gtfs_table_from_folder(p, "stops.txt", usecols=use_stops)
        routes = _read_gtfs_table_from_folder(p, "routes.txt", usecols=use_routes)
        trips = _read_gtfs_table_from_folder(p, "trips.txt", usecols=use_trips)
        stop_times = _read_gtfs_table_from_folder(p, "stop_times.txt", usecols=use_stop_times)
        shapes = _read_gtfs_table_from_folder(p, "shapes.txt", usecols=use_shapes)
    else:
        with zipfile.ZipFile(p, "r") as z:
            stops = _read_gtfs_table_from_zip(z, "stops.txt", usecols=use_stops)
            routes = _read_gtfs_table_from_zip(z, "routes.txt", usecols=use_routes)
            trips = _read_gtfs_table_from_zip(z, "trips.txt", usecols=use_trips)
            stop_times = _read_gtfs_table_from_zip(z, "stop_times.txt", usecols=use_stop_times)
            shapes = _read_gtfs_table_from_zip(z, "shapes.txt", usecols=use_shapes)

    # Normalize numeric-ish columns
    for col in ["stop_lat", "stop_lon"]:
        if col in stops.columns:
            stops[col] = pd.to_numeric(stops[col], errors="coerce")
    if "route_type" in routes.columns:
        routes["route_type"] = pd.to_numeric(routes["route_type"], errors="coerce")
    if "direction_id" in trips.columns:
        trips["direction_id"] = pd.to_numeric(trips["direction_id"], errors="coerce")
    if "stop_sequence" in stop_times.columns:
        stop_times["stop_sequence"] = pd.to_numeric(stop_times["stop_sequence"], errors="coerce")

    for col in ["shape_pt_lat", "shape_pt_lon"]:
        if col in shapes.columns:
            shapes[col] = pd.to_numeric(shapes[col], errors="coerce")
    if "shape_pt_sequence" in shapes.columns:
        shapes["shape_pt_sequence"] = pd.to_numeric(shapes["shape_pt_sequence"], errors="coerce")

    return {
        "stops": stops,
        "routes": routes,
        "trips": trips,
        "stop_times": stop_times,
        "shapes": shapes,
    }

def _rebuild_static_indexes():
    # Build small indexes for faster lookups in API (map popups, anomaly enrichment, etc.)
    if isinstance(_static_gtfs.get("stops"), pd.DataFrame) and not _static_gtfs["stops"].empty and "stop_id" in _static_gtfs["stops"].columns:
        _static_gtfs["stops_idx"] = _static_gtfs["stops"].set_index("stop_id", drop=False)
    else:
        _static_gtfs["stops_idx"] = None

    if isinstance(_static_gtfs.get("routes"), pd.DataFrame) and not _static_gtfs["routes"].empty and "route_id" in _static_gtfs["routes"].columns:
        _static_gtfs["routes_idx"] = _static_gtfs["routes"].set_index("route_id", drop=False)
    else:
        _static_gtfs["routes_idx"] = None

    if isinstance(_static_gtfs.get("trips"), pd.DataFrame) and not _static_gtfs["trips"].empty and "trip_id" in _static_gtfs["trips"].columns:
        _static_gtfs["trips_idx"] = _static_gtfs["trips"].set_index("trip_id", drop=False)
    else:
        _static_gtfs["trips_idx"] = None

    # Derived lookup: (trip_id, stop_id) -> stop_sequence + scheduled times
    try:
        stt = _static_gtfs.get("stop_times")
        if isinstance(stt, pd.DataFrame) and not stt.empty and {"trip_id", "stop_id", "stop_sequence"}.issubset(stt.columns):
            kdf = stt[[c for c in ["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time"] if c in stt.columns]].copy()
            kdf["trip_id"] = kdf["trip_id"].astype(str)
            kdf["stop_id"] = kdf["stop_id"].astype(str)
            kdf["stop_sequence"] = pd.to_numeric(kdf["stop_sequence"], errors="coerce")
            kdf = kdf[pd.notnull(kdf["stop_sequence"])].copy()
            kdf["stop_sequence"] = kdf["stop_sequence"].astype(int)
            kdf = kdf.sort_values(["trip_id", "stop_id", "stop_sequence"], kind="mergesort")
            kdf = kdf.drop_duplicates(subset=["trip_id", "stop_id"], keep="first")
            _static_gtfs["stop_times_key"] = kdf
        else:
            _static_gtfs["stop_times_key"] = None
    except Exception:
        _static_gtfs["stop_times_key"] = None


def reload_static_gtfs_from_path(path: str) -> Dict[str, Any]:
    """Reload static GTFS into memory (safe to call at runtime)."""
    try:
        gtfs_static = load_static_gtfs(path)
        _static_gtfs.update(gtfs_static)
        _static_gtfs["loaded"] = True
        _static_gtfs["path"] = path
        _rebuild_static_indexes()
        return {"ok": True, "path": path, "counts": {
            "stops": int(len(_static_gtfs.get("stops", pd.DataFrame()))),
            "routes": int(len(_static_gtfs.get("routes", pd.DataFrame()))),
            "trips": int(len(_static_gtfs.get("trips", pd.DataFrame()))),
            "stop_times": int(len(_static_gtfs.get("stop_times", pd.DataFrame()))),
            "shapes": int(len(_static_gtfs.get("shapes", pd.DataFrame()))),
        }}
    except Exception as e:
        _static_gtfs["loaded"] = False
        _static_gtfs["path"] = path
        return {"ok": False, "path": path, "error": str(e)}


def _validate_static_gtfs_zip_bytes(b: bytes) -> None:
    """Fail fast if zip is invalid or missing essential files."""
    with zipfile.ZipFile(io.BytesIO(b), "r") as z:
        names = set(z.namelist())
        required = {"stops.txt", "routes.txt", "trips.txt", "stop_times.txt"}
        missing = [x for x in required if x not in names]
        if missing:
            raise ValueError(f"Static GTFS zip missing: {missing}")


def download_static_gtfs_zip(url: str, out_path: str) -> Dict[str, Any]:
    """Download static GTFS zip, validate, write atomically, reload in-memory."""
    if not url:
        return {"ok": False, "error": "STATIC_GTFS_URL is not set"}
    tz = ZoneInfo(TZ_NAME)
    auth = (AUTH_USER, AUTH_PASS) if AUTH_USER and AUTH_PASS else None

    # TLS verify: atsevišķi statiskajam GTFS (saraksti.lv bieži ir self-signed ķēde)
    verify = STATIC_GTFS_VERIFY_TLS
    if "saraksti.lv" in str(url):
        verify = False

    r = requests.get(url, timeout=REQUEST_TIMEOUT, auth=auth, verify=verify)
    r.raise_for_status()
    b = r.content

    _validate_static_gtfs_zip_bytes(b)

    out_p = Path(out_path)
    if not out_p.is_absolute():
        out_p = (BASE_DIR / out_p).resolve()
    out_p.parent.mkdir(parents=True, exist_ok=True)

    tmp = out_p.with_suffix(out_p.suffix + ".tmp")
    tmp.write_bytes(b)
    tmp.replace(out_p)

    # reload into memory immediately
    res = reload_static_gtfs_from_path(str(out_p))
    res["downloaded_at_local"] = datetime.now(tz).isoformat()
    res["url"] = url
    return res



def _next_run_dt_local(hhmm: str, tz: ZoneInfo) -> datetime:
    """Compute next run datetime in local tz based on HH:MM."""
    now = datetime.now(tz)
    try:
        hh, mm = hhmm.split(":")
        hh = int(hh); mm = int(mm)
    except Exception:
        hh, mm = 3, 10  # fallback

    candidate = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if candidate <= now:
        candidate = candidate + timedelta(days=1)
    return candidate


def _static_gtfs_daily_worker():
    tz = ZoneInfo(TZ_NAME)
    while True:
        try:
            run_at = _next_run_dt_local(STATIC_GTFS_UPDATE_TIME, tz)
            sleep_s = max(1, int((run_at - datetime.now(tz)).total_seconds()))
            time.sleep(sleep_s)

            # Do the update
            if STATIC_GTFS_URL and STATIC_GTFS_PATH:
                res = download_static_gtfs_zip(STATIC_GTFS_URL, STATIC_GTFS_PATH)
                print(f"[static-gtfs] daily update: {res}")
            else:
                print("[static-gtfs] daily update skipped (STATIC_GTFS_URL or STATIC_GTFS_PATH missing)")
        except Exception as e:
            print(f"[static-gtfs] daily update error: {e}")
            time.sleep(60)


def static_gtfs_loaded() -> bool:
    return bool(_static_gtfs.get("loaded"))


def _static_lookup_stop_name(stop_id: Optional[str]) -> Optional[str]:
    if not static_gtfs_loaded() or not stop_id:
        return None
    stops = _static_gtfs.get("stops")
    if stops is None or stops.empty or "stop_id" not in stops.columns:
        return None
    m = stops.loc[stops["stop_id"] == str(stop_id)]
    if m.empty:
        return None
    return m.iloc[0].get("stop_name")


def _idx_first_row(v: Any) -> Optional[pd.Series]:
    """Helper: .loc can return a Series (unique) or DataFrame (duplicates)."""
    if v is None:
        return None
    if isinstance(v, pd.Series):
        return v
    if isinstance(v, pd.DataFrame):
        return v.iloc[0] if not v.empty else None
    return None


def _static_lookup_stop(stop_id: Optional[str]) -> Dict[str, Any]:
    """Return stop_name/lat/lon for a stop_id (best-effort)."""
    if not static_gtfs_loaded() or not stop_id:
        return {"stop_name": None, "stop_lat": None, "stop_lon": None}

    sid = str(stop_id)
    idx = _static_gtfs.get("stops_idx")
    if isinstance(idx, pd.DataFrame):
        try:
            row = _idx_first_row(idx.loc[sid])
            if row is None:
                return {"stop_name": None, "stop_lat": None, "stop_lon": None}
            return {
                "stop_name": row.get("stop_name"),
                "stop_lat": json_safe(row.get("stop_lat")),
                "stop_lon": json_safe(row.get("stop_lon")),
            }
        except KeyError:
            return {"stop_name": None, "stop_lat": None, "stop_lon": None}

    stops = _static_gtfs.get("stops")
    if stops is None or stops.empty or "stop_id" not in stops.columns:
        return {"stop_name": None, "stop_lat": None, "stop_lon": None}
    m = stops.loc[stops["stop_id"] == sid]
    if m.empty:
        return {"stop_name": None, "stop_lat": None, "stop_lon": None}
    r = m.iloc[0]
    return {
        "stop_name": r.get("stop_name"),
        "stop_lat": json_safe(r.get("stop_lat")),
        "stop_lon": json_safe(r.get("stop_lon")),
    }


def _static_lookup_route(route_id: Optional[str]) -> Dict[str, Any]:
    if not static_gtfs_loaded() or not route_id:
        return {"route_short_name": None, "route_long_name": None}
    rid = str(route_id)
    idx = _static_gtfs.get("routes_idx")
    if isinstance(idx, pd.DataFrame):
        try:
            row = _idx_first_row(idx.loc[rid])
            if row is None:
                return {"route_short_name": None, "route_long_name": None}
            return {
                "route_short_name": row.get("route_short_name"),
                "route_long_name": row.get("route_long_name"),
            }
        except KeyError:
            return {"route_short_name": None, "route_long_name": None}
    routes = _static_gtfs.get("routes")
    if routes is None or routes.empty or "route_id" not in routes.columns:
        return {"route_short_name": None, "route_long_name": None}
    m = routes.loc[routes["route_id"] == rid]
    if m.empty:
        return {"route_short_name": None, "route_long_name": None}
    r = m.iloc[0]
    return {"route_short_name": r.get("route_short_name"), "route_long_name": r.get("route_long_name")}


def _static_lookup_route_short_name(route_id: Optional[str]) -> Optional[str]:
    """Convenience wrapper used in multiple endpoints."""
    try:
        return _static_lookup_route(route_id).get("route_short_name")
    except Exception:
        return None


def _static_lookup_route_long_name(route_id: Optional[str]) -> Optional[str]:
    """Convenience wrapper used in multiple endpoints."""
    try:
        return _static_lookup_route(route_id).get("route_long_name")
    except Exception:
        return None


def _static_lookup_trip(trip_id: Optional[str]) -> Dict[str, Any]:
    if not static_gtfs_loaded() or not trip_id:
        return {"trip_id": None, "route_id": None, "direction_id": None, "trip_headsign": None, "shape_id": None}
    tid = str(trip_id)
    idx = _static_gtfs.get("trips_idx")
    if isinstance(idx, pd.DataFrame):
        try:
            row = _idx_first_row(idx.loc[tid])
            if row is None:
                return {"trip_id": tid, "route_id": None, "direction_id": None, "trip_headsign": None, "shape_id": None}
            return {
                "trip_id": tid,
                "route_id": row.get("route_id"),
                "direction_id": json_safe(row.get("direction_id")),
                "trip_headsign": row.get("trip_headsign"),
                "shape_id": row.get("shape_id"),
            }
        except KeyError:
            return {"trip_id": tid, "route_id": None, "direction_id": None, "trip_headsign": None, "shape_id": None}
    trips = _static_gtfs.get("trips")
    if trips is None or trips.empty or "trip_id" not in trips.columns:
        return {"trip_id": tid, "route_id": None, "direction_id": None, "trip_headsign": None, "shape_id": None}
    m = trips.loc[trips["trip_id"] == tid]
    if m.empty:
        return {"trip_id": tid, "route_id": None, "direction_id": None, "trip_headsign": None, "shape_id": None}
    r = m.iloc[0]
    return {
        "trip_id": tid,
        "route_id": r.get("route_id"),
        "direction_id": json_safe(r.get("direction_id")),
        "trip_headsign": r.get("trip_headsign"),
        "shape_id": r.get("shape_id"),
    }


def _is_blank(v: Any) -> bool:
    if v is None:
        return True
    try:
        if isinstance(v, float) and np.isnan(v):
            return True
    except Exception:
        pass
    s = str(v).strip()
    return s == "" or s.lower() == "nan" or s.lower() == "none"


def _normalize_id_value(v: Any) -> Optional[str]:
    if _is_blank(v):
        return None
    return str(v).strip()



def _normalize_id_series(s: pd.Series) -> pd.Series:
    return s.map(_normalize_id_value)



def _missing_series_mask(s: pd.Series) -> pd.Series:
    return s.isna() | s.map(_is_blank)



def _coalesce_from_static(df: pd.DataFrame, target_col: str, static_col: str) -> pd.DataFrame:
    if static_col not in df.columns:
        return df
    if target_col not in df.columns:
        df[target_col] = df[static_col]
    else:
        mask = _missing_series_mask(df[target_col])
        df.loc[mask, target_col] = df.loc[mask, static_col]
    return df.drop(columns=[static_col], errors="ignore")



def _normalize_object_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_object_dtype(out[col]) or isinstance(out[col].dtype, pd.StringDtype):
            out[col] = out[col].map(lambda v: None if _is_blank(v) else str(v).strip())
    return out



def _finalize_dataset(df: pd.DataFrame, expected_cols: List[str]) -> pd.DataFrame:
    out = _normalize_object_columns(df.copy())
    for col in expected_cols:
        if col not in out.columns:
            out[col] = None
    out = out[expected_cols].copy()
    return out.replace([np.inf, -np.inf], np.nan)



def enrich_stop_time_updates(df: pd.DataFrame) -> pd.DataFrame:
    """Enrich StopTimeUpdates with static GTFS fields and keep a clean schema."""
    if df is None:
        return df
    out = df.copy()
    if out.empty:
        return _finalize_dataset(out, CSV_EXPECTED_COLUMNS["gtfs_stop_time_updates.csv"])

    if "trip_id" in out.columns:
        out["trip_id"] = _normalize_id_series(out["trip_id"])
    if "stop_id" in out.columns:
        out["stop_id"] = _normalize_id_series(out["stop_id"])
    if "vehicle_id" in out.columns:
        out["vehicle_id"] = _normalize_id_series(out["vehicle_id"])

    if static_gtfs_loaded():
        stops = _static_gtfs.get("stops")
        if isinstance(stops, pd.DataFrame) and not stops.empty and "stop_id" in out.columns and "stop_id" in stops.columns:
            sdf = stops[[c for c in ["stop_id", "stop_name", "stop_lat", "stop_lon"] if c in stops.columns]].copy()
            sdf["stop_id"] = _normalize_id_series(sdf["stop_id"])
            out = out.merge(sdf, on="stop_id", how="left")

        trips = _static_gtfs.get("trips")
        if isinstance(trips, pd.DataFrame) and not trips.empty and "trip_id" in out.columns and "trip_id" in trips.columns:
            tcols = [c for c in ["trip_id", "route_id", "trip_headsign", "direction_id", "shape_id"] if c in trips.columns]
            tdf = trips[tcols].copy()
            tdf["trip_id"] = _normalize_id_series(tdf["trip_id"])
            rename_map = {
                "route_id": "route_id_static",
                "trip_headsign": "trip_headsign_static",
                "direction_id": "direction_id_static",
                "shape_id": "shape_id_static",
            }
            tdf = tdf.rename(columns={k: v for k, v in rename_map.items() if k in tdf.columns})
            out = out.merge(tdf, on="trip_id", how="left")
            for base_col, static_col in [
                ("route_id", "route_id_static"),
                ("trip_headsign", "trip_headsign_static"),
                ("direction_id", "direction_id_static"),
            ]:
                out = _coalesce_from_static(out, base_col, static_col)
            out = out.drop(columns=["shape_id_static"], errors="ignore")

        kdf = _static_gtfs.get("stop_times_key")
        if isinstance(kdf, pd.DataFrame) and not kdf.empty and {"trip_id", "stop_id"}.issubset(kdf.columns) and {"trip_id", "stop_id"}.issubset(out.columns):
            tmp = kdf.copy()
            tmp["trip_id"] = _normalize_id_series(tmp["trip_id"])
            tmp["stop_id"] = _normalize_id_series(tmp["stop_id"])
            tmp = tmp.rename(columns={
                "stop_sequence": "stop_sequence_static",
                "arrival_time": "scheduled_arrival_time",
                "departure_time": "scheduled_departure_time",
            })
            cols = [c for c in ["trip_id", "stop_id", "stop_sequence_static", "scheduled_arrival_time", "scheduled_departure_time"] if c in tmp.columns]
            out = out.merge(tmp[cols], on=["trip_id", "stop_id"], how="left")
            if "stop_sequence" in out.columns:
                out["stop_sequence"] = pd.to_numeric(out["stop_sequence"], errors="coerce")
                fill_mask = out["stop_sequence"].isna() | (out["stop_sequence"] <= 0)
                out.loc[fill_mask, "stop_sequence"] = out.loc[fill_mask, "stop_sequence_static"]
            else:
                out["stop_sequence"] = out.get("stop_sequence_static")
            out = out.drop(columns=["stop_sequence_static"], errors="ignore")

        routes = _static_gtfs.get("routes")
        if isinstance(routes, pd.DataFrame) and not routes.empty and "route_id" in out.columns and "route_id" in routes.columns:
            rdf = routes[[c for c in ["route_id", "route_short_name", "route_long_name"] if c in routes.columns]].copy()
            rdf["route_id"] = _normalize_id_series(rdf["route_id"])
            out["route_id"] = _normalize_id_series(out["route_id"])
            out = out.merge(rdf, on="route_id", how="left")

    expected = CSV_EXPECTED_COLUMNS["gtfs_stop_time_updates.csv"]
    return _finalize_dataset(out, expected)



def enrich_trip_updates(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return df
    out = df.copy()
    if out.empty:
        return _finalize_dataset(out, CSV_EXPECTED_COLUMNS["gtfs_trip_updates.csv"])

    if "trip_id" in out.columns:
        out["trip_id"] = _normalize_id_series(out["trip_id"])
    if "vehicle_id" in out.columns:
        out["vehicle_id"] = _normalize_id_series(out["vehicle_id"])

    if static_gtfs_loaded():
        trips = _static_gtfs.get("trips")
        if isinstance(trips, pd.DataFrame) and not trips.empty and "trip_id" in out.columns and "trip_id" in trips.columns:
            tcols = [c for c in ["trip_id", "route_id", "trip_headsign", "direction_id", "shape_id"] if c in trips.columns]
            tdf = trips[tcols].copy()
            tdf["trip_id"] = _normalize_id_series(tdf["trip_id"])
            tdf = tdf.rename(columns={
                "route_id": "route_id_static",
                "trip_headsign": "trip_headsign_static",
                "direction_id": "direction_id_static",
                "shape_id": "shape_id_static",
            })
            out = out.merge(tdf, on="trip_id", how="left")
            for base_col, static_col in [
                ("route_id", "route_id_static"),
                ("trip_headsign", "trip_headsign_static"),
                ("direction_id", "direction_id_static"),
                ("shape_id", "shape_id_static"),
            ]:
                out = _coalesce_from_static(out, base_col, static_col)

        routes = _static_gtfs.get("routes")
        if isinstance(routes, pd.DataFrame) and not routes.empty and "route_id" in out.columns and "route_id" in routes.columns:
            rdf = routes[[c for c in ["route_id", "route_short_name", "route_long_name"] if c in routes.columns]].copy()
            rdf["route_id"] = _normalize_id_series(rdf["route_id"])
            out["route_id"] = _normalize_id_series(out["route_id"])
            out = out.merge(rdf, on="route_id", how="left")

    expected = CSV_EXPECTED_COLUMNS["gtfs_trip_updates.csv"]
    return _finalize_dataset(out, expected)



def enrich_vehicle_positions(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return df
    out = df.copy()
    if out.empty:
        return _finalize_dataset(out, CSV_EXPECTED_COLUMNS["gtfs_vehicle_positions.csv"])

    if "trip_id" in out.columns:
        out["trip_id"] = _normalize_id_series(out["trip_id"])
    if "stop_id" in out.columns:
        out["stop_id"] = _normalize_id_series(out["stop_id"])
    if "vehicle_id" in out.columns:
        out["vehicle_id"] = _normalize_id_series(out["vehicle_id"])

    if static_gtfs_loaded():
        trips = _static_gtfs.get("trips")
        if isinstance(trips, pd.DataFrame) and not trips.empty and "trip_id" in out.columns and "trip_id" in trips.columns:
            tcols = [c for c in ["trip_id", "route_id", "trip_headsign", "direction_id", "shape_id"] if c in trips.columns]
            tdf = trips[tcols].copy()
            tdf["trip_id"] = _normalize_id_series(tdf["trip_id"])
            tdf = tdf.rename(columns={
                "route_id": "route_id_static",
                "trip_headsign": "trip_headsign_static",
                "direction_id": "direction_id_static",
                "shape_id": "shape_id_static",
            })
            out = out.merge(tdf, on="trip_id", how="left")
            for base_col, static_col in [
                ("route_id", "route_id_static"),
                ("trip_headsign", "trip_headsign_static"),
                ("direction_id", "direction_id_static"),
            ]:
                out = _coalesce_from_static(out, base_col, static_col)
            out = out.drop(columns=["shape_id_static"], errors="ignore")

        routes = _static_gtfs.get("routes")
        if isinstance(routes, pd.DataFrame) and not routes.empty and "route_id" in out.columns and "route_id" in routes.columns:
            rdf = routes[[c for c in ["route_id", "route_short_name", "route_long_name"] if c in routes.columns]].copy()
            rdf["route_id"] = _normalize_id_series(rdf["route_id"])
            out["route_id"] = _normalize_id_series(out["route_id"])
            out = out.merge(rdf, on="route_id", how="left")

        stops = _static_gtfs.get("stops")
        if isinstance(stops, pd.DataFrame) and not stops.empty and "stop_id" in out.columns and "stop_id" in stops.columns:
            sdf = stops[[c for c in ["stop_id", "stop_name"] if c in stops.columns]].copy()
            sdf["stop_id"] = _normalize_id_series(sdf["stop_id"])
            out = out.merge(sdf, on="stop_id", how="left")

    expected = CSV_EXPECTED_COLUMNS["gtfs_vehicle_positions.csv"]
    return _finalize_dataset(out, expected)


def decode_pb_bytes(pb_bytes: bytes, tz: ZoneInfo):
    """
    Decode GTFS-Realtime protobuf bytes into 4 analytical tables.

    Important nuance:
    - GTFS-RT is proto2, so *missing* scalar fields have a default value ("" / 0)
      BUT are still "not present". Using .HasField() is critical, otherwise we
      accidentally interpret missing values as real values (e.g., stop_sequence=0).
    """
    feed = gtfs.FeedMessage()
    feed.ParseFromString(pb_bytes)

    trips_rows, stops_rows, veh_rows, alert_rows = [], [], [], []

    header_ts_local = None
    try:
        if feed and feed.header and feed.header.HasField("timestamp"):
            header_dt = datetime.fromtimestamp(int(feed.header.timestamp), tz=timezone.utc).astimezone(tz)
            header_ts_local = header_dt.isoformat()
    except Exception:
        header_ts_local = None

    def _opt_str(msg: Any, field: str) -> Optional[str]:
        try:
            if msg is None:
                return None
            if msg.HasField(field):
                v = getattr(msg, field)
                if v is None:
                    return None
                s = str(v).strip()
                return s if s else None
        except Exception:
            return None
        return None

    def _opt_int(msg: Any, field: str) -> Optional[int]:
        try:
            if msg is None:
                return None
            if msg.HasField(field):
                return int(getattr(msg, field))
        except Exception:
            return None
        return None

    def _opt_float(msg: Any, field: str) -> Optional[float]:
        try:
            if msg is None:
                return None
            if msg.HasField(field):
                return float(getattr(msg, field))
        except Exception:
            return None
        return None

    def _ts_local(ts: Optional[int]) -> Optional[str]:
        if ts is None:
            return None
        try:
            return datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone(tz).isoformat()
        except Exception:
            return None

    for ent in feed.entity:
        entity_id = getattr(ent, "id", None)

        # -------- TripUpdates
        if ent.HasField("trip_update"):
            tu = ent.trip_update
            trip = tu.trip if tu.HasField("trip") else None
            veh = tu.vehicle if tu.HasField("vehicle") else None

            trips_rows.append({
                "entity_id": entity_id,
                "trip_id": _opt_str(trip, "trip_id"),
                "route_id": _opt_str(trip, "route_id"),
                "start_date": _opt_str(trip, "start_date"),
                "start_time": _opt_str(trip, "start_time"),
                "direction_id": _opt_int(trip, "direction_id"),
                "shape_id": _opt_str(trip, "shape_id"),
                "schedule_relationship": _opt_int(trip, "schedule_relationship"),
                "vehicle_id": _opt_str(veh, "id"),
                "vehicle_label": _opt_str(veh, "label"),
                "trip_update_timestamp_local": _ts_local(_opt_int(tu, "timestamp")),
                "feed_header_timestamp_local": header_ts_local,
            })

            # StopTimeUpdates inside TripUpdate
            for stu in getattr(tu, "stop_time_update", []):
                # arrival/departure are nested messages
                arrival_time = None
                arrival_delay = None
                if stu.HasField("arrival"):
                    arr = stu.arrival
                    arrival_time = _ts_local(_opt_int(arr, "time"))
                    arrival_delay = _opt_int(arr, "delay")

                departure_time = None
                departure_delay = None
                if stu.HasField("departure"):
                    dep = stu.departure
                    departure_time = _ts_local(_opt_int(dep, "time"))
                    departure_delay = _opt_int(dep, "delay")

                stops_rows.append({
                    "entity_id": entity_id,
                    "trip_id": _opt_str(trip, "trip_id"),
                    "vehicle_id": _opt_str(veh, "id"),
                    "vehicle_label": _opt_str(veh, "label"),
                    "route_id": _opt_str(trip, "route_id"),
                    "stop_sequence": _opt_int(stu, "stop_sequence"),
                    "stop_id": _opt_str(stu, "stop_id"),
                    "arrival_time_local": arrival_time,
                    "arrival_delay_sec": arrival_delay,
                    "departure_time_local": departure_time,
                    "departure_delay_sec": departure_delay,
                    "schedule_relationship": _opt_int(stu, "schedule_relationship"),
                    "feed_header_timestamp_local": header_ts_local,
                })

        # -------- VehiclePositions
        if ent.HasField("vehicle"):
            v = ent.vehicle
            trip = v.trip if v.HasField("trip") else None
            veh = v.vehicle if v.HasField("vehicle") else None
            pos = v.position if v.HasField("position") else None

            veh_rows.append({
                "entity_id": entity_id,
                "vehicle_id": _opt_str(veh, "id"),
                "vehicle_label": _opt_str(veh, "label"),
                "trip_id": _opt_str(trip, "trip_id"),
                "route_id": _opt_str(trip, "route_id"),
                "direction_id": _opt_int(trip, "direction_id"),
                "latitude": _opt_float(pos, "latitude"),
                "longitude": _opt_float(pos, "longitude"),
                "bearing": _opt_float(pos, "bearing"),
                "speed_mps": _opt_float(pos, "speed"),
                "current_status": _opt_int(v, "current_status"),
                "stop_id": _opt_str(v, "stop_id"),
                "timestamp_local": _ts_local(_opt_int(v, "timestamp")),
                "feed_header_timestamp_local": header_ts_local,
            })

        # -------- Alerts
        if ent.HasField("alert"):
            al = ent.alert
            header = None
            description = None
            try:
                if al.HasField("header_text") and len(al.header_text.translation):
                    header = al.header_text.translation[0].text
            except Exception:
                header = None
            try:
                if al.HasField("description_text") and len(al.description_text.translation):
                    description = al.description_text.translation[0].text
            except Exception:
                description = None

            # active_period is repeated
            start_local = None
            end_local = None
            try:
                if len(al.active_period):
                    ap = al.active_period[0]
                    start_local = _ts_local(_opt_int(ap, "start"))
                    end_local = _ts_local(_opt_int(ap, "end"))
            except Exception:
                pass

            alert_rows.append({
                "entity_id": entity_id,
                "header": header,
                "description": description,
                "cause": _opt_int(al, "cause"),
                "effect": _opt_int(al, "effect"),
                "start_local": start_local,
                "end_local": end_local,
                "feed_header_timestamp_local": header_ts_local,
            })

    trips_df = pd.DataFrame(trips_rows)
    stops_df = pd.DataFrame(stops_rows)
    veh_df = pd.DataFrame(veh_rows)
    alerts_df = pd.DataFrame(alert_rows)

    # Normalize types (best-effort)
    for df, col in [
        (stops_df, "stop_sequence"),
        (stops_df, "arrival_delay_sec"),
        (stops_df, "departure_delay_sec"),
        (stops_df, "schedule_relationship"),
        (trips_df, "direction_id"),
        (veh_df, "direction_id"),
        (veh_df, "bearing"),
        (veh_df, "speed_mps"),
        (veh_df, "current_status"),
    ]:
        if df is not None and not df.empty and col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return header_ts_local, trips_df, stops_df, veh_df, alerts_df


def write_csv(df: Optional[pd.DataFrame], filename: str, source: Optional[str] = None) -> Path:
    """Write CSV to disk.

    UX detail: ALWAYS create the file, even when there is no data.
    This avoids the UI "404 File not found" when a dataset is empty (very common for Alerts).
    """
    out_dir = (DATA_DIR / source) if source else DATA_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / filename

    expected_cols = CSV_EXPECTED_COLUMNS.get(filename, [])
    if df is None:
        df2 = pd.DataFrame(columns=expected_cols)
    else:
        df2 = df.copy()
        if df2.empty and expected_cols:
            df2 = df2.reindex(columns=expected_cols)

    df2.to_csv(out, index=False)
    return out


def _snapshot_dir(source: str, tz: ZoneInfo) -> Path:
    base = DATA_DIR / source / "snapshots"
    base.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(tz).strftime("%Y%m%d_%H%M%S")
    out = base / ts
    out.mkdir(parents=True, exist_ok=True)
    return out


def save_snapshot(
    *,
    source: str,
    pb: bytes,
    tz: ZoneInfo,
    trips_df: Optional[pd.DataFrame],
    stops_df: Optional[pd.DataFrame],
    veh_df: Optional[pd.DataFrame],
    alerts_df: Optional[pd.DataFrame],
    anomalies: List[Dict[str, Any]],
) -> Path:
    """Persist a point-in-time snapshot (optional).

    This is useful when you need concrete evidence to share with a vendor (e.g. Papercast).
    Disabled by default; enable via SNAPSHOT_ALWAYS/SNAPSHOT_ON_ANOMALY.
    """
    out = _snapshot_dir(source, tz)
    (out / "gtfs_realtime.pb").write_bytes(pb)

    # Write CSVs next to the .pb for easy sharing
    (trips_df or pd.DataFrame(columns=CSV_EXPECTED_COLUMNS["gtfs_trip_updates.csv"])).to_csv(out / "gtfs_trip_updates.csv", index=False)
    (stops_df or pd.DataFrame(columns=CSV_EXPECTED_COLUMNS["gtfs_stop_time_updates.csv"])).to_csv(out / "gtfs_stop_time_updates.csv", index=False)
    (veh_df or pd.DataFrame(columns=CSV_EXPECTED_COLUMNS["gtfs_vehicle_positions.csv"])).to_csv(out / "gtfs_vehicle_positions.csv", index=False)
    (alerts_df or pd.DataFrame(columns=CSV_EXPECTED_COLUMNS["gtfs_alerts.csv"])).to_csv(out / "gtfs_alerts.csv", index=False)

    if anomalies:
        _rows_to_anomaly_df(anomalies).to_csv(out / "anomalies.csv", index=False)
    return out


# -----------------------------
# Periodic archiving (optional)
# -----------------------------
def _archive_day_dir(source: str, tz: ZoneInfo) -> Path:
    day = datetime.now(tz).strftime("%Y-%m-%d")
    out_dir = DATA_DIR / source / "archives" / day
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def _append_snapshot_csv(*, df: Optional[pd.DataFrame], out_csv: Path, snapshot_time_local: str, source: str, expected_cols: List[str]):
    """Append a 'snapshot' chunk to a daily CSV.

    We always add snapshot_time_local + source columns, so later you can reconstruct the full day timeline.
    """
    if df is None:
        df2 = pd.DataFrame(columns=expected_cols)
    else:
        df2 = df.copy()
        if df2.empty and expected_cols:
            df2 = df2.reindex(columns=expected_cols)

    # Stable column order
    if "snapshot_time_local" not in df2.columns:
        df2.insert(0, "snapshot_time_local", snapshot_time_local)
    else:
        df2["snapshot_time_local"] = snapshot_time_local

    if "source" not in df2.columns:
        df2.insert(1, "source", source)
    else:
        df2["source"] = source

    if out_csv.exists():
        df2.to_csv(out_csv, mode="a", header=False, index=False)
    else:
        df2.to_csv(out_csv, index=False)


def archive_snapshot(source: str, *, force_refresh: bool = False) -> Dict[str, Any]:
    """Write a periodic archive snapshot.

    Depending on env flags this can:
      - append snapshot chunks to daily CSVs
      - write an .xlsx workbook snapshot
    """
    tz = ZoneInfo(TZ_NAME)
    if force_refresh:
        try:
            refresh_now(source)
        except Exception as e:
            return {"ok": False, "error": f"refresh failed: {e}"}

    st = _get_state(source)
    snapshot_time_local = st.get("header_ts_local") or datetime.now(tz).isoformat()
    out_dir = _archive_day_dir(source, tz)
    written = {}

    if ARCHIVE_APPEND_SNAPSHOTS:
        _append_snapshot_csv(
            df=st.get("trips_df"),
            out_csv=out_dir / "trip_updates_snapshots.csv",
            snapshot_time_local=snapshot_time_local,
            source=source,
            expected_cols=CSV_EXPECTED_COLUMNS["gtfs_trip_updates.csv"],
        )
        _append_snapshot_csv(
            df=st.get("stops_df"),
            out_csv=out_dir / "stop_time_updates_snapshots.csv",
            snapshot_time_local=snapshot_time_local,
            source=source,
            expected_cols=CSV_EXPECTED_COLUMNS["gtfs_stop_time_updates.csv"],
        )
        _append_snapshot_csv(
            df=st.get("veh_df"),
            out_csv=out_dir / "vehicle_positions_snapshots.csv",
            snapshot_time_local=snapshot_time_local,
            source=source,
            expected_cols=CSV_EXPECTED_COLUMNS["gtfs_vehicle_positions.csv"],
        )
        _append_snapshot_csv(
            df=st.get("alerts_df"),
            out_csv=out_dir / "alerts_snapshots.csv",
            snapshot_time_local=snapshot_time_local,
            source=source,
            expected_cols=CSV_EXPECTED_COLUMNS["gtfs_alerts.csv"],
        )
        _append_snapshot_csv(
            df=_rows_to_anomaly_df(st.get("anomalies", [])),
            out_csv=out_dir / "anomalies_snapshots.csv",
            snapshot_time_local=snapshot_time_local,
            source=source,
            expected_cols=CSV_EXPECTED_COLUMNS["anomalies.csv"],
        )
        written["daily_csv_dir"] = str(out_dir)

    if ARCHIVE_WRITE_WORKBOOK:
        try:
            wb_bytes = workbook_bytes_for_sources([source])
            ts_tag = datetime.now(tz).strftime("%Y%m%d_%H%M%S")
            out_xlsx = out_dir / f"snapshot_{source}_{ts_tag}.xlsx"
            out_xlsx.write_bytes(wb_bytes)
            written["workbook"] = str(out_xlsx)
        except Exception as e:
            written["workbook_error"] = str(e)

    st["last_archive_utc"] = datetime.now(timezone.utc).isoformat()
    return {"ok": True, "source": source, "snapshot_time_local": snapshot_time_local, "written": written}


def _archive_worker():
    tz = ZoneInfo(TZ_NAME)
    interval_s = max(60, int(ARCHIVE_INTERVAL_MINUTES) * 60)
    while True:
        try:
            for src in SOURCES.keys():
                archive_snapshot(src, force_refresh=ARCHIVE_FORCE_REFRESH)
        except Exception as e:
            print(f"[archive] error: {e}")
        time.sleep(interval_s)


def _auto_refresh_worker(source: str):
    interval_s = max(1, int(REFRESH_SECONDS))
    while True:
        started = time.monotonic()
        try:
            refresh_now(source)
        except Exception as e:
            print(f"[auto-refresh] {source} error: {e}")
        elapsed = time.monotonic() - started
        time.sleep(max(0.0, interval_s - elapsed))



def _get_state(source: str) -> Dict[str, Any]:
    if source not in SOURCES:
        raise HTTPException(status_code=400, detail=f"Unknown source: {source}. Use /api/sources")
    st = _states.get(source)
    if st is None:
        st = {
            "last_refresh_utc": None,
            "header_ts_local": None,
            "trips_df": pd.DataFrame(),
            "stops_df": pd.DataFrame(),
            "veh_df": pd.DataFrame(),
            "alerts_df": pd.DataFrame(),
            "anomalies": [],
            "db_last_store": None,
            # internal
            "debounce": {},
            "lock": threading.Lock(),
            "last_archive_utc": None,
        }
        _states[source] = st
    return st



def refresh_now(source: str = "mobis") -> Dict[str, Any]:
    tz = ZoneInfo(TZ_NAME)
    st = _get_state(source)
    with st["lock"]:
        # Keep previous snapshots for anomaly detection
        prev_header_ts_local = st.get("header_ts_local")
        prev_trips_df = st.get("trips_df")
        prev_stops_df = st.get("stops_df")
        prev_veh_df = st.get("veh_df")
        url = SOURCES[source]
        auth = (AUTH_USER, AUTH_PASS) if AUTH_USER and AUTH_PASS else None

        try:
            verify = VERIFY_TLS_DEFAULT
            if source == "saraksti":
                verify = False  # ja tiešām pašparakstīts sertifikāts

            r = requests.get(url, timeout=REQUEST_TIMEOUT, auth=auth, verify=verify)
            r.raise_for_status()
            pb = r.content
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Failed to download GTFS-RT from {url}: {e}")

        header_ts_local, trips_df, stops_df, veh_df, alerts_df = decode_pb_bytes(pb, tz)

        # Enrich with static GTFS (stop names, route short names, direction_id...) if provided
        trips_df = enrich_trip_updates(trips_df)
        stops_df = enrich_stop_time_updates(stops_df)
        veh_df = enrich_vehicle_positions(veh_df)

        # Data-quality monitor (missing / degraded columns)
        dq_assessments = {
            "trip_updates": _dq_monitor_df(source, "trip_updates", trips_df, header_ts_local),
            "stop_time_updates": _dq_monitor_df(source, "stop_time_updates", stops_df, header_ts_local),
            "vehicle_positions": _dq_monitor_df(source, "vehicle_positions", veh_df, header_ts_local),
            "alerts": _dq_monitor_df(source, "alerts", alerts_df, header_ts_local),
        }

        penalty = 0
        issues_by_dataset: Dict[str, Any] = {}
        for ds, a in dq_assessments.items():
            if not a:
                continue
            if a.get("dataset_empty") and a.get("allowed_empty"):
                continue

            ds_pen = 0
            if a.get("dataset_empty"):
                ds_pen += 20

            issue_map = a.get("column_issues") or {}
            for col, issue in issue_map.items():
                weight = int(issue.get("weight") or 0)
                if weight <= 0:
                    continue
                kind = str(issue.get("kind") or "")
                missing_fraction = float(issue.get("missing_fraction") or 0.0)
                if kind in {"missing_column", "fully_empty"}:
                    ds_pen += weight
                else:
                    ds_pen += max(1, int(round(weight * missing_fraction)))

            if ds_pen or issue_map:
                issues_by_dataset[ds] = {
                    "missing_columns": a.get("missing_columns") or [],
                    "empty_columns": a.get("empty_columns") or {},
                    "degraded_columns": a.get("degraded_columns") or {},
                    "column_issues": issue_map,
                    "row_count": a.get("row_count"),
                }

            penalty += ds_pen

        dqi = max(0, 100 - penalty)

        dq_snapshot = {
            "dqi": dqi,
            "penalty": penalty,
            "issues": issues_by_dataset,
        }
        _dq_log_refresh(source, header_ts_local, dq_snapshot)

        try:
            db_store = store_snapshot(
                source=source,
                pb_bytes=pb,
                header_ts_local=header_ts_local,
                trips_df=trips_df,
                stops_df=stops_df,
                veh_df=veh_df,
                alerts_df=alerts_df,
                dq_summary=dq_snapshot,
            )
            st["db_last_store"] = {
                "ok": bool(db_store.ok),
                "snapshot_id": db_store.snapshot_id,
                "inserted": db_store.inserted,
                "error": db_store.error,
            }
            if not db_store.ok and db_store.error:
                print(f"[postgres] store warning for {source}: {db_store.error}")
        except Exception as e:
            st["db_last_store"] = {"ok": False, "snapshot_id": None, "inserted": {}, "error": str(e)}
            print(f"[postgres] store failed for {source}: {e}")

        write_csv(trips_df, "gtfs_trip_updates.csv", source)
        write_csv(stops_df, "gtfs_stop_time_updates.csv", source)
        write_csv(veh_df, "gtfs_vehicle_positions.csv", source)
        write_csv(alerts_df, "gtfs_alerts.csv", source)

        anomalies = detect_anomalies(
            source=source,
            state=st,
            prev_header_ts_local=prev_header_ts_local,
            prev_stops_df=prev_stops_df,
            prev_veh_df=prev_veh_df,
            prev_trips_df=prev_trips_df,
            curr_header_ts_local=header_ts_local,
            curr_stops_df=stops_df,
            curr_veh_df=veh_df,
            curr_trips_df=trips_df,
            tz=tz,
        )
        if anomalies:
            append_anomalies(source, anomalies)
            if SNAPSHOT_ON_ANOMALY:
                save_snapshot(source=source, pb=pb, tz=tz, trips_df=trips_df, stops_df=stops_df, veh_df=veh_df, alerts_df=alerts_df, anomalies=anomalies)
        elif SNAPSHOT_ALWAYS:
            save_snapshot(source=source, pb=pb, tz=tz, trips_df=trips_df, stops_df=stops_df, veh_df=veh_df, alerts_df=alerts_df, anomalies=[])

        st.update({
            "last_refresh_utc": datetime.now(timezone.utc).isoformat(),
            "header_ts_local": header_ts_local,
            "trips_df": trips_df,
            "stops_df": stops_df,
            "veh_df": veh_df,
            "alerts_df": alerts_df,
        })

    return status(source)


def status(source: str = "mobis") -> Dict[str, Any]:
    st = _get_state(source)
    return {
        "gtfs_rt_url": SOURCES[source],
        "tz": TZ_NAME,
        "refresh_seconds": REFRESH_SECONDS,
        "last_refresh_utc": st["last_refresh_utc"],
        "feed_header_timestamp_local": st["header_ts_local"],
        "static_gtfs": {
            "loaded": bool(_static_gtfs.get("loaded")),
            "path": _static_gtfs.get("path"),
            "counts": {
                "stops": int(len(_static_gtfs.get("stops", pd.DataFrame()))),
                "routes": int(len(_static_gtfs.get("routes", pd.DataFrame()))),
                "trips": int(len(_static_gtfs.get("trips", pd.DataFrame()))),
                "stop_times": int(len(_static_gtfs.get("stop_times", pd.DataFrame()))),
                "shapes": int(len(_static_gtfs.get("shapes", pd.DataFrame()))),
            },
        },
        "counts": {
            "trip_updates": int(len(st["trips_df"])) if st["trips_df"] is not None else 0,
            "stop_time_updates": int(len(st["stops_df"])) if st["stops_df"] is not None else 0,
            "vehicle_positions": int(len(st["veh_df"])) if st["veh_df"] is not None else 0,
            "alerts": int(len(st["alerts_df"])) if st["alerts_df"] is not None else 0,
            "anomalies": int(len(st.get("anomalies", []))),
        },
        "db": db_status(),
        "db_last_store": st.get("db_last_store"),
    }


def df_to_records(df: pd.DataFrame, limit: int = 5000):
    """Return JSON-safe list-of-dicts for a DataFrame.

    pandas uses NaN/Inf for missing values in numeric columns. Starlette's
    JSONResponse rejects NaN/Inf and raises:
      ValueError: Out of range float values are not JSON compliant

    We normalize NaN/Inf -> None before converting to records.
    """
    if df is None or df.empty:
        return []
    if limit and len(df) > limit:
        df = df.head(limit)

    d = df.replace([np.inf, -np.inf], np.nan).astype(object)
    d = d.where(pd.notnull(d), None)

    return [sanitize_obj(r) for r in d.to_dict(orient="records")]


# -----------------------------
# Anomaly log (between consecutive refreshes)
# -----------------------------
def _anomalies_out_path(source: str) -> Path:
    out_dir = DATA_DIR / source
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / "anomalies.csv"


def _rows_to_anomaly_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    cols = CSV_EXPECTED_COLUMNS.get("anomalies.csv", [])
    df = pd.DataFrame(rows)
    if cols:
        df = df.reindex(columns=cols)
    return df


def append_anomalies(source: str, rows: List[Dict[str, Any]]) -> int:
    """Append anomalies to in-memory state and to a persistent CSV."""
    if not rows:
        return 0

    st = _get_state(source)
    st.setdefault("anomalies", [])
    st["anomalies"].extend(rows)
    if len(st["anomalies"]) > ANOMALIES_MAX_KEEP:
        st["anomalies"] = st["anomalies"][-ANOMALIES_MAX_KEEP:]

    path = _anomalies_out_path(source)
    df = _rows_to_anomaly_df(rows)
    if path.exists():
        df.to_csv(path, mode="a", header=False, index=False)
    else:
        df.to_csv(path, index=False)
    return int(len(rows))


def _stop_events_with_key(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Normalize StopTimeUpdates -> DataFrame with a stable key column."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["__key"])

    need = {"trip_id", "stop_id", "stop_sequence"}
    if not need.issubset(df.columns):
        return pd.DataFrame(columns=["__key"])

    d = df.copy()

    # Filter SKIPPED / NO_DATA if present
    if "schedule_relationship" in d.columns:
        d = d[~d["schedule_relationship"].isin([1, 2, "1", "2", "SKIPPED", "NO_DATA"])]

    d = d.dropna(subset=["trip_id", "stop_id", "stop_sequence"]).copy()
    d["trip_id"] = d["trip_id"].astype(str)
    d["stop_id"] = d["stop_id"].astype(str)
    d["stop_sequence"] = pd.to_numeric(d["stop_sequence"], errors="coerce")
    d = d[pd.notnull(d["stop_sequence"])].copy()
    d["stop_sequence"] = d["stop_sequence"].astype(int)
    # stop_sequence=0 almost always means "missing" (some feeds omit it)
    d = d[d["stop_sequence"] > 0].copy()

    d["__key"] = d["trip_id"].astype(str) + "|" + d["stop_id"].astype(str) + "|" + d["stop_sequence"].astype(str)
    return d


def _vehicles_with_key(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["__vkey"])
    d = df.copy()
    if "vehicle_id" in d.columns:
        d["vehicle_id"] = d["vehicle_id"].astype(str)
        d["__vkey"] = d["vehicle_id"]
    else:
        d["__vkey"] = d.get("entity_id", "")
    return d


def _event_time_dt(row: pd.Series) -> Optional[datetime]:
    # Prefer arrival; fallback to departure
    t1 = parse_iso(row.get("arrival_time_local"))
    t2 = parse_iso(row.get("departure_time_local"))
    if t1 and t2:
        return min(t1, t2)
    return t1 or t2


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    # Earth radius in meters
    R = 6371000.0
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dl = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2.0) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dl / 2.0) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    return float(R * c)


def _as_local_now(curr_header_ts_local: Optional[str], tz: ZoneInfo) -> datetime:
    """
    Use GTFS-RT header timestamp as the reference "now" when possible.

    Why:
      - if your refresh happens a little late/early, using system time can create false positives.
      - header timestamp matches the producer's notion of "when this snapshot was generated".
    """
    dt = parse_iso(curr_header_ts_local)
    if isinstance(dt, datetime):
        try:
            if dt.tzinfo is None:
                return dt.replace(tzinfo=tz)
            return dt.astimezone(tz)
        except Exception:
            pass
    return datetime.now(tz)


def detect_anomalies(
    *,
    source: str,
    state: Dict[str, Any],
    prev_header_ts_local: Optional[str],
    prev_stops_df: Optional[pd.DataFrame],
    prev_veh_df: Optional[pd.DataFrame],
    prev_trips_df: Optional[pd.DataFrame],
    curr_header_ts_local: Optional[str],
    curr_stops_df: Optional[pd.DataFrame],
    curr_veh_df: Optional[pd.DataFrame],
    curr_trips_df: Optional[pd.DataFrame],
    tz: ZoneInfo,
) -> List[Dict[str, Any]]:
    """Atklāj ievērojamas “atpakaļskaitīšanas pazudusi” tipa problēmas starp secīgām datu atsvaidzināšanām.

    Par anomāliju šeit uzskatām:
      1) StopTimeUpdate notikums eksistēja (trip_id + stop_id + stop_sequence) ar derīgu, tuvā nākotnē paredzētu laiku,
         bet nākamajā atsvaidzināšanā šis notikums ir pilnībā pazudis.
      2) Esošam stop_time_update atslēgas ierakstam pazūd ierašanās/izbraukšanas laiks.
      3) VehiclePosition pazaudē koordinātas/laika zīmogu.
      4) Pieturā *nākamā ierašanās* pēkšņi “aizlec” tālā nākotnē vai pazūd.
    """
    now = _as_local_now(curr_header_ts_local, tz)
    rows: List[Dict[str, Any]] = []

    # Debounce state (persisted in-memory between refreshes)
    deb = state.setdefault("debounce", {})
    stop_missing_counts: Dict[str, int] = deb.setdefault("stop_time_event_disappeared", {})
    next_missing_counts: Dict[str, int] = deb.setdefault("next_arrival_missing", {})
    veh_coord_missing_counts: Dict[str, int] = deb.setdefault("vehicle_position_disappeared", {})
    veh_ts_missing_counts: Dict[str, int] = deb.setdefault("vehicle_timestamp_disappeared", {})

    prev_st = _stop_events_with_key(prev_stops_df)
    curr_st = _stop_events_with_key(curr_stops_df)

    prev_keys = set(prev_st.get("__key", pd.Series([], dtype=str)).astype(str).tolist())
    curr_keys = set(curr_st.get("__key", pd.Series([], dtype=str)).astype(str).tolist())
    missing_keys = list(prev_keys - curr_keys)

    # Quick lookup for current vehicles and trips
    curr_veh_trip_ids: Set[str] = set()
    if curr_veh_df is not None and not curr_veh_df.empty and "trip_id" in curr_veh_df.columns:
        curr_veh_trip_ids = set(curr_veh_df["trip_id"].dropna().astype(str).tolist())

    curr_trip_ids: Set[str] = set()
    if curr_trips_df is not None and not curr_trips_df.empty and "trip_id" in curr_trips_df.columns:
        curr_trip_ids = set(curr_trips_df["trip_id"].dropna().astype(str).tolist())

    # Map trip_id -> current vehicle row (for confidence and linking)
    veh_by_trip: Dict[str, Dict[str, Any]] = {}
    if curr_veh_df is not None and not curr_veh_df.empty and "trip_id" in curr_veh_df.columns:
        d = curr_veh_df.copy()
        for c in ["latitude", "longitude"]:
            if c in d.columns:
                d[c] = pd.to_numeric(d[c], errors="coerce")
        d = d[pd.notnull(d.get("trip_id"))].copy()
        for _, rr in d.iterrows():
            tid = rr.get("trip_id")
            if tid is None:
                continue
            tid = str(tid)
            if tid in veh_by_trip:
                continue
            veh_by_trip[tid] = {
                "vehicle_id": rr.get("vehicle_id"),
                "vehicle_label": rr.get("vehicle_label"),
                "vehicle_lat": rr.get("latitude"),
                "vehicle_lon": rr.get("longitude"),
                "vehicle_timestamp_local": rr.get("timestamp_local"),
            }

    # Helper to compute confidence from (stop, vehicle)
    def confidence_for(trip_id: Optional[str], stop_id: Optional[str], stop_lat: Any, stop_lon: Any) -> Tuple[str, Optional[float]]:
        if not trip_id or str(trip_id) not in veh_by_trip:
            return "low", None
        v = veh_by_trip.get(str(trip_id))
        vlat = v.get("vehicle_lat")
        vlon = v.get("vehicle_lon")
        if vlat is None or vlon is None or pd.isna(vlat) or pd.isna(vlon):
            return "medium", None
        if stop_lat is None or stop_lon is None or pd.isna(stop_lat) or pd.isna(stop_lon):
            return "medium", None
        try:
            dist_m = _haversine_m(float(vlat), float(vlon), float(stop_lat), float(stop_lon))
        except Exception:
            dist_m = None
        if dist_m is None:
            return "medium", None
        return ("high" if dist_m <= float(ANOMALY_NEAR_STOP_METERS) else "medium"), dist_m

    # 1) StopTimeUpdate event disappeared (debounced)
    if missing_keys and not prev_st.empty:
        prev_index = prev_st.set_index("__key")

        # Reset counters for keys that recovered
        for k in list(stop_missing_counts.keys()):
            if k in curr_keys:
                stop_missing_counts.pop(k, None)

        for k in missing_keys:
            if k not in prev_index.index:
                continue
            r = prev_index.loc[k]
            if isinstance(r, pd.DataFrame):
                r = r.iloc[0]

            t = _event_time_dt(r)
            if t is None:
                continue

            # Only consider "near future" events - classic countdown disappearing
            if t < now + pd.Timedelta(seconds=ANOMALY_MIN_FUTURE_SEC):
                continue
            if t > now + pd.Timedelta(seconds=ANOMALY_WINDOW_FUTURE_SEC):
                continue
            if t < now - pd.Timedelta(seconds=ANOMALY_WINDOW_PAST_SEC):
                continue

            # Debounce: require N consecutive misses
            stop_missing_counts[k] = int(stop_missing_counts.get(k, 0)) + 1
            if stop_missing_counts[k] < int(ANOMALY_MISSING_CONSECUTIVE):
                continue

            trip_id = str(r.get("trip_id")) if r.get("trip_id") is not None else None
            route_id = str(r.get("route_id")) if r.get("route_id") is not None else None
            stop_id = str(r.get("stop_id")) if r.get("stop_id") is not None else None
            stop_name = r.get("stop_name") or _static_lookup_stop_name(stop_id)

            stop_lat = r.get("stop_lat")
            stop_lon = r.get("stop_lon")
            if (stop_lat is None or pd.isna(stop_lat) or stop_lon is None or pd.isna(stop_lon)) and stop_id:
                sm = _static_lookup_stop(stop_id)
                stop_lat = sm.get("stop_lat")
                stop_lon = sm.get("stop_lon")

            conf, dist_m = confidence_for(trip_id, stop_id, stop_lat, stop_lon)
            v = veh_by_trip.get(str(trip_id), {}) if trip_id else {}

            note_bits = [f"StopTimeUpdate event disappeared (>= {ANOMALY_MISSING_CONSECUTIVE} refreshes)"]
            if trip_id and trip_id in curr_veh_trip_ids:
                note_bits.append("vehicle still present")
            if trip_id and trip_id not in curr_trip_ids:
                note_bits.append("trip_update missing")

            rows.append({
                "detection_time_local": now.isoformat(),
                "source": source,
                "kind": "stop_time_event_disappeared",
                "confidence": conf,
                "consecutive_misses": int(stop_missing_counts.get(k, 0)),
                "trip_id": trip_id,
                "route_id": route_id,
                "route_short_name": r.get("route_short_name"),
                "stop_id": stop_id,
                "stop_name": stop_name,
                "stop_sequence": json_safe(r.get("stop_sequence")),
                "stop_lat": json_safe(stop_lat),
                "stop_lon": json_safe(stop_lon),
                "vehicle_id": v.get("vehicle_id"),
                "vehicle_lat": json_safe(v.get("vehicle_lat")),
                "vehicle_lon": json_safe(v.get("vehicle_lon")),
                "distance_to_stop_m": json_safe(dist_m),
                "vehicle_timestamp_local": v.get("vehicle_timestamp_local"),
                "prev_arrival_time_local": r.get("arrival_time_local"),
                "prev_departure_time_local": r.get("departure_time_local"),
                "curr_arrival_time_local": None,
                "curr_departure_time_local": None,
                "prev_header_ts_local": prev_header_ts_local,
                "curr_header_ts_local": curr_header_ts_local,
                "note": "; ".join(note_bits),
            })

    # 2) StopTimeUpdate time field disappeared (same key exists, but time became empty)
    common_keys = list(prev_keys & curr_keys)
    if common_keys and not prev_st.empty and not curr_st.empty:
        prev_idx = prev_st.set_index("__key")
        curr_idx = curr_st.set_index("__key")
        for k in common_keys[:ANOMALIES_MAX_PER_REFRESH]:
            if k not in prev_idx.index or k not in curr_idx.index:
                continue
            pr = prev_idx.loc[k]
            cr = curr_idx.loc[k]
            if isinstance(pr, pd.DataFrame):
                pr = pr.iloc[0]
            if isinstance(cr, pd.DataFrame):
                cr = cr.iloc[0]

            def mk_row(kind: str, note: str):
                trip_id = str(pr.get("trip_id")) if pr.get("trip_id") is not None else None
                stop_id = str(pr.get("stop_id")) if pr.get("stop_id") is not None else None
                stop_name = pr.get("stop_name") or _static_lookup_stop_name(stop_id)
                stop_lat = pr.get("stop_lat")
                stop_lon = pr.get("stop_lon")
                if (stop_lat is None or pd.isna(stop_lat) or stop_lon is None or pd.isna(stop_lon)) and stop_id:
                    sm = _static_lookup_stop(stop_id)
                    stop_lat = sm.get("stop_lat")
                    stop_lon = sm.get("stop_lon")
                conf, dist_m = confidence_for(trip_id, stop_id, stop_lat, stop_lon)
                v = veh_by_trip.get(str(trip_id), {}) if trip_id else {}
                return {
                    "detection_time_local": now.isoformat(),
                    "source": source,
                    "kind": kind,
                    "confidence": conf,
                    "consecutive_misses": None,
                    "trip_id": trip_id,
                    "route_id": str(pr.get("route_id")) if pr.get("route_id") is not None else None,
                    "route_short_name": pr.get("route_short_name"),
                    "stop_id": stop_id,
                    "stop_name": stop_name,
                    "stop_sequence": json_safe(pr.get("stop_sequence")),
                    "stop_lat": json_safe(stop_lat),
                    "stop_lon": json_safe(stop_lon),
                    "vehicle_id": v.get("vehicle_id"),
                    "vehicle_lat": json_safe(v.get("vehicle_lat")),
                    "vehicle_lon": json_safe(v.get("vehicle_lon")),
                    "distance_to_stop_m": json_safe(dist_m),
                    "vehicle_timestamp_local": v.get("vehicle_timestamp_local"),
                    "prev_arrival_time_local": pr.get("arrival_time_local"),
                    "prev_departure_time_local": pr.get("departure_time_local"),
                    "curr_arrival_time_local": cr.get("arrival_time_local"),
                    "curr_departure_time_local": cr.get("departure_time_local"),
                    "prev_header_ts_local": prev_header_ts_local,
                    "curr_header_ts_local": curr_header_ts_local,
                    "note": note,
                }

            if pr.get("arrival_time_local") and not cr.get("arrival_time_local"):
                rows.append(mk_row("stop_time_arrival_disappeared", "arrival_time disappeared for existing stop_time_update"))
            if pr.get("departure_time_local") and not cr.get("departure_time_local"):
                rows.append(mk_row("stop_time_departure_disappeared", "departure_time disappeared for existing stop_time_update"))

    # 3) Vehicle position lost coordinates/timestamp (debounced)
    prev_v = _vehicles_with_key(prev_veh_df)
    curr_v = _vehicles_with_key(curr_veh_df)
    if not prev_v.empty and not curr_v.empty and "__vkey" in prev_v.columns and "__vkey" in curr_v.columns:
        pidx = prev_v.set_index("__vkey")
        cidx = curr_v.set_index("__vkey")
        common_v = list(set(pidx.index.astype(str)) & set(cidx.index.astype(str)))

        # Reset counters for recovered vehicles
        for k in list(veh_coord_missing_counts.keys()):
            if k in cidx.index:
                rr = cidx.loc[k]
                if isinstance(rr, pd.DataFrame):
                    rr = rr.iloc[0]
                if rr.get("latitude") not in (None, "") and rr.get("longitude") not in (None, ""):
                    veh_coord_missing_counts.pop(k, None)
        for k in list(veh_ts_missing_counts.keys()):
            if k in cidx.index:
                rr = cidx.loc[k]
                if isinstance(rr, pd.DataFrame):
                    rr = rr.iloc[0]
                if rr.get("timestamp_local") not in (None, ""):
                    veh_ts_missing_counts.pop(k, None)

        def v_missing(prev_val: Any, curr_val: Any) -> bool:
            return prev_val not in (None, "", np.nan) and (curr_val in (None, "", np.nan) or (isinstance(curr_val, float) and np.isnan(curr_val)))

        for k in common_v[:ANOMALIES_MAX_PER_REFRESH]:
            pr = pidx.loc[k]
            cr = cidx.loc[k]
            if isinstance(pr, pd.DataFrame):
                pr = pr.iloc[0]
            if isinstance(cr, pd.DataFrame):
                cr = cr.iloc[0]

            vid = str(pr.get("vehicle_id")) if pr.get("vehicle_id") is not None else str(k)

            if v_missing(pr.get("latitude"), cr.get("latitude")) or v_missing(pr.get("longitude"), cr.get("longitude")):
                veh_coord_missing_counts[vid] = int(veh_coord_missing_counts.get(vid, 0)) + 1
                if veh_coord_missing_counts[vid] >= int(ANOMALY_MISSING_CONSECUTIVE):
                    rows.append({
                        "detection_time_local": now.isoformat(),
                        "source": source,
                        "kind": "vehicle_position_disappeared",
                        "confidence": "medium",
                        "consecutive_misses": int(veh_coord_missing_counts.get(vid, 0)),
                        "trip_id": str(pr.get("trip_id")) if pr.get("trip_id") is not None else None,
                        "route_id": str(pr.get("route_id")) if pr.get("route_id") is not None else None,
                        "route_short_name": pr.get("route_short_name"),
                        "stop_id": str(pr.get("stop_id")) if pr.get("stop_id") is not None else None,
                        "stop_name": pr.get("stop_name"),
                        "stop_sequence": None,
                        "stop_lat": None,
                        "stop_lon": None,
                        "vehicle_id": vid,
                        "vehicle_lat": json_safe(pr.get("latitude")),
                        "vehicle_lon": json_safe(pr.get("longitude")),
                        "distance_to_stop_m": None,
                        "vehicle_timestamp_local": pr.get("timestamp_local"),
                        "prev_arrival_time_local": None,
                        "prev_departure_time_local": None,
                        "curr_arrival_time_local": None,
                        "curr_departure_time_local": None,
                        "prev_header_ts_local": prev_header_ts_local,
                        "curr_header_ts_local": curr_header_ts_local,
                        "note": "vehicle lost lat/lon between refreshes",
                    })

            if v_missing(pr.get("timestamp_local"), cr.get("timestamp_local")):
                veh_ts_missing_counts[vid] = int(veh_ts_missing_counts.get(vid, 0)) + 1
                if veh_ts_missing_counts[vid] >= int(ANOMALY_MISSING_CONSECUTIVE):
                    rows.append({
                        "detection_time_local": now.isoformat(),
                        "source": source,
                        "kind": "vehicle_timestamp_disappeared",
                        "confidence": "medium",
                        "consecutive_misses": int(veh_ts_missing_counts.get(vid, 0)),
                        "trip_id": str(pr.get("trip_id")) if pr.get("trip_id") is not None else None,
                        "route_id": str(pr.get("route_id")) if pr.get("route_id") is not None else None,
                        "route_short_name": pr.get("route_short_name"),
                        "stop_id": str(pr.get("stop_id")) if pr.get("stop_id") is not None else None,
                        "stop_name": pr.get("stop_name"),
                        "stop_sequence": None,
                        "stop_lat": None,
                        "stop_lon": None,
                        "vehicle_id": vid,
                        "vehicle_lat": json_safe(pr.get("latitude")),
                        "vehicle_lon": json_safe(pr.get("longitude")),
                        "distance_to_stop_m": None,
                        "vehicle_timestamp_local": pr.get("timestamp_local"),
                        "prev_arrival_time_local": None,
                        "prev_departure_time_local": None,
                        "curr_arrival_time_local": None,
                        "curr_departure_time_local": None,
                        "prev_header_ts_local": prev_header_ts_local,
                        "curr_header_ts_local": curr_header_ts_local,
                        "note": "vehicle timestamp disappeared",
                    })

    # 4) "Next arrival" missing / jump (debounced missing)
    if prev_stops_df is not None and curr_stops_df is not None:
        def next_arrivals(df: pd.DataFrame) -> pd.DataFrame:
            if df is None or df.empty:
                return pd.DataFrame(columns=["route_id", "stop_id", "arrival_dt", "trip_id", "arrival_time_local"])
            d = df.copy()
            if "arrival_time_local" not in d.columns:
                return pd.DataFrame(columns=["route_id", "stop_id", "arrival_dt", "trip_id", "arrival_time_local"])
            d = d[pd.notnull(d.get("route_id")) & pd.notnull(d.get("stop_id"))].copy()
            d["arrival_dt"] = d["arrival_time_local"].apply(parse_iso)
            d = d[pd.notnull(d["arrival_dt"])].copy()
            d = d[(d["arrival_dt"] >= now - pd.Timedelta(seconds=ANOMALY_WINDOW_PAST_SEC)) & (d["arrival_dt"] <= now + pd.Timedelta(seconds=ANOMALY_NEXT_ARRIVAL_MAX_SEC))]
            # Focus on future "countdown" and ignore arrivals that are basically "now"
            d = d[d["arrival_dt"] >= now + pd.Timedelta(seconds=ANOMALY_MIN_FUTURE_SEC)]
            if d.empty:
                return pd.DataFrame(columns=["route_id", "stop_id", "arrival_dt", "trip_id", "arrival_time_local"])
            d = d.sort_values(["route_id", "stop_id", "arrival_dt"], kind="mergesort")
            return d.groupby(["route_id", "stop_id"], as_index=False).first()

        p = next_arrivals(prev_stops_df)
        c = next_arrivals(curr_stops_df)

        # Reset counters for groups that recovered
        curr_groups = set()
        if not c.empty:
            c["__g"] = c["route_id"].astype(str) + "|" + c["stop_id"].astype(str)
            curr_groups = set(c["__g"].astype(str).tolist())
        for g in list(next_missing_counts.keys()):
            if g in curr_groups:
                next_missing_counts.pop(g, None)

        if not p.empty:
            p["__g"] = p["route_id"].astype(str) + "|" + p["stop_id"].astype(str)
            cidx = c.set_index("__g") if not c.empty else None

            for _, pr in p.iterrows():
                g = pr["__g"]
                if cidx is None or g not in cidx.index:
                    # Debounce missing next arrival
                    next_missing_counts[g] = int(next_missing_counts.get(g, 0)) + 1
                    if next_missing_counts[g] < int(ANOMALY_MISSING_CONSECUTIVE):
                        continue

                    stop_id = pr.get("stop_id")
                    stop_name = _static_lookup_stop_name(stop_id)
                    sm = _static_lookup_stop(stop_id)
                    rows.append({
                        "detection_time_local": now.isoformat(),
                        "source": source,
                        "kind": "next_arrival_missing",
                        "confidence": "low",
                        "consecutive_misses": int(next_missing_counts.get(g, 0)),
                        "trip_id": pr.get("trip_id"),
                        "route_id": pr.get("route_id"),
                        "route_short_name": pr.get("route_short_name"),
                        "stop_id": stop_id,
                        "stop_name": stop_name,
                        "stop_sequence": None,
                        "stop_lat": json_safe(sm.get("stop_lat")),
                        "stop_lon": json_safe(sm.get("stop_lon")),
                        "vehicle_id": None,
                        "vehicle_lat": None,
                        "vehicle_lon": None,
                        "distance_to_stop_m": None,
                        "vehicle_timestamp_local": None,
                        "prev_arrival_time_local": pr.get("arrival_time_local"),
                        "prev_departure_time_local": None,
                        "curr_arrival_time_local": None,
                        "curr_departure_time_local": None,
                        "prev_header_ts_local": prev_header_ts_local,
                        "curr_header_ts_local": curr_header_ts_local,
                        "note": "Previously had a near-future next arrival; now no near-future arrival exists",
                    })
                else:
                    cr = cidx.loc[g]
                    if isinstance(cr, pd.DataFrame):
                        cr = cr.iloc[0]
                    if pr.get("trip_id") != cr.get("trip_id"):
                        dt_prev = pr.get("arrival_dt")
                        dt_curr = cr.get("arrival_dt")
                        if isinstance(dt_prev, datetime) and isinstance(dt_curr, datetime):
                            jump_sec = int((dt_curr - dt_prev).total_seconds())
                            if jump_sec >= ANOMALY_JUMP_THRESHOLD_SEC:
                                stop_id = pr.get("stop_id")
                                stop_name = _static_lookup_stop_name(stop_id)
                                sm = _static_lookup_stop(stop_id)
                                rows.append({
                                    "detection_time_local": now.isoformat(),
                                    "source": source,
                                    "kind": "next_arrival_jump",
                                    "confidence": "low",
                                    "consecutive_misses": None,
                                    "trip_id": pr.get("trip_id"),
                                    "route_id": pr.get("route_id"),
                                    "route_short_name": pr.get("route_short_name"),
                                    "stop_id": stop_id,
                                    "stop_name": stop_name,
                                    "stop_sequence": None,
                                    "stop_lat": json_safe(sm.get("stop_lat")),
                                    "stop_lon": json_safe(sm.get("stop_lon")),
                                    "vehicle_id": None,
                                    "vehicle_lat": None,
                                    "vehicle_lon": None,
                                    "distance_to_stop_m": None,
                                    "vehicle_timestamp_local": None,
                                    "prev_arrival_time_local": pr.get("arrival_time_local"),
                                    "prev_departure_time_local": None,
                                    "curr_arrival_time_local": cr.get("arrival_time_local"),
                                    "curr_departure_time_local": None,
                                    "prev_header_ts_local": prev_header_ts_local,
                                    "curr_header_ts_local": curr_header_ts_local,
                                    "note": f"Next arrival changed from trip {pr.get('trip_id')} to {cr.get('trip_id')} (jump {jump_sec}s)",
                                })

    if len(rows) > ANOMALIES_MAX_PER_REFRESH:
        rows = rows[:ANOMALIES_MAX_PER_REFRESH]
    return rows


def _stop_key_set(df: pd.DataFrame) -> Set[Tuple[str, str, int]]:
    """
    Notikums = (trip_id, stop_id, stop_sequence)
    """
    if df is None or df.empty:
        return set()

    need = {"trip_id", "stop_id", "stop_sequence"}
    if not need.issubset(df.columns):
        return set()

    d = df[["trip_id", "stop_id", "stop_sequence"]].copy()

    # ignorējam SKIPPED(1) un NO_DATA(2) ja ir
    if "schedule_relationship" in df.columns:
        rel = df["schedule_relationship"]
        d = d[~rel.isin([1, 2])]

    d = d.dropna()

    # normalizācija
    d["trip_id"] = d["trip_id"].astype(str)
    d["stop_id"] = d["stop_id"].astype(str)
    d["stop_sequence"] = pd.to_numeric(d["stop_sequence"], errors="coerce")
    d = d.dropna()
    d["stop_sequence"] = d["stop_sequence"].astype(int)
    d = d[d["stop_sequence"] > 0].copy()

    return set(d.itertuples(index=False, name=None))


def _stop_id_set(df: pd.DataFrame) -> Set[str]:
    """
    Pietura = stop_id (agregācija virs notikumiem)
    """
    if df is None or df.empty or "stop_id" not in df.columns:
        return set()

    d = df[["stop_id"]].copy()

    if "schedule_relationship" in df.columns:
        rel = df["schedule_relationship"]
        d = d[~rel.isin([1, 2])]

    d = d.dropna()
    return set(d["stop_id"].astype(str).tolist())


def keys_to_samples(keys: Set[Tuple[str, str, int]], missing_from: str, limit: int = 50):
    out = []
    for (trip_id, stop_id, stop_sequence) in list(keys)[:limit]:
        out.append({
            "trip_id": json_safe(trip_id),
            "stop_id": json_safe(stop_id),
            "stop_name": _static_lookup_stop_name(stop_id),
            "stop_sequence": json_safe(stop_sequence),
            "missing_from": missing_from,
        })
    return out


def stop_ids_to_samples(stop_ids: Set[str], missing_from: str, limit: int = 50):
    out = []
    for stop_id in list(stop_ids)[:limit]:
        out.append({
            "stop_id": str(stop_id),
            "stop_name": _static_lookup_stop_name(stop_id),
            "missing_from": missing_from,
        })
    return out




def _cleanup_worker():
    interval_s = max(60, int(DB_CLEANUP_INTERVAL_MINUTES) * 60)
    retention_days = max(1, int(DB_RETENTION_DAYS))

    while True:
        started = time.monotonic()
        try:
            result = cleanup_old_history(retention_days)
            if result.get("ok"):
                print(f"[db-cleanup] ok retention_days={retention_days} deleted={result.get('deleted')}")
            else:
                print(f"[db-cleanup] error: {result.get('error')}")
        except Exception as e:
            print(f"[db-cleanup] worker error: {e}")

        elapsed = time.monotonic() - started
        time.sleep(max(0.0, interval_s - elapsed))

# -----------------------------
# API
# -----------------------------
@app.on_event("startup")
def _startup():
    try:
        db_init = init_database()
        print(f"[postgres] enabled={db_init.get('enabled')} connected={db_init.get('connected')} initialized={db_init.get('initialized')}")
        if db_init.get("error"):
            print(f"[postgres] init warning: {db_init.get('error')}")
    except Exception as e:
        print(f"[postgres] init failed: {e}")

    # Load static GTFS (for stop names, route short names, directions...) if configured.
    if STATIC_GTFS_PATH:
        try:
            gtfs_static = load_static_gtfs(STATIC_GTFS_PATH)
            _static_gtfs.update(gtfs_static)
            _static_gtfs["loaded"] = True
            _static_gtfs["path"] = STATIC_GTFS_PATH
            # Build small indexes for faster lookups in API (map popups, anomaly enrichment, etc.)
            if isinstance(_static_gtfs.get("stops"), pd.DataFrame) and not _static_gtfs["stops"].empty and "stop_id" in _static_gtfs["stops"].columns:
                _static_gtfs["stops_idx"] = _static_gtfs["stops"].set_index("stop_id", drop=False)
            if isinstance(_static_gtfs.get("routes"), pd.DataFrame) and not _static_gtfs["routes"].empty and "route_id" in _static_gtfs["routes"].columns:
                _static_gtfs["routes_idx"] = _static_gtfs["routes"].set_index("route_id", drop=False)
            if isinstance(_static_gtfs.get("trips"), pd.DataFrame) and not _static_gtfs["trips"].empty and "trip_id" in _static_gtfs["trips"].columns:
                _static_gtfs["trips_idx"] = _static_gtfs["trips"].set_index("trip_id", drop=False)

            # Derived lookup: (trip_id, stop_id) -> stop_sequence + scheduled times
            try:
                stt = _static_gtfs.get("stop_times")
                if isinstance(stt, pd.DataFrame) and not stt.empty and {"trip_id", "stop_id", "stop_sequence"}.issubset(stt.columns):
                    kdf = stt[[c for c in ["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time"] if c in stt.columns]].copy()
                    kdf["trip_id"] = kdf["trip_id"].astype(str)
                    kdf["stop_id"] = kdf["stop_id"].astype(str)
                    kdf["stop_sequence"] = pd.to_numeric(kdf["stop_sequence"], errors="coerce")
                    kdf = kdf[pd.notnull(kdf["stop_sequence"])].copy()
                    kdf["stop_sequence"] = kdf["stop_sequence"].astype(int)
                    kdf = kdf.sort_values(["trip_id", "stop_id", "stop_sequence"], kind="mergesort")
                    kdf = kdf.drop_duplicates(subset=["trip_id", "stop_id"], keep="first")
                    _static_gtfs["stop_times_key"] = kdf
            except Exception:
                _static_gtfs["stop_times_key"] = None

            print(
                f"[static-gtfs] loaded from {STATIC_GTFS_PATH}: "
                f"stops={len(_static_gtfs['stops'])} routes={len(_static_gtfs['routes'])} "
                f"trips={len(_static_gtfs['trips'])} stop_times={len(_static_gtfs['stop_times'])} "
                f"shapes={len(_static_gtfs.get('shapes', pd.DataFrame()))}"
            )
        except Exception as e:
            _static_gtfs["loaded"] = False
            _static_gtfs["path"] = STATIC_GTFS_PATH
            print(f"[static-gtfs] failed to load from {STATIC_GTFS_PATH}: {e}")

    for s in list(SOURCES.keys()):
        try:
            refresh_now(s)
            # Ensure anomalies.csv exists even if no anomalies were detected yet
            write_csv(pd.DataFrame(columns=CSV_EXPECTED_COLUMNS["anomalies.csv"]), "anomalies.csv", s)
        except Exception:
            continue


       # Start background workers (optional)
    global _background_threads_started
    if not _background_threads_started:
        if AUTO_REFRESH:
            for src in SOURCES.keys():
                t = threading.Thread(target=_auto_refresh_worker, args=(src,), daemon=True)
                t.start()
            print(f"[auto-refresh] enabled for {len(SOURCES)} sources (every {REFRESH_SECONDS}s)")

        if ARCHIVE_ENABLED:
            t = threading.Thread(target=_archive_worker, daemon=True)
            t.start()
            print(f"[archive] enabled (every {ARCHIVE_INTERVAL_MINUTES} min; append_snapshots={ARCHIVE_APPEND_SNAPSHOTS}; workbook={ARCHIVE_WRITE_WORKBOOK})")

        if STATIC_GTFS_AUTO_UPDATE:
            t = threading.Thread(target=_static_gtfs_daily_worker, daemon=True)
            t.start()
            print(f"[static-gtfs] auto-update enabled (daily at {STATIC_GTFS_UPDATE_TIME})")

        if DB_CLEANUP_ENABLED:
            t = threading.Thread(target=_cleanup_worker, daemon=True)
            t.start()
            print(f"[db-cleanup] enabled (retention_days={DB_RETENTION_DAYS}; every {DB_CLEANUP_INTERVAL_MINUTES} min)")

        _background_threads_started = True


@app.get("/api/sources")
def api_sources():
    return {"sources": [{"name": k, "url": v} for k, v in SOURCES.items()]}


@app.get("/api/status")
def api_status(source: str = "mobis"):
    return status(source)


@app.get("/api/archive/status")
def api_archive_status():
    """Show archive/automation configuration and last archive time per source."""
    return {
        "auto_refresh": AUTO_REFRESH,
        "archive": {
            "enabled": ARCHIVE_ENABLED,
            "interval_minutes": ARCHIVE_INTERVAL_MINUTES,
            "append_snapshots": ARCHIVE_APPEND_SNAPSHOTS,
            "write_workbook": ARCHIVE_WRITE_WORKBOOK,
            "force_refresh": ARCHIVE_FORCE_REFRESH,
        },
        "sources": {
            s: {
                "last_refresh_utc": _get_state(s).get("last_refresh_utc"),
                "last_archive_utc": _get_state(s).get("last_archive_utc"),
            }
            for s in SOURCES.keys()
        },
    }


@app.post("/api/archive/run")
def api_archive_run(source: str = "mobis", force_refresh: bool = True):
    """Trigger an archive snapshot immediately."""
    return archive_snapshot(source, force_refresh=force_refresh)


@app.post("/api/refresh")
def api_refresh(source: str = "mobis"):
    return refresh_now(source)


@app.get("/api/compare")
def api_compare(source_a: str = "mobis", source_b: str = "saraksti"):
    a = _get_state(source_a)
    b = _get_state(source_b)

    # 1) notikumi (trip+stop+seq)
    a_keys = _stop_key_set(a["stops_df"])
    b_keys = _stop_key_set(b["stops_df"])
    a_minus_b = a_keys - b_keys
    b_minus_a = b_keys - a_keys

    # 2) pieturas (stop_id)
    a_stop_ids = _stop_id_set(a["stops_df"])
    b_stop_ids = _stop_id_set(b["stops_df"])
    a_stop_minus_b = a_stop_ids - b_stop_ids
    b_stop_minus_a = b_stop_ids - a_stop_ids

    return {
        "source_a": source_a,
        "source_b": source_b,
        "last_refresh_utc": {
            source_a: a["last_refresh_utc"],
            source_b: b["last_refresh_utc"],
        },
        "counts": {
            source_a: {
                "trip_updates": int(len(a["trips_df"])) if a["trips_df"] is not None else 0,
                "stop_time_updates": int(len(a["stops_df"])) if a["stops_df"] is not None else 0,
                "vehicle_positions": int(len(a["veh_df"])) if a["veh_df"] is not None else 0,
                "alerts": int(len(a["alerts_df"])) if a["alerts_df"] is not None else 0,
            },
            source_b: {
                "trip_updates": int(len(b["trips_df"])) if b["trips_df"] is not None else 0,
                "stop_time_updates": int(len(b["stops_df"])) if b["stops_df"] is not None else 0,
                "vehicle_positions": int(len(b["veh_df"])) if b["veh_df"] is not None else 0,
                "alerts": int(len(b["alerts_df"])) if b["alerts_df"] is not None else 0,
            },
        },

        "missing_stop_time_events": {
            f"{source_a}_not_in_{source_b}": int(len(a_minus_b)),
            f"{source_b}_not_in_{source_a}": int(len(b_minus_a)),
        },

        "missing_stop_ids": {
            f"{source_a}_not_in_{source_b}": int(len(a_stop_minus_b)),
            f"{source_b}_not_in_{source_a}": int(len(b_stop_minus_a)),
        },

        "samples": {
            f"{source_a}_events_not_in_{source_b}": keys_to_samples(a_minus_b, source_b),
            f"{source_b}_events_not_in_{source_a}": keys_to_samples(b_minus_a, source_a),
            f"{source_a}_stop_ids_not_in_{source_b}": stop_ids_to_samples(a_stop_minus_b, source_b),
            f"{source_b}_stop_ids_not_in_{source_a}": stop_ids_to_samples(b_stop_minus_a, source_a),
        },
    }


@app.get("/api/trip-updates")
def api_trip_updates(source: str = "mobis", limit: int = 5000):
    st = _get_state(source)
    return df_to_records(st["trips_df"], limit=limit)


@app.get("/api/stop-time-updates")
def api_stop_time_updates(
    source: str = "mobis",
    limit: int = 5000,
    trip_id: Optional[str] = None,
    route_id: Optional[str] = None,
):
    st = _get_state(source)
    df = st["stops_df"]
    if df is None or df.empty:
        return []
    if trip_id:
        df = df[df["trip_id"] == trip_id]
    if route_id:
        df = df[df["route_id"] == route_id]
    return df_to_records(df, limit=limit)


@app.get("/api/vehicles")
def api_vehicles(source: str = "mobis", limit: int = 5000, route_id: Optional[str] = None):
    st = _get_state(source)
    df = st["veh_df"]
    if df is None or df.empty:
        return []
    if route_id:
        df = df[df["route_id"] == route_id]
    return df_to_records(df, limit=limit)


@app.get("/api/alerts")
def api_alerts(source: str = "mobis", limit: int = 5000):
    st = _get_state(source)
    return df_to_records(st["alerts_df"], limit=limit)


@app.get("/api/anomalies")
def api_anomalies(source: str = "mobis", limit: int = 5000, kind: Optional[str] = None):
    """Return recent anomalies detected between consecutive refreshes."""
    st = _get_state(source)
    rows = list(st.get("anomalies", []))
    if kind:
        rows = [r for r in rows if str(r.get("kind")) == str(kind)]
    # newest last -> return newest first for UX
    rows = rows[-limit:][::-1] if limit else rows[::-1]
    return sanitize_obj(rows)


@app.post("/api/anomalies/clear")
def api_anomalies_clear(source: str = "mobis"):
    """Clear in-memory anomalies (does not delete persisted CSV)."""
    st = _get_state(source)
    st["anomalies"] = []
    return {"ok": True}


# -----------------------------
# Trace / timeline endpoints (operator workflow)
# -----------------------------
def _build_trip_timeline(source: str, trip_id: str) -> Dict[str, Any]:
    """Combine static stop sequence with current realtime predictions for a trip."""
    if not static_gtfs_loaded():
        return {"ok": False, "error": "Static GTFS not loaded. Set STATIC_GTFS_PATH in backend env."}

    trip_id = str(trip_id)
    trips = _static_gtfs.get("trips")
    stop_times = _static_gtfs.get("stop_times")
    stops = _static_gtfs.get("stops")

    if not isinstance(stop_times, pd.DataFrame) or stop_times.empty:
        return {"ok": False, "error": "Static stop_times.txt not available in loaded GTFS."}

    # Static trip meta (best effort)
    meta = {"trip_id": trip_id}
    try:
        trow = _static_gtfs.get("trips_idx").loc[trip_id] if _static_gtfs.get("trips_idx") is not None else None
        if isinstance(trow, pd.Series):
            route_id = trow.get("route_id")
            meta.update({
                "route_id": str(route_id) if route_id is not None else None,
                "direction_id": json_safe(trow.get("direction_id")),
                "trip_headsign": trow.get("trip_headsign"),
                "shape_id": trow.get("shape_id"),
                "route_short_name": _static_lookup_route_short_name(route_id),
                "route_long_name": _static_lookup_route_long_name(route_id),
            })
    except Exception:
        pass

    # Static stop sequence
    st_df = stop_times[stop_times["trip_id"].astype(str) == trip_id].copy()
    if st_df.empty:
        return {"ok": False, "error": f"Trip {trip_id} not found in static stop_times."}

    # Keep only essential static cols
    keep = [c for c in ["stop_sequence", "stop_id", "arrival_time", "departure_time"] if c in st_df.columns]
    st_df = st_df[keep].copy()
    st_df["stop_sequence"] = pd.to_numeric(st_df.get("stop_sequence"), errors="coerce")
    st_df = st_df[pd.notnull(st_df["stop_sequence"])].copy()
    st_df["stop_sequence"] = st_df["stop_sequence"].astype(int)
    st_df = st_df.sort_values("stop_sequence", kind="mergesort")

    st_df = st_df.rename(columns={
        "arrival_time": "scheduled_arrival_time",
        "departure_time": "scheduled_departure_time",
    })

    if isinstance(stops, pd.DataFrame) and not stops.empty and "stop_id" in st_df.columns and "stop_id" in stops.columns:
        st_df["stop_id"] = st_df["stop_id"].astype(str)
        st_df = st_df.merge(stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]], on="stop_id", how="left")

    # Current realtime for this trip
    rt = _get_state(source).get("stops_df")
    rt_df = pd.DataFrame()
    if isinstance(rt, pd.DataFrame) and not rt.empty and "trip_id" in rt.columns:
        rt_df = rt[rt["trip_id"].astype(str) == trip_id].copy()
        if not rt_df.empty:
            # Join key
            if "stop_sequence" in rt_df.columns:
                rt_df["stop_sequence"] = pd.to_numeric(rt_df["stop_sequence"], errors="coerce")
            cols = [c for c in [
                "stop_sequence", "stop_id",
                "arrival_time_local", "departure_time_local",
                "arrival_delay_sec", "departure_delay_sec",
                "schedule_relationship",
                "feed_header_timestamp_local",
            ] if c in rt_df.columns]
            rt_df = rt_df[cols].copy()

    # --- BEST-EFFORT join static stop_times with RT stop_time_updates ---
    # Goal: show RT times even when RT feed does NOT include stop_sequence (common case).

    timeline = None
    if not rt_df.empty:
        # normalize keys
        if "stop_id" in rt_df.columns:
            rt_df["stop_id"] = rt_df["stop_id"].astype(str)
        if "stop_sequence" in rt_df.columns:
            rt_df["stop_sequence"] = pd.to_numeric(rt_df["stop_sequence"], errors="coerce")

        has_stop_id = "stop_id" in rt_df.columns and rt_df["stop_id"].notna().any()
        has_seq = "stop_sequence" in rt_df.columns and rt_df["stop_sequence"].notna().any()

        seq_fill = float(rt_df["stop_sequence"].notna().mean()) if "stop_sequence" in rt_df.columns else 0.0

        # 1) Prefer strict join when stop_sequence is mostly present
        if has_stop_id and has_seq and seq_fill >= 0.5:
            st_df["stop_sequence"] = pd.to_numeric(st_df["stop_sequence"], errors="coerce").astype("Int64")
            rt_df["stop_sequence"] = rt_df["stop_sequence"].round().astype("Int64")
            timeline = st_df.merge(rt_df, on=["stop_sequence", "stop_id"], how="left")

        # 2) Fallback: join by stop_id only (most common RT format)
        elif has_stop_id:
            # If multiple RT rows per stop_id, keep the most recent one
            if "feed_header_timestamp_local" in rt_df.columns:
                rt_df = rt_df.sort_values("feed_header_timestamp_local")
            rt_df_dedup = rt_df.drop_duplicates(subset=["stop_id"], keep="last")
            timeline = st_df.merge(rt_df_dedup, on=["stop_id"], how="left")

        # 3) Rare fallback: join by stop_sequence only
        elif has_seq:
            st_df["stop_sequence"] = pd.to_numeric(st_df["stop_sequence"], errors="coerce").astype("Int64")
            rt_df["stop_sequence"] = rt_df["stop_sequence"].round().astype("Int64")
            rt_df_dedup = rt_df.drop_duplicates(subset=["stop_sequence"], keep="last")
            timeline = st_df.merge(rt_df_dedup, on=["stop_sequence"], how="left")

    # If we couldn't join RT at all, keep static only and add empty RT columns
    if timeline is None:
        timeline = st_df.copy()
        for c in [
            "arrival_time_local",
            "departure_time_local",
            "arrival_delay_sec",
            "departure_delay_sec",
            "schedule_relationship",
            "feed_header_timestamp_local",
        ]:
            if c not in timeline.columns:
                timeline[c] = None


    # Attach current vehicle (if any)
    veh = _get_state(source).get("veh_df")
    vehicle = None
    if isinstance(veh, pd.DataFrame) and not veh.empty and "trip_id" in veh.columns:
        vv = veh[veh["trip_id"].astype(str) == trip_id].copy()
        if not vv.empty:
            r0 = vv.iloc[0].to_dict()
            vehicle = {
                "vehicle_id": r0.get("vehicle_id"),
                "vehicle_label": r0.get("vehicle_label"),
                "latitude": json_safe(r0.get("latitude")),
                "longitude": json_safe(r0.get("longitude")),
                "timestamp_local": r0.get("timestamp_local"),
                "route_id": r0.get("route_id"),
                "route_short_name": r0.get("route_short_name"),
            }

    return {"ok": True, "meta": meta, "vehicle": vehicle, "timeline": df_to_records(timeline, limit=5000)}


@app.get("/api/trip/timeline")
def api_trip_timeline(source: str = "mobis", trip_id: str = ""):
    if not trip_id:
        return {"ok": False, "error": "trip_id is required"}
    return _build_trip_timeline(source, trip_id)


@app.get("/api/trace")
def api_trace(
    source: str = "mobis",
    vehicle_id: Optional[str] = None,
    trip_id: Optional[str] = None,
    route_id: Optional[str] = None,
    direction_id: Optional[int] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 500,
):
    """DB-based trace/history lookup by vehicle_id / trip_id / route_id / direction_id."""
    try:
        return fetch_trace_history(
            source=source,
            vehicle_id=vehicle_id,
            trip_id=trip_id,
            route_id=route_id,
            direction_id=direction_id,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Trace query failed: {e}")


# -----------------------------
# Static GTFS endpoints (for map UI)
# -----------------------------
@app.get("/api/static/routes")
def api_static_routes(limit: int = 5000):
    if not static_gtfs_loaded():
        return []
    routes = _static_gtfs.get("routes")
    if routes is None or routes.empty:
        return []
    cols = [c for c in ["route_id", "route_short_name", "route_long_name", "route_type"] if c in routes.columns]
    df = routes[cols].copy() if cols else routes.copy()
    df = df.drop_duplicates(subset=["route_id"]) if "route_id" in df.columns else df
    df = df.head(int(limit)) if limit else df
    return df_to_records(df, limit=int(limit) if limit else 5000)


@app.get("/api/static/stops")
def api_static_stops(limit: int = 10000, q: Optional[str] = None):
    """Return static GTFS stops with coordinates.

    Riga feed is ~1.6k stops, so returning all is OK.
    """
    if not static_gtfs_loaded():
        return []
    stops = _static_gtfs.get("stops")
    if stops is None or stops.empty:
        return []
    df = stops.copy()
    if q:
        qq = str(q).strip().lower()
        if qq:
            # search in stop_id OR stop_name
            cols = []
            if "stop_id" in df.columns:
                cols.append(df["stop_id"].astype(str).str.lower().str.contains(qq, na=False))
            if "stop_name" in df.columns:
                cols.append(df["stop_name"].astype(str).str.lower().str.contains(qq, na=False))
            if cols:
                m = cols[0]
                for c in cols[1:]:
                    m = m | c
                df = df[m].copy()
    cols = [c for c in ["stop_id", "stop_name", "stop_lat", "stop_lon"] if c in stops.columns]
    df = df[cols].copy() if cols else df.copy()
    df = df[pd.notnull(df.get("stop_lat")) & pd.notnull(df.get("stop_lon"))].copy()
    df = df.head(int(limit)) if limit else df
    return df_to_records(df, limit=int(limit) if limit else 10000)

@app.post("/api/static/reload")
def api_static_reload():
    if not STATIC_GTFS_URL:
        raise HTTPException(status_code=400, detail="STATIC_GTFS_URL not set")
    if not STATIC_GTFS_PATH:
        raise HTTPException(status_code=400, detail="STATIC_GTFS_PATH not set")
    try:
        return download_static_gtfs_zip(STATIC_GTFS_URL, STATIC_GTFS_PATH)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------
# Map endpoints
# -----------------------------
@app.get("/api/map/overview")
def api_map_overview(
    source: str = "mobis",
    route_id: Optional[str] = None,
    trip_id: Optional[str] = None,
    vehicle_id: Optional[str] = None,
    stop_id: Optional[str] = None,
    direction_id: Optional[int] = None,
    q: Optional[str] = None,
    minutes: int = 90,
    limit_vehicles: int = 2000,
    limit_anomalies: int = 500,
):
    """Compact dataset for a live map with server-side filters before limits."""
    def _norm(s: Any) -> str:
        return str(s or "").strip().lower()

    def _norm_id(s: Any) -> str:
        t = str(s or "").strip().lower()
        if not t:
            return ""
        if t.isdigit():
            z = t.lstrip("0")
            return z or "0"
        return t

    def _id_match(value: Any, flt: Optional[str]) -> bool:
        f = str(flt or "").strip()
        if not f:
            return True
        fv = _norm_id(f)
        vv = _norm_id(value)
        if not vv:
            return False
        return vv == fv or fv in vv or vv in fv

    def _text_match(parts: List[Any], flt: Optional[str]) -> bool:
        needle = _norm(flt)
        if not needle:
            return True
        hay = " ".join(str(x or "") for x in parts).lower()
        return needle in hay

    tz = ZoneInfo(TZ_NAME)
    now = datetime.now(tz)
    st = _get_state(source)

    vehicles: List[Dict[str, Any]] = []
    veh_df = st.get("veh_df")
    if veh_df is not None and not veh_df.empty:
        d = veh_df.copy()
        if "latitude" in d.columns and "longitude" in d.columns:
            d = d[pd.notnull(d["latitude"]) & pd.notnull(d["longitude"])].copy()
        for _, r in d.iterrows():
            if route_id and not (_id_match(r.get("route_id"), route_id) or _id_match(r.get("route_short_name"), route_id)):
                continue
            if trip_id and not _id_match(r.get("trip_id"), trip_id):
                continue
            if vehicle_id and not (_id_match(r.get("vehicle_id"), vehicle_id) or _id_match(r.get("vehicle_label"), vehicle_id)):
                continue
            if stop_id and not _id_match(r.get("stop_id"), stop_id):
                continue
            if direction_id is not None:
                try:
                    if pd.isna(r.get("direction_id")) or int(float(r.get("direction_id"))) != int(direction_id):
                        continue
                except Exception:
                    continue
            if not _text_match([
                r.get("vehicle_id"), r.get("vehicle_label"), r.get("trip_id"), r.get("route_id"),
                r.get("route_short_name"), r.get("trip_headsign"), r.get("stop_id"), r.get("stop_name")
            ], q):
                continue
            vehicles.append({
                "vehicle_id": r.get("vehicle_id"),
                "vehicle_label": r.get("vehicle_label"),
                "trip_id": r.get("trip_id"),
                "route_id": r.get("route_id"),
                "route_short_name": r.get("route_short_name"),
                "trip_headsign": r.get("trip_headsign"),
                "direction_id": json_safe(r.get("direction_id")),
                "lat": json_safe(r.get("latitude")),
                "lon": json_safe(r.get("longitude")),
                "bearing": json_safe(r.get("bearing")),
                "speed_mps": json_safe(r.get("speed_mps")),
                "timestamp_local": r.get("timestamp_local"),
                "stop_id": r.get("stop_id"),
                "stop_name": r.get("stop_name"),
            })
            if len(vehicles) >= int(limit_vehicles):
                break

    veh_by_trip: Dict[str, Dict[str, Any]] = {}
    veh_by_vehicle: Dict[str, Dict[str, Any]] = {}
    for v in vehicles:
        if v.get("trip_id"):
            veh_by_trip[str(v["trip_id"])] = v
        if v.get("vehicle_id"):
            veh_by_vehicle[str(v["vehicle_id"])] = v

    cutoff = now - timedelta(minutes=int(minutes))
    anomalies_src = list(st.get("anomalies", []))
    anomalies_out: List[Dict[str, Any]] = []
    for a in reversed(anomalies_src):
        if len(anomalies_out) >= int(limit_anomalies):
            break
        dt = parse_iso(a.get("detection_time_local"))
        if isinstance(dt, datetime) and dt < cutoff:
            break
        if route_id and not (_id_match(a.get("route_id"), route_id) or _id_match(a.get("route_short_name"), route_id)):
            continue
        if trip_id and not _id_match(a.get("trip_id"), trip_id):
            continue
        if vehicle_id and not _id_match(a.get("vehicle_id"), vehicle_id):
            continue
        if stop_id and not _id_match(a.get("stop_id"), stop_id):
            continue
        if direction_id is not None:
            try:
                av = a.get("direction_id")
                if av is None or int(float(av)) != int(direction_id):
                    continue
            except Exception:
                continue
        if not _text_match([
            a.get("kind"), a.get("trip_id"), a.get("route_id"), a.get("route_short_name"),
            a.get("stop_id"), a.get("stop_name"), a.get("vehicle_id"), a.get("note")
        ], q):
            continue
        stop_id_val = a.get("stop_id")
        stop_meta = _static_lookup_stop(stop_id_val) if stop_id_val else {"stop_name": None, "stop_lat": None, "stop_lon": None}
        curr_vehicle = None
        if a.get("vehicle_id"):
            curr_vehicle = veh_by_vehicle.get(str(a.get("vehicle_id")))
        if curr_vehicle is None and a.get("trip_id"):
            curr_vehicle = veh_by_trip.get(str(a.get("trip_id")))
        vehicle_lat = curr_vehicle.get("lat") if curr_vehicle else None
        vehicle_lon = curr_vehicle.get("lon") if curr_vehicle else None
        stop_lat = stop_meta.get("stop_lat")
        stop_lon = stop_meta.get("stop_lon")
        link = None
        if vehicle_lat is not None and vehicle_lon is not None and stop_lat is not None and stop_lon is not None:
            link = [[vehicle_lat, vehicle_lon], [stop_lat, stop_lon]]
        anomalies_out.append({
            **{k: json_safe(v) for k, v in a.items()},
            "stop_name": a.get("stop_name") or stop_meta.get("stop_name"),
            "stop_lat": json_safe(stop_lat),
            "stop_lon": json_safe(stop_lon),
            "vehicle_id": a.get("vehicle_id") or (curr_vehicle.get("vehicle_id") if curr_vehicle else None),
            "lat": json_safe(vehicle_lat),
            "lon": json_safe(vehicle_lon),
            "link": sanitize_obj(link),
        })

    payload = {
        "source": source,
        "feed_header_timestamp_local": st.get("header_ts_local"),
        "generated_at_local": now.isoformat(),
        "static_gtfs_loaded": bool(_static_gtfs.get("loaded")),
        "vehicles": vehicles,
        "anomalies": anomalies_out,
    }
    return sanitize_obj(payload)


@app.get("/api/map/trip")
def api_map_trip(source: str = "mobis", trip_id: str = ""):
    """Return a focused dataset to render *one* trip on a map.

    Needs STATIC_GTFS_PATH (for stop coordinates and shapes). If shapes are absent,
    the polyline falls back to the stop sequence.
    """
    if not trip_id:
        raise HTTPException(status_code=400, detail="trip_id is required")

    st = _get_state(source)
    tz = ZoneInfo(TZ_NAME)
    now = datetime.now(tz)

    trip_meta = _static_lookup_trip(trip_id)
    route_meta = _static_lookup_route(trip_meta.get("route_id")) if trip_meta.get("route_id") else {"route_short_name": None, "route_long_name": None}

    # Current RT stop_time_updates for this trip
    rt_df = st.get("stops_df")
    rt_present: Set[int] = set()
    rt_rows = []
    if rt_df is not None and not rt_df.empty and "trip_id" in rt_df.columns:
        d = rt_df[rt_df["trip_id"].astype(str) == str(trip_id)].copy()
        if not d.empty:
            if "stop_sequence" in d.columns:
                d["stop_sequence"] = pd.to_numeric(d["stop_sequence"], errors="coerce")
                for x in d["stop_sequence"].dropna().astype(int).tolist():
                    rt_present.add(int(x))
            # Keep a small list of RT rows for popups
            cols = [c for c in ["stop_sequence", "stop_id", "stop_name", "arrival_time_local", "departure_time_local", "arrival_delay_sec", "departure_delay_sec"] if c in d.columns]
            rt_rows = df_to_records(d[cols] if cols else d, limit=5000)

    # Trip static stop sequence
    stops_out: List[Dict[str, Any]] = []
    polyline: List[List[float]] = []

    if static_gtfs_loaded():
        stop_times = _static_gtfs.get("stop_times")
        stops = _static_gtfs.get("stops")
        if isinstance(stop_times, pd.DataFrame) and not stop_times.empty and "trip_id" in stop_times.columns:
            d = stop_times[stop_times["trip_id"].astype(str) == str(trip_id)].copy()
            if not d.empty:
                if "stop_sequence" in d.columns:
                    d["stop_sequence"] = pd.to_numeric(d["stop_sequence"], errors="coerce")
                d = d.sort_values(["stop_sequence"], kind="mergesort")
                if isinstance(stops, pd.DataFrame) and not stops.empty and "stop_id" in stops.columns:
                    d["stop_id"] = d["stop_id"].astype(str)
                    d = d.merge(stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]], on="stop_id", how="left")

                # Mark anomalies for this trip (latest N)
                anomalies = [a for a in st.get("anomalies", []) if str(a.get("trip_id") or "") == str(trip_id)]
                anomaly_keys = set()
                for a in anomalies:
                    try:
                        ss = int(float(a.get("stop_sequence"))) if a.get("stop_sequence") not in (None, "") else None
                    except Exception:
                        ss = None
                    if a.get("stop_id") and ss is not None:
                        anomaly_keys.add((str(a.get("stop_id")), int(ss)))

                for _, r in d.iterrows():
                    ss = r.get("stop_sequence")
                    ss_int = int(ss) if pd.notnull(ss) else None
                    stop_id = r.get("stop_id")
                    stop_lat = r.get("stop_lat")
                    stop_lon = r.get("stop_lon")
                    has_coords = pd.notnull(stop_lat) and pd.notnull(stop_lon)

                    flags = {
                        "rt_present": bool(ss_int is not None and ss_int in rt_present),
                        "has_anomaly": bool(stop_id is not None and ss_int is not None and (str(stop_id), int(ss_int)) in anomaly_keys),
                    }
                    stops_out.append(
                        {
                            "stop_sequence": ss_int,
                            "stop_id": str(stop_id) if stop_id is not None else None,
                            "stop_name": r.get("stop_name"),
                            "stop_lat": json_safe(stop_lat) if has_coords else None,
                            "stop_lon": json_safe(stop_lon) if has_coords else None,
                            **flags,
                        }
                    )

                # Fallback polyline from stop sequence
                for s in stops_out:
                    if s.get("stop_lat") is not None and s.get("stop_lon") is not None:
                        polyline.append([float(s["stop_lat"]), float(s["stop_lon"])])

        # Better polyline from shapes.txt if shape_id exists
        shape_id = trip_meta.get("shape_id")
        shapes = _static_gtfs.get("shapes")
        if shape_id and isinstance(shapes, pd.DataFrame) and not shapes.empty and "shape_id" in shapes.columns:
            sh = shapes[shapes["shape_id"].astype(str) == str(shape_id)].copy()
            if not sh.empty:
                if "shape_pt_sequence" in sh.columns:
                    sh["shape_pt_sequence"] = pd.to_numeric(sh["shape_pt_sequence"], errors="coerce")
                sh = sh.sort_values(["shape_pt_sequence"], kind="mergesort")
                pts = []
                for _, r in sh.iterrows():
                    lat = r.get("shape_pt_lat")
                    lon = r.get("shape_pt_lon")
                    if pd.notnull(lat) and pd.notnull(lon):
                        pts.append([float(lat), float(lon)])
                if len(pts) >= 2:
                    polyline = pts

    # Recent anomalies for this trip (for sidebar)
    anomalies_trip = [a for a in reversed(st.get("anomalies", [])) if str(a.get("trip_id") or "") == str(trip_id)]
    anomalies_trip = anomalies_trip[:200]

    # Fit bounds
    lats = [p[0] for p in polyline]
    lons = [p[1] for p in polyline]
    bounds = None
    if lats and lons:
        bounds = {
            "min_lat": float(min(lats)),
            "min_lon": float(min(lons)),
            "max_lat": float(max(lats)),
            "max_lon": float(max(lons)),
        }

    return {
        "source": source,
        "generated_at_local": now.isoformat(),
        "trip": {
            **trip_meta,
            **route_meta,
        },
        "polyline": polyline,
        "stops": stops_out,
        "rt_stop_time_updates": rt_rows,
        "anomalies": [{k: json_safe(v) for k, v in a.items()} for a in anomalies_trip],
        "bounds": bounds,
    }


@app.get("/api/metrics/routes")
def api_metrics_routes(source: str = "mobis"):
    """Live route activity summary.

    The upstream MOBIS PB samples do not provide delay fields reliably, so the old
    delay-by-route metric was usually empty or misleading. This endpoint now returns
    useful live operational metrics based on the current in-memory snapshot.
    """
    st = _get_state(source)
    veh = st.get("veh_df") if isinstance(st.get("veh_df"), pd.DataFrame) else pd.DataFrame()
    trips = st.get("trips_df") if isinstance(st.get("trips_df"), pd.DataFrame) else pd.DataFrame()
    stops = st.get("stops_df") if isinstance(st.get("stops_df"), pd.DataFrame) else pd.DataFrame()

    frames = []
    if not veh.empty:
        v = veh[[c for c in ["route_id", "route_short_name", "route_long_name", "vehicle_id", "trip_id"] if c in veh.columns]].copy()
        v["vehicle_rows"] = 1
        frames.append(v)
    if not trips.empty:
        t = trips[[c for c in ["route_id", "route_short_name", "route_long_name", "trip_id", "vehicle_id"] if c in trips.columns]].copy()
        t["trip_rows"] = 1
        frames.append(t)
    if not stops.empty:
        s = stops[[c for c in ["route_id", "route_short_name", "route_long_name", "trip_id", "stop_id", "vehicle_id"] if c in stops.columns]].copy()
        s["stop_rows"] = 1
        frames.append(s)
    if not frames:
        return []

    merged = pd.concat(frames, ignore_index=True, sort=False)
    if "route_id" not in merged.columns:
        return []
    merged = merged[pd.notnull(merged["route_id"])].copy()
    if merged.empty:
        return []

    agg_map: Dict[str, Any] = {}
    if "vehicle_id" in merged.columns:
        agg_map["vehicle_id"] = pd.Series.nunique
    if "trip_id" in merged.columns:
        agg_map["trip_id"] = pd.Series.nunique
    if "stop_id" in merged.columns:
        agg_map["stop_id"] = pd.Series.nunique
    for col in ["vehicle_rows", "trip_rows", "stop_rows"]:
        if col in merged.columns:
            agg_map[col] = "sum"
    grouped = merged.groupby([c for c in ["route_id", "route_short_name", "route_long_name"] if c in merged.columns], dropna=False).agg(agg_map).reset_index()

    rename_map = {
        "vehicle_id": "active_vehicle_count",
        "trip_id": "active_trip_count",
        "stop_id": "active_stop_count",
    }
    grouped.rename(columns=rename_map, inplace=True)
    for col in ["vehicle_rows", "trip_rows", "stop_rows", "active_vehicle_count", "active_trip_count", "active_stop_count"]:
        if col not in grouped.columns:
            grouped[col] = 0
    grouped["activity_score"] = grouped[[c for c in ["vehicle_rows", "trip_rows", "stop_rows"] if c in grouped.columns]].fillna(0).sum(axis=1)
    grouped = grouped.sort_values(["activity_score", "active_vehicle_count", "active_trip_count"], ascending=[False, False, False])
    return df_to_records(grouped, limit=2000)


@app.get("/api/history")
def api_history(
    source: str = "mobis",
    dataset: str = "all",
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    stop_id: Optional[str] = None,
    route_id: Optional[str] = None,
    trip_id: Optional[str] = None,
    vehicle_id: Optional[str] = None,
    limit: int = 2000,
):
    try:
        return fetch_history(
            source=source,
            dataset=dataset,
            start_time=start_time,
            end_time=end_time,
            stop_id=stop_id,
            route_id=route_id,
            trip_id=trip_id,
            vehicle_id=vehicle_id,
            limit=limit,
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"History query failed: {e}")


@app.get("/api/history/stops")
def api_history_stops(
    source: str = "mobis",
    stop_id: str = "",
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 2000,
):
    try:
        return fetch_stop_history(
            source=source,
            stop_id=stop_id,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Stop history query failed: {e}")


@app.get("/api/history/vehicles")
def api_history_vehicles(
    source: str = "mobis",
    vehicle_id: str = "",
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 2000,
):
    try:
        return fetch_vehicle_history(
            source=source,
            vehicle_id=vehicle_id,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Vehicle history query failed: {e}")


@app.get("/api/analytics/overview")
def api_analytics_overview(source: str = "mobis", hours: int = 24, top_n: int = 20):
    try:
        return analytics_overview(source=source, hours=hours, top_n=top_n)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Analytics query failed: {e}")


def _safe_sheet_name(name: str) -> str:
    # Excel sheet name rules: max 31 chars, no [: \ / ? * ]
    bad = [":", "\\", "/", "?", "*", "[", "]"]
    for b in bad:
        name = name.replace(b, "-")
    return name[:31]


def workbook_bytes_for_sources(sources_list: List[str]) -> bytes:
    tz = ZoneInfo(TZ_NAME)
    now = datetime.now(tz)

    # Metadata sheet (per source)
    meta_rows = []
    for s in sources_list:
        st = _get_state(s)
        meta_rows.append({
            "source": s,
            "gtfs_rt_url": SOURCES.get(s),
            "last_refresh_utc": st.get("last_refresh_utc"),
            "feed_header_timestamp_local": st.get("header_ts_local"),
            "static_gtfs_loaded": bool(_static_gtfs.get("loaded")),
            "static_gtfs_path": _static_gtfs.get("path"),
        })

    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as xw:
        pd.DataFrame(meta_rows).to_excel(xw, sheet_name=_safe_sheet_name("metadata"), index=False)

        multi = len(sources_list) > 1
        for s in sources_list:
            st = _get_state(s)
            prefix = f"{s}_" if multi else ""

            def w(sheet: str, df: Optional[pd.DataFrame], expected_name: Optional[str] = None):
                if df is None or df.empty:
                    cols = CSV_EXPECTED_COLUMNS.get(expected_name or "", [])
                    df2 = pd.DataFrame(columns=cols)
                else:
                    df2 = df
                df2.to_excel(xw, sheet_name=_safe_sheet_name(prefix + sheet), index=False)

            w("trip_updates", st.get("trips_df"), "gtfs_trip_updates.csv")
            w("stop_time_updates", st.get("stops_df"), "gtfs_stop_time_updates.csv")
            w("vehicle_positions", st.get("veh_df"), "gtfs_vehicle_positions.csv")
            w("alerts", st.get("alerts_df"), "gtfs_alerts.csv")

            # Anomalies (in-memory)
            an = st.get("anomalies", [])
            an_df = _rows_to_anomaly_df(an) if an else pd.DataFrame(columns=CSV_EXPECTED_COLUMNS["anomalies.csv"])
            an_df.to_excel(xw, sheet_name=_safe_sheet_name(prefix + "anomalies"), index=False)

    bio.seek(0)
    return bio.getvalue()


@app.get("/api/download/workbook.xlsx")
def api_download_workbook(source: str = "mobis"):
    """Download current decoded tables as a single Excel workbook.

    - source=mobis (default)
    - source=all (includes all configured sources)
    - source=mobis,saraksti (comma-separated)
    """
    if source.strip().lower() == "all":
        sources_list = list(SOURCES.keys())
    else:
        sources_list = [s.strip() for s in source.split(",") if s.strip()]
    for s in sources_list:
        if s not in SOURCES:
            raise HTTPException(status_code=400, detail=f"Unknown source: {s}. Use /api/sources")

    content = workbook_bytes_for_sources(sources_list)
    tz = ZoneInfo(TZ_NAME)
    ts = datetime.now(tz).strftime("%Y%m%d_%H%M%S")
    name = "all" if len(sources_list) > 1 else sources_list[0]
    filename = f"gtfs_rt_{name}_{ts}.xlsx"
    return StreamingResponse(
        io.BytesIO(content),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=\"{filename}\""},
    )


@app.get("/api/download/{name}")
def api_download(name: str, source: str = "mobis"):
    allowed = set(CSV_EXPECTED_COLUMNS.keys())
    if name not in allowed:
        raise HTTPException(status_code=404, detail="Unknown file")
    path = (DATA_DIR / source) / name
    if path.exists():
        return FileResponse(str(path), media_type="text/csv", filename=name)

    # Fallback: return an empty CSV (headers only) instead of a 404.
    df = pd.DataFrame(columns=CSV_EXPECTED_COLUMNS.get(name, []))
    content = df.to_csv(index=False).encode("utf-8")
    return Response(
        content=content,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=\"{name}\""},
    )
