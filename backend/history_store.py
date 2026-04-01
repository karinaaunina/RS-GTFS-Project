import json
import math
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
SQL_INIT_PATH = BASE_DIR / "sql" / "01_init_history_schema.sql"

DB_ENABLED = os.getenv("DB_ENABLED", "true").lower() == "true"
DB_STORE_RAW_PB = os.getenv("DB_STORE_RAW_PB", "true").lower() == "true"
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "gtfs_monitor")
DB_USER = os.getenv("DB_USER", "gtfs_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "gtfs_pass")
DB_SSLMODE = os.getenv("DB_SSLMODE", "prefer")
DB_CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", "5"))
DB_QUERY_LIMIT_MAX = int(os.getenv("DB_QUERY_LIMIT_MAX", "20000"))
REFRESH_SECONDS = max(1, int(os.getenv("REFRESH_SECONDS", "5")))
DB_CLEANUP_ENABLED = os.getenv("DB_CLEANUP_ENABLED", "true").lower() == "true"
DB_RETENTION_DAYS = int(os.getenv("DB_RETENTION_DAYS", "3"))
DB_CLEANUP_INTERVAL_MINUTES = int(os.getenv("DB_CLEANUP_INTERVAL_MINUTES", "1440"))


def _psycopg_modules():
    try:
        import psycopg2  # type: ignore
        from psycopg2.extras import Json, RealDictCursor, execute_values  # type: ignore
        return psycopg2, Json, RealDictCursor, execute_values
    except Exception as exc:  # pragma: no cover - runtime guard
        raise RuntimeError(f"psycopg2 is not installed: {exc}")

_init_lock = threading.Lock()
_db_initialized = False
_last_error: Optional[str] = None

TRIP_COLUMNS = [
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
]

STOP_COLUMNS = [
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
]

VEHICLE_COLUMNS = [
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
]

ALERT_COLUMNS = [
    "entity_id",
    "header",
    "description",
    "cause",
    "effect",
    "start_local",
    "end_local",
    "feed_header_timestamp_local",
]


@dataclass
class SnapshotStoreResult:
    snapshot_id: Optional[int]
    inserted: Dict[str, int]
    ok: bool
    error: Optional[str] = None


def _db_config() -> Dict[str, Any]:
    return {
        "host": DB_HOST,
        "port": DB_PORT,
        "dbname": DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "sslmode": DB_SSLMODE,
        "connect_timeout": DB_CONNECT_TIMEOUT,
    }



def _set_error(msg: Optional[str]) -> None:
    global _last_error
    _last_error = msg



def _connect():
    psycopg2, _, _, _ = _psycopg_modules()
    return psycopg2.connect(**_db_config())



def _ensure_initialized() -> None:
    global _db_initialized
    if not DB_ENABLED or _db_initialized:
        return
    with _init_lock:
        if _db_initialized or not DB_ENABLED:
            return
        if not SQL_INIT_PATH.exists():
            raise FileNotFoundError(f"DB init SQL not found: {SQL_INIT_PATH}")
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(SQL_INIT_PATH.read_text(encoding="utf-8"))
        _db_initialized = True
        _set_error(None)



def init_database() -> Dict[str, Any]:
    if not DB_ENABLED:
        return {"enabled": False, "connected": False, "initialized": False, "error": None}
    try:
        _ensure_initialized()
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"enabled": True, "connected": True, "initialized": True, "error": None}
    except Exception as exc:  # pragma: no cover - best effort runtime guard
        _set_error(str(exc))
        return {"enabled": True, "connected": False, "initialized": False, "error": str(exc)}



def db_status() -> Dict[str, Any]:
    if not DB_ENABLED:
        return {"enabled": False, "connected": False, "initialized": False, "error": None}
    try:
        _ensure_initialized()
        with _connect() as conn:
            with conn.cursor(cursor_factory=_psycopg_modules()[2]) as cur:
                cur.execute(
                    """
                    SELECT
                        (SELECT MAX(received_at) FROM feed_snapshots) AS last_received_at,
                        (SELECT MAX(feed_header_timestamp) FROM feed_snapshots) AS last_feed_header_timestamp,
                        (SELECT reltuples::bigint FROM pg_class WHERE relname = 'feed_snapshots') AS approx_snapshot_count,
                        (SELECT reltuples::bigint FROM pg_class WHERE relname = 'trip_updates_history') AS approx_trip_updates_count,
                        (SELECT reltuples::bigint FROM pg_class WHERE relname = 'stop_time_updates_history') AS approx_stop_time_updates_count,
                        (SELECT reltuples::bigint FROM pg_class WHERE relname = 'vehicle_positions_history') AS approx_vehicle_positions_count,
                        (SELECT reltuples::bigint FROM pg_class WHERE relname = 'alerts_history') AS approx_alerts_count
                    """
                )
                row = cur.fetchone() or {}
        _set_error(None)
        return {
            "enabled": True,
            "connected": True,
            "initialized": True,
            "error": None,
            "counts": {
                "snapshots": int(row.get("approx_snapshot_count") or 0),
                "trip_updates": int(row.get("approx_trip_updates_count") or 0),
                "stop_time_updates": int(row.get("approx_stop_time_updates_count") or 0),
                "vehicle_positions": int(row.get("approx_vehicle_positions_count") or 0),
                "alerts": int(row.get("approx_alerts_count") or 0),
            },
            "last_received_at": _to_iso(row.get("last_received_at")),
            "last_feed_header_timestamp": _to_iso(row.get("last_feed_header_timestamp")),
        }
    except Exception as exc:  # pragma: no cover - best effort runtime guard
        _set_error(str(exc))
        return {"enabled": True, "connected": False, "initialized": _db_initialized, "error": str(exc)}



def _to_iso(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.isoformat()
    return str(v)



def _is_missing(v: Any) -> bool:
    if v is None:
        return True
    try:
        if isinstance(v, float) and math.isnan(v):
            return True
    except Exception:
        pass
    try:
        return bool(pd.isna(v))
    except Exception:
        return False



def _clean_value(v: Any) -> Any:
    if _is_missing(v):
        return None
    if hasattr(v, "item"):
        try:
            v = v.item()
        except Exception:
            pass
    if isinstance(v, str):
        s = v.strip()
        return s if s != "" else None
    return v



def _parse_ts(v: Any) -> Optional[datetime]:
    v = _clean_value(v)
    if v is None:
        return None
    if isinstance(v, datetime):
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v
    s = str(v).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        try:
            dt = pd.to_datetime(s, utc=True).to_pydatetime()
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt



def _safe_int(v: Any) -> Optional[int]:
    v = _clean_value(v)
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None



def _safe_float(v: Any) -> Optional[float]:
    v = _clean_value(v)
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None



def _snapshot_insert(cur, *, source: str, received_at: datetime, feed_header_timestamp: Optional[datetime], pb_bytes: bytes, trips_df: pd.DataFrame, stops_df: pd.DataFrame, veh_df: pd.DataFrame, alerts_df: pd.DataFrame, dq_summary: Optional[Dict[str, Any]]) -> int:
    dqi = dq_summary.get("dqi") if isinstance(dq_summary, dict) else None
    penalty = dq_summary.get("penalty") if isinstance(dq_summary, dict) else None
    issues = dq_summary.get("issues") if isinstance(dq_summary, dict) else None
    cur.execute(
        """
        INSERT INTO feed_snapshots (
            source,
            received_at,
            feed_header_timestamp,
            raw_pb,
            raw_pb_size,
            trip_updates_count,
            stop_time_updates_count,
            vehicle_positions_count,
            alerts_count,
            dqi,
            penalty,
            issues
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            source,
            received_at,
            feed_header_timestamp,
            _psycopg_modules()[0].Binary(pb_bytes) if DB_STORE_RAW_PB else None,
            len(pb_bytes) if pb_bytes is not None else None,
            int(len(trips_df.index)) if trips_df is not None else 0,
            int(len(stops_df.index)) if stops_df is not None else 0,
            int(len(veh_df.index)) if veh_df is not None else 0,
            int(len(alerts_df.index)) if alerts_df is not None else 0,
            dqi,
            penalty,
            _psycopg_modules()[1](issues) if issues is not None else None,
        ),
    )
    return int(cur.fetchone()[0])



def _bulk_insert(cur, table_name: str, columns: Sequence[str], rows: Sequence[Tuple[Any, ...]]) -> int:
    if not rows:
        return 0
    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
    _psycopg_modules()[3](cur, insert_sql, rows, page_size=1000)
    return len(rows)



def _rows_for_trip_updates(df: pd.DataFrame, *, snapshot_id: int, source: str, recorded_at: datetime) -> List[Tuple[Any, ...]]:
    if df is None or df.empty:
        return []
    rows: List[Tuple[Any, ...]] = []
    for record in df.to_dict(orient="records"):
        rows.append(
            (
                snapshot_id,
                source,
                recorded_at,
                _clean_value(record.get("entity_id")),
                _clean_value(record.get("trip_id")),
                _clean_value(record.get("route_id")),
                _clean_value(record.get("route_short_name")),
                _clean_value(record.get("route_long_name")),
                _clean_value(record.get("start_date")),
                _clean_value(record.get("start_time")),
                _safe_int(record.get("direction_id")),
                _clean_value(record.get("trip_headsign")),
                _clean_value(record.get("shape_id")),
                _safe_int(record.get("schedule_relationship")),
                _clean_value(record.get("vehicle_id")),
                _clean_value(record.get("vehicle_label")),
                _parse_ts(record.get("trip_update_timestamp_local")),
                _parse_ts(record.get("feed_header_timestamp_local")),
            )
        )
    return rows



def _rows_for_stop_updates(df: pd.DataFrame, *, snapshot_id: int, source: str, recorded_at: datetime) -> List[Tuple[Any, ...]]:
    if df is None or df.empty:
        return []
    rows: List[Tuple[Any, ...]] = []
    for record in df.to_dict(orient="records"):
        rows.append(
            (
                snapshot_id,
                source,
                recorded_at,
                _clean_value(record.get("entity_id")),
                _clean_value(record.get("trip_id")),
                _clean_value(record.get("vehicle_id")),
                _clean_value(record.get("vehicle_label")),
                _clean_value(record.get("route_id")),
                _clean_value(record.get("route_short_name")),
                _clean_value(record.get("route_long_name")),
                _safe_int(record.get("direction_id")),
                _clean_value(record.get("trip_headsign")),
                _safe_int(record.get("stop_sequence")),
                _clean_value(record.get("stop_id")),
                _clean_value(record.get("stop_name")),
                _safe_float(record.get("stop_lat")),
                _safe_float(record.get("stop_lon")),
                _parse_ts(record.get("arrival_time_local")),
                _safe_int(record.get("arrival_delay_sec")),
                _parse_ts(record.get("departure_time_local")),
                _safe_int(record.get("departure_delay_sec")),
                _clean_value(record.get("scheduled_arrival_time")),
                _clean_value(record.get("scheduled_departure_time")),
                _safe_int(record.get("schedule_relationship")),
                _parse_ts(record.get("feed_header_timestamp_local")),
            )
        )
    return rows



def _rows_for_vehicle_positions(df: pd.DataFrame, *, snapshot_id: int, source: str, recorded_at: datetime) -> List[Tuple[Any, ...]]:
    if df is None or df.empty:
        return []
    rows: List[Tuple[Any, ...]] = []
    for record in df.to_dict(orient="records"):
        rows.append(
            (
                snapshot_id,
                source,
                recorded_at,
                _clean_value(record.get("entity_id")),
                _clean_value(record.get("vehicle_id")),
                _clean_value(record.get("vehicle_label")),
                _clean_value(record.get("trip_id")),
                _clean_value(record.get("route_id")),
                _clean_value(record.get("route_short_name")),
                _clean_value(record.get("route_long_name")),
                _safe_int(record.get("direction_id")),
                _clean_value(record.get("trip_headsign")),
                _safe_float(record.get("latitude")),
                _safe_float(record.get("longitude")),
                _safe_float(record.get("bearing")),
                _safe_float(record.get("speed_mps")),
                _safe_int(record.get("current_status")),
                _clean_value(record.get("stop_id")),
                _clean_value(record.get("stop_name")),
                _parse_ts(record.get("timestamp_local")),
                _parse_ts(record.get("feed_header_timestamp_local")),
            )
        )
    return rows



def _rows_for_alerts(df: pd.DataFrame, *, snapshot_id: int, source: str, recorded_at: datetime) -> List[Tuple[Any, ...]]:
    if df is None or df.empty:
        return []
    rows: List[Tuple[Any, ...]] = []
    for record in df.to_dict(orient="records"):
        rows.append(
            (
                snapshot_id,
                source,
                recorded_at,
                _clean_value(record.get("entity_id")),
                _clean_value(record.get("header")),
                _clean_value(record.get("description")),
                _safe_int(record.get("cause")),
                _safe_int(record.get("effect")),
                _parse_ts(record.get("start_local")),
                _parse_ts(record.get("end_local")),
                _parse_ts(record.get("feed_header_timestamp_local")),
            )
        )
    return rows





def cleanup_old_history(retention_days: Optional[int] = None) -> Dict[str, Any]:
    if not DB_ENABLED:
        return {"ok": False, "error": "db disabled"}

    _ensure_initialized()
    days = max(1, int(retention_days or DB_RETENTION_DAYS))

    deleted = {
        "trip_updates_history": 0,
        "stop_time_updates_history": 0,
        "vehicle_positions_history": 0,
        "alerts_history": 0,
        "feed_snapshots": 0,
    }

    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM trip_updates_history WHERE recorded_at < NOW() - (%s * INTERVAL '1 day')",
                    (days,),
                )
                deleted["trip_updates_history"] = cur.rowcount

                cur.execute(
                    "DELETE FROM stop_time_updates_history WHERE recorded_at < NOW() - (%s * INTERVAL '1 day')",
                    (days,),
                )
                deleted["stop_time_updates_history"] = cur.rowcount

                cur.execute(
                    "DELETE FROM vehicle_positions_history WHERE recorded_at < NOW() - (%s * INTERVAL '1 day')",
                    (days,),
                )
                deleted["vehicle_positions_history"] = cur.rowcount

                cur.execute(
                    "DELETE FROM alerts_history WHERE recorded_at < NOW() - (%s * INTERVAL '1 day')",
                    (days,),
                )
                deleted["alerts_history"] = cur.rowcount

                cur.execute(
                    "DELETE FROM feed_snapshots WHERE received_at < NOW() - (%s * INTERVAL '1 day')",
                    (days,),
                )
                deleted["feed_snapshots"] = cur.rowcount
            conn.commit()
        return {"ok": True, "retention_days": days, "deleted": deleted}
    except Exception as exc:
        _set_error(str(exc))
        return {"ok": False, "error": str(exc), "deleted": deleted}

def store_snapshot(
    *,
    source: str,
    pb_bytes: bytes,
    header_ts_local: Optional[str],
    trips_df: pd.DataFrame,
    stops_df: pd.DataFrame,
    veh_df: pd.DataFrame,
    alerts_df: pd.DataFrame,
    dq_summary: Optional[Dict[str, Any]] = None,
) -> SnapshotStoreResult:
    if not DB_ENABLED:
        return SnapshotStoreResult(snapshot_id=None, inserted={}, ok=False, error="DB disabled")
    try:
        _ensure_initialized()
        received_at = datetime.now(timezone.utc)
        feed_header_timestamp = _parse_ts(header_ts_local)
        with _connect() as conn:
            with conn.cursor() as cur:
                snapshot_id = _snapshot_insert(
                    cur,
                    source=source,
                    received_at=received_at,
                    feed_header_timestamp=feed_header_timestamp,
                    pb_bytes=pb_bytes,
                    trips_df=trips_df,
                    stops_df=stops_df,
                    veh_df=veh_df,
                    alerts_df=alerts_df,
                    dq_summary=dq_summary or {},
                )
                inserted = {
                    "trip_updates": _bulk_insert(
                        cur,
                        "trip_updates_history",
                        [
                            "snapshot_id", "source", "recorded_at", "entity_id", "trip_id", "route_id", "route_short_name", "route_long_name", "start_date", "start_time", "direction_id", "trip_headsign", "shape_id", "schedule_relationship", "vehicle_id", "vehicle_label", "trip_update_timestamp", "feed_header_timestamp"
                        ],
                        _rows_for_trip_updates(trips_df, snapshot_id=snapshot_id, source=source, recorded_at=received_at),
                    ),
                    "stop_time_updates": _bulk_insert(
                        cur,
                        "stop_time_updates_history",
                        [
                            "snapshot_id", "source", "recorded_at", "entity_id", "trip_id", "vehicle_id", "vehicle_label", "route_id", "route_short_name", "route_long_name", "direction_id", "trip_headsign", "stop_sequence", "stop_id", "stop_name", "stop_lat", "stop_lon", "arrival_time", "arrival_delay_sec", "departure_time", "departure_delay_sec", "scheduled_arrival_time", "scheduled_departure_time", "schedule_relationship", "feed_header_timestamp"
                        ],
                        _rows_for_stop_updates(stops_df, snapshot_id=snapshot_id, source=source, recorded_at=received_at),
                    ),
                    "vehicle_positions": _bulk_insert(
                        cur,
                        "vehicle_positions_history",
                        [
                            "snapshot_id", "source", "recorded_at", "entity_id", "vehicle_id", "vehicle_label", "trip_id", "route_id", "route_short_name", "route_long_name", "direction_id", "trip_headsign", "latitude", "longitude", "bearing", "speed_mps", "current_status", "stop_id", "stop_name", "vehicle_timestamp", "feed_header_timestamp"
                        ],
                        _rows_for_vehicle_positions(veh_df, snapshot_id=snapshot_id, source=source, recorded_at=received_at),
                    ),
                    "alerts": _bulk_insert(
                        cur,
                        "alerts_history",
                        [
                            "snapshot_id", "source", "recorded_at", "entity_id", "header", "description", "cause", "effect", "start_time", "end_time", "feed_header_timestamp"
                        ],
                        _rows_for_alerts(alerts_df, snapshot_id=snapshot_id, source=source, recorded_at=received_at),
                    ),
                }
        _set_error(None)
        return SnapshotStoreResult(snapshot_id=snapshot_id, inserted=inserted, ok=True)
    except Exception as exc:  # pragma: no cover - runtime guard
        _set_error(str(exc))
        return SnapshotStoreResult(snapshot_id=None, inserted={}, ok=False, error=str(exc))



def _parse_range_ts(v: Optional[str]) -> Optional[datetime]:
    if not v:
        return None
    return _parse_ts(v)



def _history_query_sql(dataset: str) -> str:
    if dataset == "trip_updates":
        return """
            SELECT
                'trip_updates' AS dataset,
                source,
                recorded_at,
                feed_header_timestamp,
                snapshot_id,
                entity_id,
                route_id,
                route_short_name,
                route_long_name,
                trip_id,
                vehicle_id,
                NULL::text AS stop_id,
                NULL::text AS stop_name,
                NULL::integer AS stop_sequence,
                NULL::double precision AS latitude,
                NULL::double precision AS longitude,
                NULL::timestamp with time zone AS arrival_time,
                NULL::timestamp with time zone AS departure_time,
                trip_update_timestamp AS event_timestamp,
                vehicle_label,
                direction_id,
                trip_headsign,
                start_date,
                start_time,
                shape_id,
                schedule_relationship,
                NULL::integer AS arrival_delay_sec,
                NULL::integer AS departure_delay_sec,
                NULL::text AS scheduled_arrival_time,
                NULL::text AS scheduled_departure_time,
                NULL::text AS header,
                NULL::text AS description,
                NULL::integer AS cause,
                NULL::integer AS effect
            FROM trip_updates_history
        """
    if dataset == "stop_time_updates":
        return """
            SELECT
                'stop_time_updates' AS dataset,
                source,
                recorded_at,
                feed_header_timestamp,
                snapshot_id,
                entity_id,
                route_id,
                route_short_name,
                route_long_name,
                trip_id,
                vehicle_id,
                stop_id,
                stop_name,
                stop_sequence,
                stop_lat AS latitude,
                stop_lon AS longitude,
                arrival_time,
                departure_time,
                COALESCE(arrival_time, departure_time) AS event_timestamp,
                vehicle_label,
                direction_id,
                trip_headsign,
                NULL::text AS start_date,
                NULL::text AS start_time,
                NULL::text AS shape_id,
                schedule_relationship,
                arrival_delay_sec,
                departure_delay_sec,
                scheduled_arrival_time,
                scheduled_departure_time,
                NULL::text AS header,
                NULL::text AS description,
                NULL::integer AS cause,
                NULL::integer AS effect
            FROM stop_time_updates_history
        """
    if dataset == "vehicle_positions":
        return """
            SELECT
                'vehicle_positions' AS dataset,
                source,
                recorded_at,
                feed_header_timestamp,
                snapshot_id,
                entity_id,
                route_id,
                route_short_name,
                route_long_name,
                trip_id,
                vehicle_id,
                stop_id,
                stop_name,
                NULL::integer AS stop_sequence,
                latitude,
                longitude,
                NULL::timestamp with time zone AS arrival_time,
                NULL::timestamp with time zone AS departure_time,
                vehicle_timestamp AS event_timestamp,
                vehicle_label,
                direction_id,
                trip_headsign,
                NULL::text AS start_date,
                NULL::text AS start_time,
                NULL::text AS shape_id,
                current_status AS schedule_relationship,
                NULL::integer AS arrival_delay_sec,
                NULL::integer AS departure_delay_sec,
                NULL::text AS scheduled_arrival_time,
                NULL::text AS scheduled_departure_time,
                NULL::text AS header,
                NULL::text AS description,
                NULL::integer AS cause,
                NULL::integer AS effect
            FROM vehicle_positions_history
        """
    if dataset == "alerts":
        return """
            SELECT
                'alerts' AS dataset,
                source,
                recorded_at,
                feed_header_timestamp,
                snapshot_id,
                entity_id,
                NULL::text AS route_id,
                NULL::text AS route_short_name,
                NULL::text AS route_long_name,
                NULL::text AS trip_id,
                NULL::text AS vehicle_id,
                NULL::text AS stop_id,
                NULL::text AS stop_name,
                NULL::integer AS stop_sequence,
                NULL::double precision AS latitude,
                NULL::double precision AS longitude,
                start_time AS arrival_time,
                end_time AS departure_time,
                start_time AS event_timestamp,
                NULL::text AS vehicle_label,
                NULL::integer AS direction_id,
                NULL::text AS trip_headsign,
                NULL::text AS start_date,
                NULL::text AS start_time,
                NULL::text AS shape_id,
                NULL::integer AS schedule_relationship,
                NULL::integer AS arrival_delay_sec,
                NULL::integer AS departure_delay_sec,
                NULL::text AS scheduled_arrival_time,
                NULL::text AS scheduled_departure_time,
                header,
                description,
                cause,
                effect
            FROM alerts_history
        """
    # dataset == all
    return " UNION ALL ".join(
        [
            _history_query_sql("trip_updates"),
            _history_query_sql("stop_time_updates"),
            _history_query_sql("vehicle_positions"),
            _history_query_sql("alerts"),
        ]
    )



def fetch_history(
    *,
    source: str,
    dataset: str = "all",
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    stop_id: Optional[str] = None,
    route_id: Optional[str] = None,
    trip_id: Optional[str] = None,
    vehicle_id: Optional[str] = None,
    limit: int = 2000,
) -> Dict[str, Any]:
    if not DB_ENABLED:
        raise RuntimeError("PostgreSQL storage is disabled")
    _ensure_initialized()
    ds = dataset if dataset in {"all", "trip_updates", "stop_time_updates", "vehicle_positions", "alerts"} else "all"
    sql = _history_query_sql(ds)
    filters = ["source = %s"]
    params: List[Any] = [source]

    start_dt = _parse_range_ts(start_time)
    end_dt = _parse_range_ts(end_time)
    if start_dt is not None:
        filters.append("recorded_at >= %s")
        params.append(start_dt)
    if end_dt is not None:
        filters.append("recorded_at <= %s")
        params.append(end_dt)
    if stop_id:
        filters.append("stop_id = %s")
        params.append(stop_id)
    if route_id:
        filters.append("(route_id = %s OR route_short_name = %s)")
        params.extend([route_id, route_id])
    if trip_id:
        filters.append("trip_id = %s")
        params.append(trip_id)
    if vehicle_id:
        filters.append("vehicle_id = %s")
        params.append(vehicle_id)

    safe_limit = max(1, min(int(limit or 2000), DB_QUERY_LIMIT_MAX))

    wrapped_sql = f"SELECT * FROM ({sql}) q WHERE {' AND '.join(filters)} ORDER BY recorded_at DESC, dataset ASC LIMIT %s"
    params.append(safe_limit)

    summary_sql = f"SELECT dataset, COUNT(*)::bigint AS row_count, MAX(recorded_at) AS latest_recorded_at FROM ({sql}) q WHERE {' AND '.join(filters)} GROUP BY dataset ORDER BY dataset"
    summary_params = params[:-1]

    with _connect() as conn:
        with conn.cursor(cursor_factory=_psycopg_modules()[2]) as cur:
            cur.execute(wrapped_sql, params)
            rows = cur.fetchall() or []
            cur.execute(summary_sql, summary_params)
            summary_rows = cur.fetchall() or []
    return {
        "rows": [_normalize_row(r) for r in rows],
        "summary": [_normalize_row(r) for r in summary_rows],
        "dataset": ds,
        "limit": safe_limit,
    }



def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in dict(row).items():
        if isinstance(v, datetime):
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out





def _fetch_snapshots_in_range(*, source: str, start_dt: Optional[datetime], end_dt: Optional[datetime]) -> List[Dict[str, Any]]:
    filters = ["source = %s"]
    params: List[Any] = [source]
    if start_dt is not None:
        filters.append("received_at >= %s")
        params.append(start_dt)
    if end_dt is not None:
        filters.append("received_at <= %s")
        params.append(end_dt)
    sql = f"""
        SELECT id AS snapshot_id, received_at, feed_header_timestamp,
               trip_updates_count, stop_time_updates_count, vehicle_positions_count, alerts_count
        FROM feed_snapshots
        WHERE {' AND '.join(filters)}
        ORDER BY received_at ASC
    """
    with _connect() as conn:
        with conn.cursor(cursor_factory=_psycopg_modules()[2]) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []
    return [_normalize_row(r) for r in rows]


def _fetch_entity_rows(*, table: str, source: str, id_column: str, id_value: str, start_dt: Optional[datetime], end_dt: Optional[datetime], limit: int) -> List[Dict[str, Any]]:
    filters = ["source = %s", f"{id_column} = %s"]
    params: List[Any] = [source, id_value]
    if start_dt is not None:
        filters.append("recorded_at >= %s")
        params.append(start_dt)
    if end_dt is not None:
        filters.append("recorded_at <= %s")
        params.append(end_dt)
    safe_limit = max(1, min(int(limit or 2000), DB_QUERY_LIMIT_MAX))
    sql = f"SELECT * FROM {table} WHERE {' AND '.join(filters)} ORDER BY recorded_at DESC LIMIT %s"
    params.append(safe_limit)
    with _connect() as conn:
        with conn.cursor(cursor_factory=_psycopg_modules()[2]) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []
    return [_normalize_row(r) for r in rows]


def _fetch_entity_snapshot_presence(*, table: str, source: str, id_column: str, id_value: str, start_dt: Optional[datetime], end_dt: Optional[datetime]) -> Tuple[set[int], Optional[str], Optional[str]]:
    filters = ["source = %s", f"{id_column} = %s"]
    params: List[Any] = [source, id_value]
    if start_dt is not None:
        filters.append("recorded_at >= %s")
        params.append(start_dt)
    if end_dt is not None:
        filters.append("recorded_at <= %s")
        params.append(end_dt)
    sql = f"""
        SELECT snapshot_id, MIN(recorded_at) AS first_seen_at, MAX(recorded_at) AS last_seen_at
        FROM {table}
        WHERE {' AND '.join(filters)}
        GROUP BY snapshot_id
        ORDER BY snapshot_id ASC
    """
    with _connect() as conn:
        with conn.cursor(cursor_factory=_psycopg_modules()[2]) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []
    snapshot_ids = {int(r['snapshot_id']) for r in rows if r.get('snapshot_id') is not None}
    first_seen = _to_iso(rows[0].get('first_seen_at')) if rows else None
    last_seen = _to_iso(rows[-1].get('last_seen_at')) if rows else None
    return snapshot_ids, first_seen, last_seen


def _compute_presence_gaps(*, snapshots: List[Dict[str, Any]], present_snapshot_ids: set[int]) -> List[Dict[str, Any]]:
    if not snapshots:
        return []
    gaps: List[Dict[str, Any]] = []
    current = None
    expected_gap = max(1, REFRESH_SECONDS)
    for snap in snapshots:
        sid = snap.get('snapshot_id')
        present = sid in present_snapshot_ids
        if not present:
            if current is None:
                current = {
                    'gap_started_at': snap.get('received_at'),
                    'gap_ended_at': snap.get('received_at'),
                    'missing_snapshot_count': 1,
                    'snapshot_ids': [sid],
                }
            else:
                current['gap_ended_at'] = snap.get('received_at')
                current['missing_snapshot_count'] += 1
                current['snapshot_ids'].append(sid)
        elif current is not None:
            start_iso = current.get('gap_started_at')
            end_iso = current.get('gap_ended_at')
            duration_seconds = None
            try:
                start_dt = _parse_ts(start_iso)
                end_dt = _parse_ts(end_iso)
                if start_dt and end_dt:
                    duration_seconds = int(max(0, (end_dt - start_dt).total_seconds()) + expected_gap)
            except Exception:
                duration_seconds = None
            gaps.append({
                'gap_started_at': start_iso,
                'gap_ended_at': end_iso,
                'missing_snapshot_count': int(current.get('missing_snapshot_count') or 0),
                'approx_gap_seconds': duration_seconds,
                'first_missing_snapshot_id': current['snapshot_ids'][0] if current.get('snapshot_ids') else None,
                'last_missing_snapshot_id': current['snapshot_ids'][-1] if current.get('snapshot_ids') else None,
            })
            current = None
    if current is not None:
        start_iso = current.get('gap_started_at')
        end_iso = current.get('gap_ended_at')
        duration_seconds = None
        try:
            start_dt = _parse_ts(start_iso)
            end_dt = _parse_ts(end_iso)
            if start_dt and end_dt:
                duration_seconds = int(max(0, (end_dt - start_dt).total_seconds()) + expected_gap)
        except Exception:
            duration_seconds = None
        gaps.append({
            'gap_started_at': start_iso,
            'gap_ended_at': end_iso,
            'missing_snapshot_count': int(current.get('missing_snapshot_count') or 0),
            'approx_gap_seconds': duration_seconds,
            'first_missing_snapshot_id': current['snapshot_ids'][0] if current.get('snapshot_ids') else None,
            'last_missing_snapshot_id': current['snapshot_ids'][-1] if current.get('snapshot_ids') else None,
        })
    return gaps


def _entity_history_payload(*, source: str, table: str, id_column: str, id_value: str, start_time: Optional[str], end_time: Optional[str], limit: int) -> Dict[str, Any]:
    if not id_value or not str(id_value).strip():
        raise ValueError(f"Missing required filter: {id_column}")
    _ensure_initialized()
    start_dt = _parse_range_ts(start_time)
    end_dt = _parse_range_ts(end_time)
    rows = _fetch_entity_rows(table=table, source=source, id_column=id_column, id_value=str(id_value).strip(), start_dt=start_dt, end_dt=end_dt, limit=limit)
    snapshots = _fetch_snapshots_in_range(source=source, start_dt=start_dt, end_dt=end_dt)
    present_snapshot_ids, first_seen_at, last_seen_at = _fetch_entity_snapshot_presence(table=table, source=source, id_column=id_column, id_value=str(id_value).strip(), start_dt=start_dt, end_dt=end_dt)
    gaps = _compute_presence_gaps(snapshots=snapshots, present_snapshot_ids=present_snapshot_ids)
    return {
        'source': source,
        'entity_type': 'stop' if id_column == 'stop_id' else 'vehicle',
        'filter': {
            id_column: str(id_value).strip(),
            'start_time': _to_iso(start_dt),
            'end_time': _to_iso(end_dt),
            'limit': max(1, min(int(limit or 2000), DB_QUERY_LIMIT_MAX)),
        },
        'summary': {
            id_column: str(id_value).strip(),
            'snapshot_count_in_range': len(snapshots),
            'snapshot_hits': len(present_snapshot_ids),
            'snapshot_misses': max(0, len(snapshots) - len(present_snapshot_ids)),
            'gap_count': len(gaps),
            'first_seen_at': first_seen_at,
            'last_seen_at': last_seen_at,
            'expected_refresh_seconds': REFRESH_SECONDS,
        },
        'gaps': gaps,
        'rows': rows,
    }



def fetch_trace_history(*, source: str, vehicle_id: Optional[str] = None, trip_id: Optional[str] = None, route_id: Optional[str] = None, direction_id: Optional[int] = None, start_time: Optional[str] = None, end_time: Optional[str] = None, limit: int = 500) -> Dict[str, Any]:
    if not DB_ENABLED:
        raise RuntimeError("PostgreSQL storage is disabled")
    _ensure_initialized()

    start_dt = _parse_range_ts(start_time)
    end_dt = _parse_range_ts(end_time)
    safe_limit = max(1, min(int(limit or 500), DB_QUERY_LIMIT_MAX))

    base_filters = ["source = %s"]
    base_params: List[Any] = [source]
    if start_dt is not None:
        base_filters.append("recorded_at >= %s")
        base_params.append(start_dt)
    if end_dt is not None:
        base_filters.append("recorded_at <= %s")
        base_params.append(end_dt)
    if vehicle_id:
        base_filters.append("vehicle_id = %s")
        base_params.append(str(vehicle_id).strip())
    if trip_id:
        base_filters.append("trip_id = %s")
        base_params.append(str(trip_id).strip())
    if route_id:
        base_filters.append("(route_id = %s OR route_short_name = %s)")
        base_params.extend([str(route_id).strip(), str(route_id).strip()])
    if direction_id is not None:
        base_filters.append("direction_id = %s")
        base_params.append(int(direction_id))

    where_sql = " AND ".join(base_filters)

    unified_sql = f"""
        SELECT *
        FROM ({_history_query_sql('all')}) q
        WHERE {where_sql}
        ORDER BY recorded_at DESC, dataset ASC
        LIMIT %s
    """
    unified_params = [*base_params, safe_limit]

    summary_sql = f"""
        SELECT
            COUNT(*)::bigint AS row_count,
            MIN(recorded_at) AS first_recorded_at,
            MAX(recorded_at) AS last_recorded_at,
            COUNT(DISTINCT snapshot_id)::bigint AS snapshot_count,
            COUNT(DISTINCT trip_id) FILTER (WHERE trip_id IS NOT NULL) AS trip_count,
            COUNT(DISTINCT vehicle_id) FILTER (WHERE vehicle_id IS NOT NULL) AS vehicle_count,
            COUNT(DISTINCT stop_id) FILTER (WHERE stop_id IS NOT NULL) AS stop_count
        FROM ({_history_query_sql('all')}) q
        WHERE {where_sql}
    """

    matched_vehicles_sql = f"""
        SELECT vehicle_id,
               MAX(vehicle_label) AS vehicle_label,
               MAX(route_id) AS route_id,
               MAX(route_short_name) AS route_short_name,
               MAX(route_long_name) AS route_long_name,
               MAX(trip_id) AS trip_id,
               MAX(direction_id) AS direction_id,
               MAX(recorded_at) AS last_seen_at,
               COUNT(*)::bigint AS hit_count
        FROM ({_history_query_sql('all')}) q
        WHERE {where_sql} AND vehicle_id IS NOT NULL
        GROUP BY vehicle_id
        ORDER BY MAX(recorded_at) DESC
        LIMIT %s
    """
    matched_trips_sql = f"""
        SELECT trip_id,
               MAX(route_id) AS route_id,
               MAX(route_short_name) AS route_short_name,
               MAX(route_long_name) AS route_long_name,
               MAX(direction_id) AS direction_id,
               MAX(trip_headsign) AS trip_headsign,
               MAX(vehicle_id) AS vehicle_id,
               MAX(recorded_at) AS last_seen_at,
               COUNT(*)::bigint AS hit_count
        FROM ({_history_query_sql('all')}) q
        WHERE {where_sql} AND trip_id IS NOT NULL
        GROUP BY trip_id
        ORDER BY MAX(recorded_at) DESC
        LIMIT %s
    """

    with _connect() as conn:
        with conn.cursor(cursor_factory=_psycopg_modules()[2]) as cur:
            cur.execute(unified_sql, unified_params)
            rows = cur.fetchall() or []

            cur.execute(summary_sql, base_params)
            summary = cur.fetchone() or {}

            cur.execute(matched_vehicles_sql, [*base_params, min(safe_limit, 100)])
            matched_vehicles = cur.fetchall() or []

            cur.execute(matched_trips_sql, [*base_params, min(safe_limit, 100)])
            matched_trips = cur.fetchall() or []

    norm_rows = [_normalize_row(r) for r in rows]
    norm_matched_vehicles = [_normalize_row(r) for r in matched_vehicles]
    norm_matched_trips = [_normalize_row(r) for r in matched_trips]
    norm_summary = _normalize_row(summary)

    resolved_trip_id = str(trip_id).strip() if trip_id and str(trip_id).strip() else None
    if not resolved_trip_id and norm_matched_trips:
        resolved_trip_id = str(norm_matched_trips[0].get('trip_id') or '').strip() or None
    if not resolved_trip_id and norm_matched_vehicles:
        resolved_trip_id = str(norm_matched_vehicles[0].get('trip_id') or '').strip() or None

    timeline: List[Dict[str, Any]] = []
    if resolved_trip_id:
        timeline = [r for r in norm_rows if r.get('trip_id') == resolved_trip_id and r.get('dataset') == 'stop_time_updates']
        timeline.sort(key=lambda r: (
            r.get('recorded_at') or '',
            int(r.get('stop_sequence') or 0) if str(r.get('stop_sequence') or '').isdigit() else 0,
            str(r.get('stop_id') or ''),
        ))

    positions = [r for r in norm_rows if r.get('dataset') == 'vehicle_positions']

    return {
        'source': source,
        'query': {
            'vehicle_id': vehicle_id,
            'trip_id': trip_id,
            'route_id': route_id,
            'direction_id': direction_id,
            'start_time': _to_iso(start_dt),
            'end_time': _to_iso(end_dt),
            'limit': safe_limit,
        },
        'summary': norm_summary,
        'matched': {
            'vehicles': norm_matched_vehicles,
            'trips': norm_matched_trips,
        },
        'rows': norm_rows,
        'positions': positions,
        'resolved_trip_id': resolved_trip_id,
        'timeline': {
            'ok': True,
            'meta': {
                'trip_id': resolved_trip_id,
                'row_count': len(timeline),
            },
            'timeline': timeline,
        } if resolved_trip_id else None,
    }
def fetch_stop_history(*, source: str, stop_id: str, start_time: Optional[str] = None, end_time: Optional[str] = None, limit: int = 2000) -> Dict[str, Any]:
    return _entity_history_payload(
        source=source,
        table='stop_time_updates_history',
        id_column='stop_id',
        id_value=stop_id,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
    )


def fetch_vehicle_history(*, source: str, vehicle_id: str, start_time: Optional[str] = None, end_time: Optional[str] = None, limit: int = 2000) -> Dict[str, Any]:
    return _entity_history_payload(
        source=source,
        table='vehicle_positions_history',
        id_column='vehicle_id',
        id_value=vehicle_id,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
    )


def analytics_overview(*, source: str, hours: int = 24, top_n: int = 20) -> Dict[str, Any]:
    if not DB_ENABLED:
        raise RuntimeError("PostgreSQL storage is disabled")
    _ensure_initialized()
    safe_hours = max(1, min(int(hours or 24), 24 * 30))
    safe_top_n = max(1, min(int(top_n or 20), 100))
    interval_literal = f"{safe_hours} hours"

    with _connect() as conn:
        with conn.cursor(cursor_factory=_psycopg_modules()[2]) as cur:
            cur.execute(
                """
                SELECT id, source, received_at, feed_header_timestamp,
                       trip_updates_count, stop_time_updates_count, vehicle_positions_count, alerts_count,
                       dqi, penalty, issues
                FROM feed_snapshots
                WHERE source = %s
                ORDER BY received_at DESC
                LIMIT 1
                """,
                (source,),
            )
            last_snapshot = cur.fetchone() or {}

            cur.execute(
                """
                SELECT received_at, feed_header_timestamp, trip_updates_count, stop_time_updates_count,
                       vehicle_positions_count, alerts_count, dqi
                FROM feed_snapshots
                WHERE source = %s
                  AND received_at >= NOW() - (%s)::interval
                ORDER BY received_at ASC
                """,
                (source, interval_literal),
            )
            snapshots = cur.fetchall() or []

            cur.execute(
                """
                SELECT COUNT(DISTINCT vehicle_id)::bigint AS active_vehicles
                FROM vehicle_positions_history
                WHERE source = %s
                  AND recorded_at >= NOW() - INTERVAL '1 hour'
                  AND vehicle_id IS NOT NULL
                """,
                (source,),
            )
            active_vehicles = int((cur.fetchone() or {}).get("active_vehicles") or 0)

            cur.execute(
                """
                SELECT COUNT(DISTINCT trip_id)::bigint AS active_trips
                FROM trip_updates_history
                WHERE source = %s
                  AND recorded_at >= NOW() - INTERVAL '1 hour'
                  AND trip_id IS NOT NULL
                """,
                (source,),
            )
            active_trips = int((cur.fetchone() or {}).get("active_trips") or 0)

            cur.execute(
                """
                SELECT COUNT(DISTINCT stop_id)::bigint AS active_stops
                FROM stop_time_updates_history
                WHERE source = %s
                  AND recorded_at >= NOW() - INTERVAL '1 hour'
                  AND stop_id IS NOT NULL
                """,
                (source,),
            )
            active_stops = int((cur.fetchone() or {}).get("active_stops") or 0)

            cur.execute(
                """
                SELECT COUNT(DISTINCT route_id)::bigint AS active_routes
                FROM (
                    SELECT route_id FROM vehicle_positions_history WHERE source = %s AND recorded_at >= NOW() - INTERVAL '1 hour'
                    UNION ALL
                    SELECT route_id FROM trip_updates_history WHERE source = %s AND recorded_at >= NOW() - INTERVAL '1 hour'
                    UNION ALL
                    SELECT route_id FROM stop_time_updates_history WHERE source = %s AND recorded_at >= NOW() - INTERVAL '1 hour'
                ) t
                WHERE route_id IS NOT NULL
                """,
                (source, source, source),
            )
            active_routes = int((cur.fetchone() or {}).get("active_routes") or 0)

            cur.execute(
                """
                SELECT route_id, route_short_name,
                       COUNT(*)::bigint AS row_count,
                       COUNT(DISTINCT vehicle_id)::bigint AS active_vehicle_count,
                       MAX(recorded_at) AS last_seen
                FROM vehicle_positions_history
                WHERE source = %s
                  AND recorded_at >= NOW() - (%s)::interval
                  AND route_id IS NOT NULL
                GROUP BY route_id, route_short_name
                ORDER BY COUNT(*) DESC, MAX(recorded_at) DESC
                LIMIT %s
                """,
                (source, interval_literal, safe_top_n),
            )
            top_routes = [_normalize_row(r) for r in cur.fetchall() or []]

            cur.execute(
                """
                SELECT stop_id, stop_name,
                       COUNT(*)::bigint AS row_count,
                       MAX(recorded_at) AS last_seen
                FROM stop_time_updates_history
                WHERE source = %s
                  AND recorded_at >= NOW() - (%s)::interval
                  AND stop_id IS NOT NULL
                GROUP BY stop_id, stop_name
                ORDER BY COUNT(*) DESC, MAX(recorded_at) DESC
                LIMIT %s
                """,
                (source, interval_literal, safe_top_n),
            )
            top_stops = [_normalize_row(r) for r in cur.fetchall() or []]

            cur.execute(
                """
                SELECT trip_id, route_id, route_short_name,
                       COUNT(*)::bigint AS row_count,
                       MAX(recorded_at) AS last_seen
                FROM trip_updates_history
                WHERE source = %s
                  AND recorded_at >= NOW() - (%s)::interval
                  AND trip_id IS NOT NULL
                GROUP BY trip_id, route_id, route_short_name
                ORDER BY COUNT(*) DESC, MAX(recorded_at) DESC
                LIMIT %s
                """,
                (source, interval_literal, safe_top_n),
            )
            top_trips = [_normalize_row(r) for r in cur.fetchall() or []]

            cur.execute(
                """
                SELECT vehicle_id,
                       MAX(recorded_at) AS last_seen,
                       EXTRACT(EPOCH FROM (NOW() - MAX(recorded_at)))::bigint AS silence_seconds,
                       MAX(route_id) AS route_id,
                       MAX(route_short_name) AS route_short_name,
                       MAX(trip_id) AS trip_id
                FROM vehicle_positions_history
                WHERE source = %s
                  AND recorded_at >= NOW() - INTERVAL '24 hours'
                  AND vehicle_id IS NOT NULL
                GROUP BY vehicle_id
                HAVING NOW() - MAX(recorded_at) > INTERVAL '5 minutes'
                ORDER BY silence_seconds DESC
                LIMIT %s
                """,
                (source, safe_top_n),
            )
            silent_vehicles = [_normalize_row(r) for r in cur.fetchall() or []]

            cur.execute(
                """
                SELECT
                    date_trunc('hour', received_at) AS bucket_start,
                    COUNT(*)::bigint AS snapshot_count,
                    AVG(dqi)::numeric(10,2) AS avg_dqi,
                    MIN(dqi)::numeric(10,2) AS min_dqi,
                    MAX(received_at) AS last_snapshot_at,
                    SUM(CASE WHEN COALESCE(trip_updates_count, 0) = 0 THEN 1 ELSE 0 END)::bigint AS zero_trip_snapshots,
                    SUM(CASE WHEN COALESCE(stop_time_updates_count, 0) = 0 THEN 1 ELSE 0 END)::bigint AS zero_stop_snapshots,
                    SUM(CASE WHEN COALESCE(vehicle_positions_count, 0) = 0 THEN 1 ELSE 0 END)::bigint AS zero_vehicle_snapshots
                FROM feed_snapshots
                WHERE source = %s
                  AND received_at >= NOW() - (%s)::interval
                GROUP BY 1
                ORDER BY 1 DESC
                LIMIT 72
                """,
                (source, interval_literal),
            )
            hourly_coverage = [_normalize_row(r) for r in cur.fetchall() or []]

    gap_stats = _compute_gap_stats(snapshots)
    expected_per_hour = max(1, int(round(3600 / max(1, REFRESH_SECONDS))))
    for row in hourly_coverage:
        actual = int(row.get("snapshot_count") or 0)
        row["expected_snapshots"] = expected_per_hour
        row["uptime_percent"] = round((actual / expected_per_hour) * 100, 2) if expected_per_hour else None

    quality_checks = {
        "snapshots_in_window": len(snapshots),
        "expected_gap_seconds": gap_stats.get("expected_gap_seconds"),
        "gap_breach_threshold_seconds": gap_stats.get("gap_breach_threshold_seconds"),
        "low_dqi_snapshots": sum(1 for s in snapshots if s.get("dqi") is not None and float(s.get("dqi")) < 90),
        "zero_trip_snapshots": sum(1 for s in snapshots if int(s.get("trip_updates_count") or 0) == 0),
        "zero_stop_snapshots": sum(1 for s in snapshots if int(s.get("stop_time_updates_count") or 0) == 0),
        "zero_vehicle_snapshots": sum(1 for s in snapshots if int(s.get("vehicle_positions_count") or 0) == 0),
        "gap_breaches": gap_stats.get("breach_count", 0),
        "silent_vehicle_count": len(silent_vehicles),
    }
    last_received_at = _to_iso(last_snapshot.get("received_at"))
    last_header_at = _to_iso(last_snapshot.get("feed_header_timestamp"))
    header_lag_seconds = None
    if last_snapshot and last_snapshot.get("received_at") and last_snapshot.get("feed_header_timestamp"):
        recv = last_snapshot.get("received_at")
        hdr = last_snapshot.get("feed_header_timestamp")
        if isinstance(recv, datetime) and isinstance(hdr, datetime):
            header_lag_seconds = max(0, int((recv - hdr).total_seconds()))
    return {
        "source": source,
        "hours": safe_hours,
        "last_snapshot": _normalize_row(last_snapshot) if last_snapshot else None,
        "kpis": {
            "last_received_at": last_received_at,
            "last_feed_header_timestamp": last_header_at,
            "seconds_since_last_snapshot": _seconds_since(last_snapshot.get("received_at")),
            "seconds_since_last_header": _seconds_since(last_snapshot.get("feed_header_timestamp")),
            "last_header_lag_seconds": header_lag_seconds,
            "active_vehicles_last_hour": active_vehicles,
            "active_trips_last_hour": active_trips,
            "active_stops_last_hour": active_stops,
            "active_routes_last_hour": active_routes,
            "expected_gap_seconds": gap_stats.get("expected_gap_seconds"),
            "gap_breach_threshold_seconds": gap_stats.get("gap_breach_threshold_seconds"),
            "median_gap_seconds": gap_stats.get("median_gap_seconds"),
            "avg_gap_seconds": gap_stats.get("avg_gap_seconds"),
            "max_gap_seconds": gap_stats.get("max_gap_seconds"),
            "gap_breaches": gap_stats.get("breach_count", 0),
        },
        "quality_checks": quality_checks,
        "recent_gaps": gap_stats.get("largest_gaps", []),
        "hourly_coverage": hourly_coverage,
        "top_routes": top_routes,
        "top_stops": top_stops,
        "top_trips": top_trips,
        "silent_vehicles": silent_vehicles,
    }



def _seconds_since(v: Any) -> Optional[int]:
    if v is None:
        return None
    dt = v if isinstance(v, datetime) else _parse_ts(v)
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0, int((datetime.now(timezone.utc) - dt).total_seconds()))



def _compute_gap_stats(snapshots: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if not snapshots:
        return {
            "expected_gap_seconds": REFRESH_SECONDS,
            "gap_breach_threshold_seconds": max(15.0, float(REFRESH_SECONDS) * 3.0),
            "median_gap_seconds": None,
            "avg_gap_seconds": None,
            "max_gap_seconds": None,
            "breach_count": 0,
            "largest_gaps": [],
        }
    normalized: List[Dict[str, Any]] = []
    for row in snapshots:
        dt = row.get("received_at")
        if isinstance(dt, str):
            dt = _parse_ts(dt)
        elif isinstance(dt, datetime) and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if not isinstance(dt, datetime):
            continue
        normalized.append({**row, "received_at": dt})
    if len(normalized) < 2:
        return {
            "expected_gap_seconds": REFRESH_SECONDS,
            "gap_breach_threshold_seconds": max(15.0, float(REFRESH_SECONDS) * 3.0),
            "median_gap_seconds": None,
            "avg_gap_seconds": None,
            "max_gap_seconds": None,
            "breach_count": 0,
            "largest_gaps": [],
        }

    gaps: List[float] = []
    largest: List[Dict[str, Any]] = []
    for prev, curr in zip(normalized[:-1], normalized[1:]):
        gap = (curr["received_at"] - prev["received_at"]).total_seconds()
        if gap < 0:
            continue
        gaps.append(gap)
        largest.append(
            {
                "prev_received_at": prev["received_at"].isoformat(),
                "curr_received_at": curr["received_at"].isoformat(),
                "gap_seconds": round(gap, 2),
                "prev_trip_updates_count": prev.get("trip_updates_count"),
                "curr_trip_updates_count": curr.get("trip_updates_count"),
                "prev_stop_time_updates_count": prev.get("stop_time_updates_count"),
                "curr_stop_time_updates_count": curr.get("stop_time_updates_count"),
                "prev_vehicle_positions_count": prev.get("vehicle_positions_count"),
                "curr_vehicle_positions_count": curr.get("vehicle_positions_count"),
                "prev_dqi": prev.get("dqi"),
                "curr_dqi": curr.get("dqi"),
            }
        )
    if not gaps:
        return {
            "expected_gap_seconds": REFRESH_SECONDS,
            "gap_breach_threshold_seconds": max(15.0, float(REFRESH_SECONDS) * 3.0),
            "median_gap_seconds": None,
            "avg_gap_seconds": None,
            "max_gap_seconds": None,
            "breach_count": 0,
            "largest_gaps": [],
        }

    sorted_gaps = sorted(gaps)
    mid = len(sorted_gaps) // 2
    median = sorted_gaps[mid] if len(sorted_gaps) % 2 else (sorted_gaps[mid - 1] + sorted_gaps[mid]) / 2
    avg_gap = sum(gaps) / len(gaps)
    max_gap = max(gaps)
    breach_threshold = max(15.0, float(REFRESH_SECONDS) * 3.0)
    largest = sorted(largest, key=lambda x: x["gap_seconds"], reverse=True)[:10]
    breach_count = sum(1 for g in gaps if g > breach_threshold)
    return {
        "expected_gap_seconds": REFRESH_SECONDS,
        "gap_breach_threshold_seconds": round(breach_threshold, 2),
        "median_gap_seconds": round(median, 2),
        "avg_gap_seconds": round(avg_gap, 2),
        "max_gap_seconds": round(max_gap, 2),
        "breach_count": breach_count,
        "largest_gaps": largest,
    }
