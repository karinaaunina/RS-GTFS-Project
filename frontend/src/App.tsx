import React, { useEffect, useMemo, useState } from "react";
import MapView from "./MapView";
import TraceView from "./TraceView";

type Status = {
  gtfs_rt_url?: string;
  tz: string;
  refresh_seconds: number;
  last_refresh_utc: string | null;
  feed_header_timestamp_local: string | null;
  static_gtfs?: {
    loaded: boolean;
    path?: string | null;
    counts?: Record<string, number>;
  };
  counts: Record<string, number>;
  db?: {
    enabled?: boolean;
    connected?: boolean;
    initialized?: boolean;
    error?: string | null;
    last_received_at?: string | null;
    last_feed_header_timestamp?: string | null;
    counts?: Record<string, number>;
  };
  db_last_store?: {
    ok?: boolean;
    snapshot_id?: number | null;
    inserted?: Record<string, number>;
    error?: string | null;
  } | null;
};

type Source = { name: string; url: string };

type CompareResult = {
  source_a: string;
  source_b: string;
  last_refresh_utc: Record<string, string | null>;
  counts: Record<string, Record<string, number>>;
  missing_stop_time_events: Record<string, number>;
  missing_stop_ids: Record<string, number>;
  samples: Record<string, any[]>;
};

type HistoryResult = {
  rows: Row[];
  summary: Row[];
  dataset: string;
  limit: number;
};

type AnalyticsResult = {
  source: string;
  hours: number;
  last_snapshot?: Row | null;
  kpis: Row;
  quality_checks: Row;
  recent_gaps: Row[];
  hourly_coverage: Row[];
  top_routes: Row[];
  top_stops: Row[];
  top_trips: Row[];
  silent_vehicles: Row[];
};

type EntityHistoryResult = {
  source: string;
  entity_type: "stop" | "vehicle";
  filter: Row;
  summary: Row;
  gaps: Row[];
  rows: Row[];
};

type Row = Record<string, any>;

type ColumnLabels = Record<string, string>;


type MonitorEvent = {
  ts_local?: string;
  ts_utc?: string;
  source: string;
  dataset: string;
  event: string; // e.g. dataset_empty / missing_columns / empty_columns / warning / info
  row_count?: number | null;
  missing_columns?: string[] | null;
  empty_columns?: string[] | null;
  note?: string | null;
};


const API_BASE = import.meta.env.VITE_API_BASE ? String(import.meta.env.VITE_API_BASE) : "";

function toDateTimeLocalValue(date: Date) {
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function hoursAgoLocalValue(hours: number) {
  return toDateTimeLocalValue(new Date(Date.now() - hours * 3600 * 1000));
}

function nowLocalValue() {
  return toDateTimeLocalValue(new Date());
}

function isIsoLike(s: string) {
  return /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(s);
}

function isEmptyLike(v: any) {
  if (v === null || v === undefined) return true;
  if (typeof v === "number") return !Number.isFinite(v);
  if (typeof v === "string") {
    const s = v.trim().toLowerCase();
    return s === "" || s === "nan" || s === "none" || s === "null";
  }
  if (Array.isArray(v)) return v.length === 0;
  if (typeof v === "object") return Object.keys(v).length === 0;
  return false;
}

function fmt(v: any) {
  if (isEmptyLike(v)) return "—";
  // Keep dates readable
  if (typeof v === "string") {
    const s = v;
    if (isIsoLike(s)) {
      const d = new Date(s);
      if (!Number.isNaN(d.getTime())) {
        return new Intl.DateTimeFormat("lv-LV", {
          year: "numeric",
          month: "2-digit",
          day: "2-digit",
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit",
          hour12: false,
        }).format(d);
      }
    }
    return s;
  }

  if (Array.isArray(v)) {
    const arr = v.map((x) => (isEmptyLike(x) ? "" : String(x))).filter(Boolean);
    if (!arr.length) return "—";
    const head = arr.slice(0, 20).join(", ");
    return arr.length > 20 ? `${head} …(+${arr.length - 20})` : head;
  }

  if (typeof v === "object") {
    try {
      const obj = v as Record<string, any>;
      const keys = Object.keys(obj).filter((k) => !isEmptyLike(obj[k]));
      if (!keys.length) return "—";
      if (keys.every((k) => typeof obj[k] === "number" || obj[k] === null)) {
        const parts = keys.slice(0, 8).map((k) => `${k}:${obj[k] ?? "—"}`);
        const head = parts.join(", ");
        return keys.length > 8 ? `${head} …(+${keys.length - 8})` : head;
      }
      const js = JSON.stringify(v);
      return js.length > 180 ? js.slice(0, 180) + "…" : js;
    } catch {
      return String(v);
    }
  }

  return String(v);
}

function Table({
  rows,
  title,
  columnLabels,
  onRowClick,
}: {
  rows: Row[];
  title: string;
  columnLabels?: ColumnLabels;
  onRowClick?: (row: Row) => void;
}) {
  const columns = useMemo(() => {
    const sample = rows.slice(0, 500);
    const keys = new Set<string>();
    sample.forEach((r) => Object.keys(r).forEach((k) => keys.add(k)));
    return Array.from(keys).filter((k) => sample.some((r) => !isEmptyLike(r[k])));
  }, [rows]);

  const labelFor = (c: string) => {
    const lbl = columnLabels?.[c];
    return lbl ? `${lbl} (${c})` : c;
  };

  return (
    <div className="card card--main" style={{ marginTop: 12 }}>
      <div className="card__hd">
        <div className="card__title">
          <h2>{title}</h2>
          <p>{rows.length} ieraksti</p>
        </div>
        {rows.length === 0 ? <span className="badge">Nav datu</span> : <span className="badge badge--ok">OK</span>}
      </div>

      <div className="card__bd">
        {rows.length === 0 ? (
          <p style={{ margin: 0, color: "var(--muted)" }}>Nav datu.</p>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  {columns.map((c) => (
                    <th key={c} title={c}>
                      {labelFor(c)}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.slice(0, 2000).map((r, idx) => (
                  <tr
                    key={idx}
                    style={onRowClick ? { cursor: "pointer" } : undefined}
                    onClick={() => onRowClick?.(r)}
                  >
                    {columns.map((c) => (
                      <td
                        key={`${idx}-${c}`}
                        title={
                          typeof r[c] === "object" && r[c] !== null
                            ? (() => {
                                try {
                                  return JSON.stringify(r[c], null, 2);
                                } catch {
                                  return fmt(r[c]);
                                }
                              })()
                            : fmt(r[c])
                        }
                        className={
                          c.includes("id") ||
                          c.includes("timestamp") ||
                          c.includes("ts_") ||
                          c.includes("_utc") ||
                          c.includes("_local")
                            ? "cell-mono"
                            : ""
                        }
                      >
                        {fmt(r[c])}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {rows.length > 2000 && (
          <div style={{ marginTop: 10, fontSize: 12, color: "var(--muted)" }}>Rādām pirmās 2000 rindas.</div>
        )}
      </div>
    </div>
  );
}

function toNumeric(v: any): number | null {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function BarListCard({
  title,
  rows,
  labelKey,
  valueKey,
  fallbackLabelKeys = [],
  suffix = "",
  maxItems = 12,
}: {
  title: string;
  rows: Row[];
  labelKey: string;
  valueKey: string;
  fallbackLabelKeys?: string[];
  suffix?: string;
  maxItems?: number;
}) {
  const items = useMemo(() => {
    const prepared = (rows ?? [])
      .map((row) => {
        const labelCandidates = [labelKey, ...fallbackLabelKeys]
          .map((k) => row?.[k])
          .find((v) => !isEmptyLike(v));
        const value = toNumeric(row?.[valueKey]);
        if (!labelCandidates || value === null) return null;
        return { label: String(labelCandidates), value };
      })
      .filter(Boolean) as Array<{ label: string; value: number }>;
    const sliced = prepared.slice(0, maxItems);
    const maxValue = sliced.reduce((m, item) => Math.max(m, item.value), 0);
    return { rows: sliced, maxValue: maxValue > 0 ? maxValue : 1 };
  }, [rows, labelKey, valueKey, fallbackLabelKeys, maxItems]);

  return (
    <div className="card card--main" style={{ marginTop: 12 }}>
      <div className="card__hd">
        <div className="card__title">
          <h2>{title}</h2>
          <p>{items.rows.length} rindas</p>
        </div>
        {items.rows.length === 0 ? <span className="badge">Nav datu</span> : <span className="badge badge--ok">diagramma</span>}
      </div>
      <div className="card__bd">
        {items.rows.length === 0 ? (
          <p style={{ margin: 0, color: "var(--muted)" }}>Nav datu.</p>
        ) : (
          <div style={{ display: "grid", gap: 10 }}>
            {items.rows.map((item, idx) => {
              const width = Math.max(4, Math.round((item.value / items.maxValue) * 100));
              return (
                <div key={`${item.label}-${idx}`} style={{ display: "grid", gap: 6 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center" }}>
                    <div className="cell-mono" style={{ minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={item.label}>
                      {item.label}
                    </div>
                    <div className="cell-mono" style={{ flexShrink: 0 }}>{fmt(item.value)}{suffix}</div>
                  </div>
                  <div style={{ height: 10, borderRadius: 999, background: "var(--bg-elev)", overflow: "hidden", border: "1px solid var(--border)" }}>
                    <div style={{ width: `${width}%`, height: "100%", borderRadius: 999, background: "linear-gradient(90deg, var(--primary), var(--ok))" }} />
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}

export default function App() {
  const [sources, setSources] = useState<Source[]>([]);
  const [source, setSource] = useState<string>("mobis");

  const [status, setStatus] = useState<Status | null>(null);
  const [tab, setTab] = useState<
    "trips" | "stops" | "vehicles" | "map" | "trace" | "alerts" | "anomalies" | "metrics" | "compare" | "monitor" | "history" | "analytics"
  >("trips");

  const tabLabel = (t: typeof tab): string => {
    switch (t) {
      case "trips": return "Reisi";
      case "stops": return "Pieturas";
      case "vehicles": return "Transportlīdzekļi";
      case "map": return "Karte";
      case "trace": return "Izsekošana";
      case "alerts": return "Brīdinājumi";
      case "anomalies": return "Anomālijas";
      case "metrics": return "Rādītāji";
      case "compare": return "Salīdzinājums";
      case "monitor": return "Monitorings";
      case "history": return "Vēsture";
      case "analytics": return "Analītika";
      default: return String(t);
    }
  };
  const [rows, setIeraksti] = useState<Row[]>([]);
  // Global filters (applied to list views; Stop ID is also passed to Map)
  const [stopIdFiltrs, setStopIdFiltrs] = useState<string>("");
  const [routeIdFiltrs, setRouteIdFiltrs] = useState<string>("");
  const [tripIdFiltrs, setTripIdFiltrs] = useState<string>("");
  const [vehicleIdFiltrs, setVehicleIdFiltrs] = useState<string>("");
  const [freeTextFiltrs, setFreeTextFiltrs] = useState<string>("");
  // Monitoringa filtri (tikai "Monitorings" tabam)
  const [monitorQuery, setMonitorQuery] = useState<string>("");
  const [monitorHours, setMonitorHours] = useState<number>(24);
  const [monitorDataset, setMonitorDataset] = useState<string>("");
  const [monitorEventType, setMonitorEventType] = useState<string>("");
  const [monitorEvents, setMonitorEvents] = useState<MonitorEvent[]>([]);
  const [selectedRow, setSelectedRow] = useState<Row | null>(null);
  const [showRowModal, setShowRowModal] = useState(false);

  const [compare, setCompare] = useState<CompareResult | null>(null);
  const [historyDataset, setHistoryDataset] = useState<string>("all");
  const [historyStart, setHistoryStart] = useState<string>(hoursAgoLocalValue(6));
  const [historyEnd, setHistoryEnd] = useState<string>(nowLocalValue());
  const [historyLimit, setHistoryLimit] = useState<number>(2000);
  const [historySummary, setHistorySummary] = useState<Row[]>([]);
  const [historyStopDetails, setHistoryStopDetails] = useState<EntityHistoryResult | null>(null);
  const [historyVehicleDetails, setHistoryVehicleDetails] = useState<EntityHistoryResult | null>(null);
  const [analyticsHours, setAnalyticsHours] = useState<number>(24);
  const [analyticsData, setAnalyticsData] = useState<AnalyticsResult | null>(null);
const monitorFiltered = useMemo(() => {
  const now = Date.now();
  const q = (monitorQuery ?? "").trim().toLowerCase();
  const hours = Number(monitorHours ?? 24);

  return (monitorEvents ?? []).filter((ev: any) => {
    // 1) periods (pēc ts_utc vai ts_local)
    if (hours > 0) {
      const t = ev.ts_utc || ev.ts_local;
      if (t) {
        const ms = new Date(t).getTime();
        if (!Number.isNaN(ms)) {
          const ageH = (now - ms) / 3600000;
          if (ageH > hours) return false;
        }
      }
    }

    // 2) dataset filtrs
    if (monitorDataset && monitorDataset !== "all") {
      if ((ev.dataset ?? "") !== monitorDataset) return false;
    }

    // 3) event type filtrs
    if (monitorEventType && monitorEventType !== "all") {
      if ((ev.event ?? ev.event_type ?? ev.kind ?? "") !== monitorEventType) return false;
    }

    // 4) brīvais teksts (meklē pa visu event JSON)
    if (q) {
      const hay = JSON.stringify(ev).toLowerCase();
      if (!hay.includes(q)) return false;
    }

    return true;
  });
}, [monitorEvents, monitorQuery, monitorHours, monitorDataset, monitorEventType]);

  const monitorColumnLabels: ColumnLabels = {
    ts_local: "Laiks (vietējais)",
    ts_utc: "Laiks (UTC)",
    source: "Avots",
    dataset: "Datu kopa",
    event: "Notikums",
    row_count: "Rindu skaits",
    missing_columns: "Trūkstošās kolonnas",
    empty_columns: "Tukšās kolonnas",
    note: "Piezīme",
  };

  const filteredIeraksti = useMemo(() => {
    const baseRows = tab === "monitor" ? (monitorFiltered as any as Row[]) : rows;

    const f = {
      stopId: stopIdFiltrs,
      routeId: routeIdFiltrs,
      tripId: tripIdFiltrs,
      vehicleId: vehicleIdFiltrs,
      q: freeTextFiltrs,
    };

    if (!baseRows.length) return baseRows;
    if (!f.stopId.trim() && !f.routeId.trim() && !f.tripId.trim() && !f.vehicleId.trim() && !f.q.trim()) return baseRows;

    return baseRows.filter((r) => rowMatchesFiltrss(r, f));
  }, [rows, monitorFiltered, stopIdFiltrs, routeIdFiltrs, tripIdFiltrs, vehicleIdFiltrs, freeTextFiltrs, tab]);
  const [compareA, setCompareA] = useState<string>("mobis");
  const [compareB, setCompareB] = useState<string>("saraksti");

  const [autoAtjaunot, setAutoAtjaunot] = useState<boolean>(true);
  const [loading, setIelāde] = useState<boolean>(false);
  const [error, setKļūda] = useState<string | null>(null);

  const [anomalyKind, setAnomalyKind] = useState<string>("");
  const [anomalyMeklēt, setAnomalyMeklēt] = useState<string>("");

  const [theme, setTheme] = useState<"light" | "dark">("dark");

  const clearGlobalFilters = () => {
    setStopIdFiltrs("");
    setRouteIdFiltrs("");
    setTripIdFiltrs("");
    setVehicleIdFiltrs("");
    setFreeTextFiltrs("");
  };

  const tabFilterSupport: Record<string, { stop: boolean; route: boolean; trip: boolean; vehicle: boolean; text: boolean }> = {
    trips: { stop: true, route: true, trip: true, vehicle: true, text: true },
    stops: { stop: true, route: true, trip: true, vehicle: true, text: true },
    vehicles: { stop: false, route: true, trip: true, vehicle: true, text: true },
    map: { stop: true, route: true, trip: true, vehicle: true, text: true },
    alerts: { stop: false, route: true, trip: true, vehicle: false, text: true },
    anomalies: { stop: false, route: true, trip: true, vehicle: true, text: true },
    metrics: { stop: false, route: true, trip: false, vehicle: false, text: true },
    history: { stop: true, route: true, trip: true, vehicle: true, text: false },
    monitor: { stop: false, route: false, trip: false, vehicle: false, text: false },
    trace: { stop: false, route: false, trip: false, vehicle: false, text: false },
    compare: { stop: false, route: false, trip: false, vehicle: false, text: false },
    analytics: { stop: false, route: false, trip: false, vehicle: false, text: false },
  };

  useEffect(() => {
    const html = document.documentElement;
    html.setAttribute("data-theme", theme);
  }, [theme]);

  

function rowMatchesFiltrss(
  r: Row,
  {
    stopId,
    routeId,
    tripId,
    vehicleId,
    q,
  }: { stopId: string; routeId: string; tripId: string; vehicleId: string; q: string }
) {
  const norm = (v: any) => (v === null || v === undefined ? "" : String(v)).toLowerCase();

  const normId = (v: any) => {
    const s = (v ?? "").toString().trim().toLowerCase();
    if (!s) return "";
    // if it's numeric-like, compare without leading zeros
    if (/^\d+$/.test(s)) {
      const z = s.replace(/^0+/, "");
      return z || "0";
    }
    return s;
  };

  const idMatch = (value: any, filter: string) => {
    const f = (filter ?? "").toString().trim().toLowerCase();
    if (!f) return true;
    const fv = normId(f);
    const vv = normId(value);
    if (!vv) return false;
    // exact or partial
    return vv === fv || vv.includes(fv) || fv.includes(vv);
  };

  const stop = stopId.trim();
  const route = routeId.trim();
  const trip = tripId.trim();
  const veh = vehicleId.trim();
  const query = q.trim().toLowerCase();

  // Try common keys; fall back to scanning all fields for free-text
  const stopVal = r.stop_id ?? (r as any).stopId ?? (r as any).stop ?? (r as any).current_stop_id;
  const routeVal = r.route_id ?? (r as any).routeId ?? (r as any).route ?? (r as any).route_short_name;
  const tripVal = r.trip_id ?? (r as any).tripId ?? (r as any).trip ?? (r as any).trip_update_id;
  const vehVal = r.vehicle_id ?? (r as any).vehicleId ?? (r as any).vehicle ?? (r as any).vehicle_label;

  if (stop && !idMatch(stopVal, stop)) return false;
  if (route && !idMatch(routeVal, route)) return false;
  if (trip && !idMatch(tripVal, trip)) return false;
  if (veh && !idMatch(vehVal, veh)) return false;

  if (query) {
    const hay = Object.values(r)
      .slice(0, 200)
      .map(norm)
      .join(" | ");
    if (!hay.includes(query)) return false;
  }
  return true;
}


async function fetchJson(path: string, opts?: RequestInit) {
    const res = await fetch(`${API_BASE}${path}`, opts);
    if (!res.ok) {
      const t = await res.text();
      throw new Error(`${res.status} ${res.statusText}: ${t}`);
    }
    return res.json();
  }

  function _filenameFromContentDisposition(cd: string | null): string | null {
    if (!cd) return null;
    // e.g. attachment; filename="gtfs_rt_mobis_20260127_120000.xlsx"
    const m = /filename\*=UTF-8''([^;]+)|filename="([^"]+)"|filename=([^;]+)/i.exec(cd);
    if (!m) return null;
    const name = decodeURIComponent(m[1] || m[2] || m[3] || "").trim();
    return name || null;
  }

  async function downloadFile(path: string, fallbackName: string) {
    setKļūda(null);
    try {
      const res = await fetch(`${API_BASE}${path}`);
      if (!res.ok) {
        const t = await res.text();
        throw new Error(`${res.status} ${res.statusText}: ${t}`);
      }
      const blob = await res.blob();
      const cd = res.headers.get("Content-Disposition");
      const filename = _filenameFromContentDisposition(cd) ?? fallbackName;

      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);
    } catch (e: any) {
      setKļūda(e?.message ?? String(e));
    }
  }

  async function loadSources() {
    try {
      const data = await fetchJson("/api/sources");
      const list: Source[] = data.sources ?? [];
      setSources(list);
      if (list.length && !list.find((s) => s.name === source)) setSource(list[0].name);
    } catch {
      // ignore
    }
  }

  async function loadTab(
    t: typeof tab,
    opts?: {
      forceRefresh?: boolean;
      filterOverrides?: {
        stopId?: string;
        routeId?: string;
        tripId?: string;
        vehicleId?: string;
        freeText?: string;
      };
    }
  ) {
    setIelāde(true);
    setKļūda(null);

    try {
      await loadSources();

      const effectiveStopId = opts?.filterOverrides?.stopId ?? stopIdFiltrs;
      const effectiveRouteId = opts?.filterOverrides?.routeId ?? routeIdFiltrs;
      const effectiveTripId = opts?.filterOverrides?.tripId ?? tripIdFiltrs;
      const effectiveVehicleId = opts?.filterOverrides?.vehicleId ?? vehicleIdFiltrs;

      if (t === "compare") {
        // Ieteicams: refreshojam abus avotus pirms compare, citādi salīdzina vecus snapshotus
        if (opts?.forceRefresh) {
          await fetchJson(`/api/refresh?source=${encodeURIComponent(compareA)}`, { method: "POST" });
          await fetchJson(`/api/refresh?source=${encodeURIComponent(compareB)}`, { method: "POST" });
        }

        const cmp = await fetchJson(
          `/api/compare?source_a=${encodeURIComponent(compareA)}&source_b=${encodeURIComponent(compareB)}`
        );
        setCompare(cmp);
        setIeraksti([]);
        setStatus(null);
        return;
      }

      if (t === "map") {
        // Map has its own data loader; we only fetch status here
        if (opts?.forceRefresh) {
          const st = await fetchJson(`/api/refresh?source=${encodeURIComponent(source)}`, { method: "POST" });
          setStatus(st);
        } else {
          const st = await fetchJson(`/api/status?source=${encodeURIComponent(source)}`);
          setStatus(st);
        }
        setIeraksti([]);
        setCompare(null);
        return;
      }

      if (opts?.forceRefresh) {
        const st = await fetchJson(`/api/refresh?source=${encodeURIComponent(source)}`, { method: "POST" });
        setStatus(st);
      } else {
        const st = await fetchJson(`/api/status?source=${encodeURIComponent(source)}`);
        setStatus(st);
      }


      if (t === "monitor") {
        // Feed quality monitoring (events written by backend)
        const q = new URLSearchParams({ source });
        // backend may ignore these params, but they are safe
        if (monitorHours) q.set("hours", String(monitorHours));
        if (monitorDataset) q.set("dataset", monitorDataset);
        if (monitorEventType) q.set("event", monitorEventType);
        const ev = await fetchJson(`/api/monitor/events?${q.toString()}`);
        setMonitorEvents(ev);
        setIeraksti(ev as any);
        setCompare(null);
        setHistorySummary([]);
        setHistoryStopDetails(null);
        setHistoryVehicleDetails(null);
        setAnalyticsData(null);
        return;
      }

      if (t === "history") {
        const q = new URLSearchParams({
          source,
          dataset: historyDataset,
          limit: String(historyLimit),
        });
        if (historyStart) q.set("start_time", new Date(historyStart).toISOString());
        if (historyEnd) q.set("end_time", new Date(historyEnd).toISOString());
        if (effectiveStopId.trim()) q.set("stop_id", effectiveStopId.trim());
        if (effectiveRouteId.trim()) q.set("route_id", effectiveRouteId.trim());
        if (effectiveTripId.trim()) q.set("trip_id", effectiveTripId.trim());
        if (effectiveVehicleId.trim()) q.set("vehicle_id", effectiveVehicleId.trim());
        const history = (await fetchJson(`/api/history?${q.toString()}`)) as HistoryResult;

        let stopDetails: EntityHistoryResult | null = null;
        let vehicleDetails: EntityHistoryResult | null = null;

        if (effectiveStopId.trim()) {
          const stopQ = new URLSearchParams({ source, stop_id: effectiveStopId.trim(), limit: String(historyLimit) });
          if (historyStart) stopQ.set("start_time", new Date(historyStart).toISOString());
          if (historyEnd) stopQ.set("end_time", new Date(historyEnd).toISOString());
          stopDetails = (await fetchJson(`/api/history/stops?${stopQ.toString()}`)) as EntityHistoryResult;
        }

        if (effectiveVehicleId.trim()) {
          const vehicleQ = new URLSearchParams({ source, vehicle_id: effectiveVehicleId.trim(), limit: String(historyLimit) });
          if (historyStart) vehicleQ.set("start_time", new Date(historyStart).toISOString());
          if (historyEnd) vehicleQ.set("end_time", new Date(historyEnd).toISOString());
          vehicleDetails = (await fetchJson(`/api/history/vehicles?${vehicleQ.toString()}`)) as EntityHistoryResult;
        }

        setHistorySummary(history.summary ?? []);
        setHistoryStopDetails(stopDetails);
        setHistoryVehicleDetails(vehicleDetails);
        setIeraksti(history.rows ?? []);
        setCompare(null);
        setAnalyticsData(null);
        return;
      }

      if (t === "analytics") {
        const q = new URLSearchParams({ source, hours: String(analyticsHours) });
        const overview = (await fetchJson(`/api/analytics/overview?${q.toString()}`)) as AnalyticsResult;
        setAnalyticsData(overview);
        setHistorySummary([]);
        setHistoryStopDetails(null);
        setHistoryVehicleDetails(null);
        setIeraksti([]);
        setCompare(null);
        return;
      }

      let data: Row[] = [];
      if (t === "trips") data = await fetchJson(`/api/trip-updates?source=${encodeURIComponent(source)}&limit=5000`);
      if (t === "stops") data = await fetchJson(`/api/stop-time-updates?source=${encodeURIComponent(source)}&limit=5000`);
      if (t === "vehicles") data = await fetchJson(`/api/vehicles?source=${encodeURIComponent(source)}&limit=5000`);
      if (t === "alerts") data = await fetchJson(`/api/alerts?source=${encodeURIComponent(source)}&limit=5000`);
      if (t === "anomalies") data = await fetchJson(`/api/anomalies?source=${encodeURIComponent(source)}&limit=5000`);
      if (t === "metrics") data = await fetchJson(`/api/metrics/routes?source=${encodeURIComponent(source)}`);

      setHistorySummary([]);
      setHistoryStopDetails(null);
      setHistoryVehicleDetails(null);
      setAnalyticsData(null);
      setIeraksti(data);
      setCompare(null);
    } catch (e: any) {
      setKļūda(e?.message ?? String(e));
      setIeraksti([]);
      setCompare(null);
      setHistoryStopDetails(null);
      setHistoryVehicleDetails(null);
      setStatus(null);
    } finally {
      setIelāde(false);
    }
  }

  useEffect(() => {
    loadSources();
    loadTab(tab, { forceRefresh: false });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    const support = tabFilterSupport[tab] ?? { stop: false, route: false, trip: false, vehicle: false, text: false };
    const nextStopId = support.stop ? stopIdFiltrs : "";
    const nextRouteId = support.route ? routeIdFiltrs : "";
    const nextTripId = support.trip ? tripIdFiltrs : "";
    const nextVehicleId = support.vehicle ? vehicleIdFiltrs : "";
    const nextFreeText = support.text ? freeTextFiltrs : "";

    if (!support.stop && stopIdFiltrs) setStopIdFiltrs("");
    if (!support.route && routeIdFiltrs) setRouteIdFiltrs("");
    if (!support.trip && tripIdFiltrs) setTripIdFiltrs("");
    if (!support.vehicle && vehicleIdFiltrs) setVehicleIdFiltrs("");
    if (!support.text && freeTextFiltrs) setFreeTextFiltrs("");

    loadTab(tab, {
      forceRefresh: false,
      filterOverrides: {
        stopId: nextStopId,
        routeId: nextRouteId,
        tripId: nextTripId,
        vehicleId: nextVehicleId,
        freeText: nextFreeText,
      },
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, source, compareA, compareB]);

  useEffect(() => {
    if (!autoAtjaunot || tab === "compare" || tab === "history" || tab === "analytics") return;
    const intervalMs = Math.max(5, status?.refresh_seconds ?? 30) * 1000;
    const id = window.setInterval(() => loadTab(tab, { forceRefresh: false }), intervalMs);
    return () => window.clearInterval(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoAtjaunot, status?.refresh_seconds, tab, source]);

  const title =
    tab === "trips"
      ? "Reisu atjauninājumi"
      : tab === "stops"
      ? "Pieturu laiku atjauninājumi"
      : tab === "vehicles"
      ? "Transportlīdzekļu pozīcijas"
      : tab === "map"
      ? "Karte"
      : tab === "alerts"
      ? "Brīdinājumi"
      : tab === "anomalies"
      ? "Anomālijas (datu pārrāvumi / trūkstoši countdown dati)"
      : tab === "metrics"
      ? "Maršrutu aktivitātes rādītāji"
      : tab === "history"
      ? "PostgreSQL vēsture"
      : tab === "analytics"
      ? "Analītikas pārskats"
      : "Avotu salīdzinājums";

  const countsText =
    tab !== "compare" && status
      ? Object.entries(status.counts)
          .map(([k, v]) => `${k}=${v}`)
          .join(", ")
      : "—";

  const eventsKeyA = compare ? `${compare.source_a}_events_not_in_${compare.source_b}` : "";
  const eventsKeyB = compare ? `${compare.source_b}_events_not_in_${compare.source_a}` : "";
  const stopsKeyA = compare ? `${compare.source_a}_stop_ids_not_in_${compare.source_b}` : "";
  const stopsKeyB = compare ? `${compare.source_b}_stop_ids_not_in_${compare.source_a}` : "";

  const eventsSamples =
    compare?.samples ? ([] as any[]).concat(compare.samples[eventsKeyA] ?? [], compare.samples[eventsKeyB] ?? []) : [];
  const stopsSamples =
    compare?.samples ? ([] as any[]).concat(compare.samples[stopsKeyA] ?? [], compare.samples[stopsKeyB] ?? []) : [];

  const anomalyKinds = useMemo(() => {
    if (tab !== "anomalies") return [] as string[];
    const s = new Set<string>();
    rows.forEach((r) => {
      const k = r?.kind ? String(r.kind) : "";
      if (k) s.add(k);
    });
    return Array.from(s).sort();
  }, [rows, tab]);

  const ierakstiView = useMemo(() => {
    // Apply global filters first for every tab.
    const base = filteredIeraksti;
    if (tab !== "anomalies") return base;
    const q = anomalyMeklēt.trim().toLowerCase();
    return base.filter((r) => {
      if (anomalyKind && String(r.kind) !== anomalyKind) return false;
      if (!q) return true;
      // search across a few key fields (avoid huge JSON.stringify)
      const hay = `${r.kind ?? ""} ${r.route_short_name ?? ""} ${r.route_id ?? ""} ${r.stop_name ?? ""} ${r.stop_id ?? ""} ${r.trip_id ?? ""} ${r.vehicle_id ?? ""} ${r.note ?? ""}`.toLowerCase();
      return hay.includes(q);
    });
  }, [rows, tab, anomalyKind, anomalyMeklēt]);

  return (
    <div className="container">
      <div className="topbar">
        <div className="brand">
          <div className="brand__logoWrap" aria-hidden="true">
            <img className="brand__logo" src="/rs.png" alt="Rīgas satiksme" />
          </div>
         <div className="brand__title">
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <strong>GTFS reāllaika datu monitorings</strong>

            {loading ? (
              <span className="badge badge--warn">Ielādē…</span>
            ) : error ? (
              <span className="badge badge--bad">Kļūda</span>
            ) : (
              <span className="badge badge--ok">Gatavs</span>
            )}
          </div>
        </div>

        </div>

        <div className="actions">
          <div className="field field--theme">
            <select className="select" value={theme} onChange={(e) => setTheme(e.target.value as any)}>
              <option value="dark">Tumšais</option>
              <option value="light">Gaišais</option>
            </select>
          </div>

          {tab !== "compare" && (
            <>
              <span className="badge">{source}</span>
              <span className="badge">Atjauno: {status?.refresh_seconds ?? 30}s</span>
              <button className="btn btn--primary" onClick={() => loadTab(tab, { forceRefresh: true })} disabled={loading}>
                Atjaunot
              </button>
              <label className="badge" style={{ cursor: "pointer" }}>
                <input
                  type="checkbox"
                  checked={autoAtjaunot}
                  onChange={(e) => setAutoAtjaunot(e.target.checked)}
                  style={{ marginRight: 8 }}
                />
                Automātiska atsvaidzināšana
              </label>
            </>
          )}
        </div>
      </div>

      <div className="grid">
        {/* Left */}
        <div className="card">
          <div className="card__hd">
            <div className="card__title">
              <h2>Vadības elementi</h2>
              <p>Avoti, filtri un režīmi</p>
            </div>
            {loading ? <span className="badge badge--warn">Ielāde…</span> : <span className="badge badge--ok">Gatavs</span>}
          </div>

          <div className="card__bd">
            {tab !== "compare" ? (
              <>
                <div className="row">
                  <div className="field">
                    <div className="label">Datu avots</div>
                    <select className="select" value={source} onChange={(e) => setSource(e.target.value)}>
                      {sources.map((s) => (
                        <option key={s.name} value={s.name}>
                          {s.name}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>

                <div className="hr" />

                <div className="kpis">
                  <div className="kpi">
                    <div className="kpi__label">Plūsmas galvene (vietējais laiks)</div>
                    <div className="kpi__value" style={{ fontSize: 14, fontFamily: "var(--mono)" }}>
                      {status?.feed_header_timestamp_local ? fmt(status.feed_header_timestamp_local) : "—"}
                    </div>
                  </div>
                  <div className="kpi">
                    <div className="kpi__label">Pēdējā atjaunošana (UTC)</div>
                    <div className="kpi__value" style={{ fontSize: 14, fontFamily: "var(--mono)" }}>
                      {status?.last_refresh_utc ? fmt(status.last_refresh_utc) : "—"}
                    </div>
                  </div>
                </div>

                <div className="hr" />

                <div className="alert">
                  <p className="alert__title">Ierakstu skaits</p>
                  <p className="alert__text" style={{ fontFamily: "var(--mono)" }}>
                    {countsText}
                  </p>
                </div>

                <div className="hr" />

                <div className="alert">
                  <p className="alert__title">Statiskie GTFS</p>
                  <p className="alert__text">
                    {status?.static_gtfs?.loaded
                      ? `Lejuplādēts (stops=${status.static_gtfs.counts?.stops ?? 0}, routes=${status.static_gtfs.counts?.routes ?? 0}, trips=${status.static_gtfs.counts?.trips ?? 0})`
                      : "Nav lejuplādēts (set STATIC_GTFS_PATH in backend env)"}
                  </p>
                </div>

                <div className="hr" />

                <div className="alert">
                  <p className="alert__title">PostgreSQL</p>
                  <p className="alert__text">
                    {!status?.db?.enabled
                      ? "Atspējots"
                      : status?.db?.connected
                      ? `Pieslēgts · pēdējais DB snapshot ${status.db.last_received_at ? fmt(status.db.last_received_at) : "—"}`
                      : `Nav pieejams: ${status?.db?.error ?? "nezināma kļūda"}`}
                  </p>
                  {status?.db_last_store ? (
                    <p className="alert__text" style={{ fontFamily: "var(--mono)", marginTop: 6 }}>
                      last_store={status.db_last_store.ok ? "ok" : "error"}
                      {status.db_last_store.snapshot_id ? ` · snapshot_id=${status.db_last_store.snapshot_id}` : ""}
                    </p>
                  ) : null}
                </div>

                <div className="hr" />

                <div className="alert">
                  <p className="alert__title">Failu lejuplāde</p>
                  <p className="alert__text" style={{ marginBottom: 10 }}>
                  
                  </p>
                  <div className="row" style={{ gap: 10, flexWrap: "wrap" }}>
                    <button
                      className="btn btn--primary"
                      type="button"
                      disabled={loading}
                      onClick={() => downloadFile(`/api/download/workbook.xlsx?source=${encodeURIComponent(source)}`, `gtfs_rt_${source}.xlsx`)}
                    >
                      Excel darbgrāmata (.xlsx)
                    </button>
                    <button
                      className="btn btn--ghost"
                      type="button"
                      disabled={loading}
                      onClick={() => downloadFile(`/api/download/anomalies.csv?source=${encodeURIComponent(source)}`, `anomalies_${source}.csv`)}
                      title="Anomāliju žurnāls (pazuduši stop_time/coords u.c.)"
                    >
                      anomalies.csv ({status?.counts?.anomalies ?? 0})
                    </button>
                  </div>

                  <div className="hr" />

                  <div className="row" style={{ gap: 10, flexWrap: "wrap" }}>
                    <button
                      className="btn btn--ghost"
                      type="button"
                      disabled={loading}
                      onClick={() =>
                        downloadFile(
                          `/api/download/gtfs_trip_updates.csv?source=${encodeURIComponent(source)}`,
                          `gtfs_trip_updates_${source}.csv`
                        )
                      }
                    >
                      trip_updates.csv ({status?.counts?.trip_updates ?? 0})
                    </button>
                    <button
                      className="btn btn--ghost"
                      type="button"
                      disabled={loading}
                      onClick={() =>
                        downloadFile(
                          `/api/download/gtfs_stop_time_updates.csv?source=${encodeURIComponent(source)}`,
                          `gtfs_stop_time_updates_${source}.csv`
                        )
                      }
                    >
                      stop_time_updates.csv ({status?.counts?.stop_time_updates ?? 0})
                    </button>
                    <button
                      className="btn btn--ghost"
                      type="button"
                      disabled={loading}
                      onClick={() =>
                        downloadFile(
                          `/api/download/gtfs_vehicle_positions.csv?source=${encodeURIComponent(source)}`,
                          `gtfs_vehicle_positions_${source}.csv`
                        )
                      }
                    >
                      vehicle_positions.csv ({status?.counts?.vehicle_positions ?? 0})
                    </button>
                    <button
                      className="btn btn--ghost"
                      type="button"
                      disabled={loading}
                      onClick={() =>
                        downloadFile(`/api/download/gtfs_alerts.csv?source=${encodeURIComponent(source)}`, `gtfs_alerts_${source}.csv`)
                      }
                    >
                      alerts.csv ({status?.counts?.alerts ?? 0})
                    </button>
                  </div>
                </div>
              </>
            ) : (
              <>
                <div className="row">
                  <div className="field">
                    <div className="label">A</div>
                    <select className="select" value={compareA} onChange={(e) => setCompareA(e.target.value)}>
                      {sources.map((s) => (
                        <option key={s.name} value={s.name}>
                          {s.name}
                        </option>
                      ))}
                    </select>
                  </div>

                  <div className="field">
                    <div className="label">B</div>
                    <select className="select" value={compareB} onChange={(e) => setCompareB(e.target.value)}>
                      {sources.map((s) => (
                        <option key={s.name} value={s.name}>
                          {s.name}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>

                <div className="card__ft" style={{ paddingLeft: 0, paddingRight: 0 }}>
                  <button className="btn btn--primary" onClick={() => loadTab("compare", { forceRefresh: true })} disabled={loading}>
                    Salīdzināt
                  </button>
                </div>
              </>
            )}
          </div>

          <div className="card__ft" style={{ justifyContent: "flex-start", flexWrap: "wrap" }}>
            <div className="pill-tabs">
              {(["trips", "stops", "vehicles", "map", "trace", "alerts", "anomalies", "metrics", "compare", "monitor", "history", "analytics"] as const).map((t) => (
                <button key={t} className="pill" aria-selected={tab === t} onClick={() => setTab(t)} type="button">
                  {tabLabel(t)}
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* Right */}
        <div>
          {error && (
            <div className="alert alert--bad" style={{ marginTop: 0 }}>
              <p className="alert__title">Kļūda</p>
              <p className="alert__text" style={{ fontFamily: "var(--mono)", whiteSpace: "pre-wrap" }}>
                {error}
              </p>
            </div>
          )}

          
          {(tabFilterSupport[tab]?.stop || tabFilterSupport[tab]?.route || tabFilterSupport[tab]?.trip || tabFilterSupport[tab]?.vehicle || tabFilterSupport[tab]?.text) && (
          <div className="card card--main" style={{ marginTop: 0, marginBottom: 12 }}>
            <div className="card__hd">
              <div className="card__title">
                <h2>FILTRI</h2>
                <p>Rādām {filteredIeraksti.length} / {rows.length}</p>
              </div>
              <button
                className="btn btn--ghost"
                type="button"
                onClick={clearGlobalFilters}
              >
                Notīrīt filtrus
              </button>
            </div>
            <div className="card__bd">
              <div className="row" style={{ gap: 10, flexWrap: "wrap" }}>
                {tabFilterSupport[tab]?.stop && (
                <div className="field" style={{ minWidth: 180 }}>
                  <div className="label">Pieturas ID</div>
                  <input className="input" placeholder="stop_id" value={stopIdFiltrs} onChange={(e) => setStopIdFiltrs(e.target.value)} />
                </div>
                )}
                {tabFilterSupport[tab]?.route && (
                <div className="field" style={{ minWidth: 180 }}>
                  <div className="label">Maršruta ID</div>
                  <input className="input" placeholder="route_id" value={routeIdFiltrs} onChange={(e) => setRouteIdFiltrs(e.target.value)} />
                </div>
                )}
                {tabFilterSupport[tab]?.trip && (
                <div className="field" style={{ minWidth: 180 }}>
                  <div className="label">Reisa ID</div>
                  <input className="input" placeholder="trip_id" value={tripIdFiltrs} onChange={(e) => setTripIdFiltrs(e.target.value)} />
                </div>
                )}
                {tabFilterSupport[tab]?.vehicle && (
                <div className="field" style={{ minWidth: 180 }}>
                  <div className="label">Transportlīdzekļa ID</div>
                  <input className="input" placeholder="vehicle_id" value={vehicleIdFiltrs} onChange={(e) => setVehicleIdFiltrs(e.target.value)} />
                </div>
                )}
                {tabFilterSupport[tab]?.text && (
                <div className="field" style={{ minWidth: 220, flex: 1 }}>
                  <div className="label">Meklēt</div>
                  <input className="input" placeholder="brīvais teksts…" value={freeTextFiltrs} onChange={(e) => setFreeTextFiltrs(e.target.value)} />
                </div>
                )}
              </div>
            </div>
          </div>
          )}

{tab === "history" && (
            <div className="card" style={{ marginTop: error ? 12 : 0, marginBottom: 12 }}>
              <div className="card__hd">
                <div className="card__title">
                  <h2>Vēsture no PostgreSQL</h2>
                  <p>Server-side vaicājums pēc laika intervāla un ID filtriem</p>
                </div>
                <span className="badge">{rows.length} rindas</span>
              </div>
              <div className="card__bd">
                <div className="row" style={{ gap: 10, flexWrap: "wrap" }}>
                  <div className="field" style={{ minWidth: 220 }}>
                    <div className="label">Datu kopa</div>
                    <select className="select" value={historyDataset} onChange={(e) => setHistoryDataset(e.target.value)}>
                      <option value="all">Visas kopas</option>
                      <option value="trip_updates">trip_updates</option>
                      <option value="stop_time_updates">stop_time_updates</option>
                      <option value="vehicle_positions">vehicle_positions</option>
                      <option value="alerts">alerts</option>
                    </select>
                  </div>
                  <div className="field" style={{ minWidth: 220 }}>
                    <div className="label">No (pārlūka lokālais laiks)</div>
                    <input className="input" type="datetime-local" value={historyStart} onChange={(e) => setHistoryStart(e.target.value)} />
                  </div>
                  <div className="field" style={{ minWidth: 220 }}>
                    <div className="label">Līdz (pārlūka lokālais laiks)</div>
                    <input className="input" type="datetime-local" value={historyEnd} onChange={(e) => setHistoryEnd(e.target.value)} />
                  </div>
                  <div className="field" style={{ minWidth: 140 }}>
                    <div className="label">Limits</div>
                    <select className="select" value={historyLimit} onChange={(e) => setHistoryLimit(parseInt(e.target.value, 10))}>
                      {[500, 1000, 2000, 5000, 10000].map((v) => (
                        <option key={v} value={v}>{v}</option>
                      ))}
                    </select>
                  </div>
                </div>

                <div className="row" style={{ gap: 10, flexWrap: "wrap", marginTop: 12 }}>
                  <button className="btn btn--primary" type="button" disabled={loading} onClick={() => loadTab("history", { forceRefresh: false })}>
                    Meklēt DB
                  </button>
                  <button
                    className="btn btn--ghost"
                    type="button"
                    disabled={loading || !stopIdFiltrs.trim()}
                    title={stopIdFiltrs.trim() ? "Meklēt tikai izvēlēto stop_id no DB" : "Ievadi stop_id augšējā filtrā"}
                    onClick={() => {
                      setHistoryDataset("stop_time_updates");
                      setTimeout(() => loadTab("history", { forceRefresh: false }), 0);
                    }}
                  >
                    Meklēt tikai stop DB
                  </button>
                  <button
                    className="btn btn--ghost"
                    type="button"
                    disabled={loading || !vehicleIdFiltrs.trim()}
                    title={vehicleIdFiltrs.trim() ? "Meklēt tikai izvēlēto vehicle_id no DB" : "Ievadi vehicle_id augšējā filtrā"}
                    onClick={() => {
                      setHistoryDataset("vehicle_positions");
                      setTimeout(() => loadTab("history", { forceRefresh: false }), 0);
                    }}
                  >
                    Meklēt tikai vehicle DB
                  </button>
                  <button
                    className="btn btn--ghost"
                    type="button"
                    onClick={() => {
                      setHistoryStart(hoursAgoLocalValue(6));
                      setHistoryEnd(nowLocalValue());
                      setHistoryDataset("all");
                      setHistoryLimit(2000);
                      setTimeout(() => loadTab("history", { forceRefresh: false }), 0);
                    }}
                  >
                    Pēdējās 6h
                  </button>
                  <button
                    className="btn btn--ghost"
                    type="button"
                    onClick={() => {
                      setHistoryStart(hoursAgoLocalValue(24));
                      setHistoryEnd(nowLocalValue());
                      setTimeout(() => loadTab("history", { forceRefresh: false }), 0);
                    }}
                  >
                    Pēdējās 24h
                  </button>
                </div>

                <p style={{ marginTop: 10, color: "var(--muted)", fontSize: 12 }}>
                  Stop ID / Route ID / Trip ID / Vehicle ID filtri augšā tiek iekļauti pašā DB vaicājumā. Ātrās pogas izmanto DB history un fokusējas uz stop_time_updates vai vehicle_positions.
                </p>

                {historySummary.length > 0 ? <Table rows={historySummary} title="Vēstures kopsavilkums pa datasetiem" /> : null}

                {historyStopDetails ? (
                  <>
                    <Table rows={[historyStopDetails.summary]} title={`Pieturas DB kopsavilkums — stop_id=${historyStopDetails.summary?.stop_id ?? stopIdFiltrs}`} />
                    <Table rows={historyStopDetails.gaps ?? []} title="Pieturas gap periodi no DB snapshotiem" />
                    <Table rows={historyStopDetails.rows ?? []} title="Pieturas ieraksti no DB" />
                  </>
                ) : null}

                {historyVehicleDetails ? (
                  <>
                    <Table rows={[historyVehicleDetails.summary]} title={`Transportlīdzekļa DB kopsavilkums — vehicle_id=${historyVehicleDetails.summary?.vehicle_id ?? vehicleIdFiltrs}`} />
                    <Table rows={historyVehicleDetails.gaps ?? []} title="Transportlīdzekļa gap periodi no DB snapshotiem" />
                    <Table rows={historyVehicleDetails.rows ?? []} title="Transportlīdzekļa ieraksti no DB" />
                  </>
                ) : null}
              </div>
            </div>
          )}

          {tab === "analytics" && (
            <div className="card" style={{ marginTop: error ? 12 : 0, marginBottom: 12 }}>
              <div className="card__hd">
                <div className="card__title">
                  <h2>Analītikas panelis</h2>
                  <p>Datu pārrāvumi, svaigums, aktivitāte un datu kvalitāte no PostgreSQL</p>
                </div>
                <span className="badge">{analyticsData ? `${analyticsData.hours}h logs` : "nav datu"}</span>
              </div>
              <div className="card__bd">
                <div className="row" style={{ gap: 10, flexWrap: "wrap" }}>
                  <div className="field" style={{ minWidth: 180 }}>
                    <div className="label">Periods</div>
                    <select className="select" value={analyticsHours} onChange={(e) => setAnalyticsHours(parseInt(e.target.value, 10))}>
                      {[1, 3, 6, 12, 24, 48, 72, 168].map((h) => (
                        <option key={h} value={h}>pēdējās {h}h</option>
                      ))}
                    </select>
                  </div>
                  <button className="btn btn--primary" type="button" disabled={loading} onClick={() => loadTab("analytics", { forceRefresh: false })}>
                    Pārrēķināt
                  </button>
                </div>

                {analyticsData ? (
                  <>
                    <div className="hr" />
                    <div className="kpis">
                      <div className="kpi">
                        <div className="kpi__label">Pēdējais DB momentuzņēmums</div>
                        <div className="kpi__value" style={{ fontSize: 14 }}>{analyticsData.kpis?.last_received_at ? fmt(analyticsData.kpis.last_received_at) : "—"}</div>
                      </div>
                      <div className="kpi">
                        <div className="kpi__label">Sekundes kopš pēdējā momentuzņēmuma</div>
                        <div className="kpi__value">{fmt(analyticsData.kpis?.seconds_since_last_snapshot)}</div>
                      </div>
                      <div className="kpi">
                        <div className="kpi__label">Pēdējās plūsmas galvenes nobīde (s)</div>
                        <div className="kpi__value">{fmt(analyticsData.kpis?.last_header_lag_seconds)}</div>
                      </div>
                      <div className="kpi">
                        <div className="kpi__label">Aktīvie vehicle / trip / stop</div>
                        <div className="kpi__value" style={{ fontSize: 14 }}>
                          {fmt(analyticsData.kpis?.active_vehicles_last_hour)} / {fmt(analyticsData.kpis?.active_trips_last_hour)} / {fmt(analyticsData.kpis?.active_stops_last_hour)}
                        </div>
                      </div>
                      <div className="kpi">
                        <div className="kpi__label">Sagaidāmais pārrāvums / robežpārkāpums (s)</div>
                        <div className="kpi__value" style={{ fontSize: 14 }}>
                          {fmt(analyticsData.kpis?.expected_gap_seconds)} / {fmt(analyticsData.kpis?.gap_breach_threshold_seconds)}
                        </div>
                      </div>
                      <div className="kpi">
                        <div className="kpi__label">Mediāna / maks. pārrāvums (s)</div>
                        <div className="kpi__value" style={{ fontSize: 14 }}>
                          {fmt(analyticsData.kpis?.median_gap_seconds)} / {fmt(analyticsData.kpis?.max_gap_seconds)}
                        </div>
                      </div>
                    </div>

                    <BarListCard rows={analyticsData.hourly_coverage ?? []} title="Stundu pieejamība (%)" labelKey="hour_bucket_local" valueKey="uptime_percent" suffix="%" />
                    <BarListCard rows={analyticsData.top_routes ?? []} title="Aktīvākie maršruti" labelKey="route_short_name" fallbackLabelKeys={["route_id"]} valueKey="row_count" />
                    <BarListCard rows={analyticsData.top_trips ?? []} title="Aktīvākie reisi" labelKey="trip_id" valueKey="row_count" />
                    <BarListCard rows={analyticsData.top_stops ?? []} title="Aktīvākās pieturas" labelKey="stop_name" fallbackLabelKeys={["stop_id"]} valueKey="row_count" />

                    <Table rows={[analyticsData.quality_checks]} title="Kvalitātes pārbaudes" />
                    <Table rows={analyticsData.recent_gaps ?? []} title="Lielākie plūsmas pārrāvumi" />
                    <Table rows={analyticsData.hourly_coverage ?? []} title="Stundu pārklājums" />
                    <Table rows={analyticsData.top_routes ?? []} title="Aktīvākie maršruti" />
                    <Table rows={analyticsData.top_trips ?? []} title="Aktīvākie reisi" />
                    <Table rows={analyticsData.top_stops ?? []} title="Aktīvākās pieturas" />
                    <Table rows={analyticsData.silent_vehicles ?? []} title="Nekustīgi vai klusējoši transportlīdzekļi" />
                  </>
                ) : (
                  <p style={{ marginTop: 12, color: "var(--muted)" }}>Analītikas dati vēl nav ielādēti.</p>
                )}
              </div>
            </div>
          )}

{tab === "compare" && compare && (
            <div className="card" style={{ marginTop: error ? 12 : 0 }}>
              <div className="card__hd">
                <div className="card__title">
                  <h2>Salīdzinājums</h2>
                  <p>
                    {compare.source_a} vs {compare.source_b}
                  </p>
                </div>
                <span className="badge badge--warn">diff</span>
              </div>

              <div className="card__bd">
                <div className="alert">
                  <p className="alert__title">Pēdējā atjaunošana UTC</p>
                  <p className="alert__text" style={{ fontFamily: "var(--mono)" }}>
                    {compare.source_a}={compare.last_refresh_utc[compare.source_a] ?? "—"} · {compare.source_b}=
                    {compare.last_refresh_utc[compare.source_b] ?? "—"}
                  </p>
                </div>

                <div className="hr" />

                <div className="kpis">
                  <div className="kpi">
                    <div className="kpi__label">{compare.source_a} ierakstu skaits</div>
                    <div className="kpi__value" style={{ fontSize: 12, fontFamily: "var(--mono)" }}>
                      {Object.entries(compare.counts[compare.source_a]).map(([k, v]) => `${k}=${v}`).join(", ")}
                    </div>
                  </div>
                  <div className="kpi">
                    <div className="kpi__label">{compare.source_b} ierakstu skaits</div>
                    <div className="kpi__value" style={{ fontSize: 12, fontFamily: "var(--mono)" }}>
                      {Object.entries(compare.counts[compare.source_b]).map(([k, v]) => `${k}=${v}`).join(", ")}
                    </div>
                  </div>
                </div>

                <div className="hr" />

                <div className="alert alert--warn">
                  <p className="alert__title">Trūkstošie StopTimeUpdate notikumi (trip+stop+seq)</p>
                  <p className="alert__text">
                    {Object.entries(compare.missing_stop_time_events).map(([k, v]) => (
                      <span key={k} style={{ display: "inline-block", marginRight: 14, fontFamily: "var(--mono)" }}>
                        {k}: <b>{v}</b>
                      </span>
                    ))}
                  </p>
                </div>

                <div className="hr" />

                <div className="alert alert--warn">
                  <p className="alert__title">Trūkstošās pieturas (stop_id)</p>
                  <p className="alert__text">
                    {Object.entries(compare.missing_stop_ids).map(([k, v]) => (
                      <span key={k} style={{ display: "inline-block", marginRight: 14, fontFamily: "var(--mono)" }}>
                        {k}: <b>{v}</b>
                      </span>
                    ))}
                  </p>
                </div>

                <Table rows={eventsSamples} title="Piemēri: notikumi" />
                <Table rows={stopsSamples} title="Piemēri: pieturas" />
              </div>
            </div>
          )}

          {tab !== "compare" && tab === "anomalies" && (
            <div className="card" style={{ marginTop: error ? 12 : 0 }}>
              <div className="card__hd">
                <div className="card__title">
                  <h2>Anomāliju filtri</h2>
                  <p>Rādām {rows.length} / {rows.length}</p>
                </div>
                <span className="badge badge--warn">monitorings</span>
              </div>

              <div className="card__bd">
                <div className="row" style={{ gap: 10, flexWrap: "wrap" }}>
                  <div className="field" style={{ minWidth: 240 }}>
                    <div className="label">Tips</div>
                    <select className="select" value={anomalyKind} onChange={(e) => setAnomalyKind(e.target.value)}>
                      <option value="">All</option>
                      {anomalyKinds.map((k) => (
                        <option key={k} value={k}>
                          {k}
                        </option>
                      ))}
                    </select>
                  </div>

                  <div className="field" style={{ minWidth: 280, flex: 1 }}>
                    <div className="label">Meklēt</div>
                    <input
                      className="input"
                      placeholder="route / stop / trip / note..."
                      value={anomalyMeklēt}
                      onChange={(e) => setAnomalyMeklēt(e.target.value)}
                    />
                  </div>
                </div>
              </div>

              <div className="card__ft" style={{ justifyContent: "space-between" }}>
                <button
                  className="btn btn--ghost"
                  type="button"
                  onClick={() => {
                    setAnomalyKind("");
                    setAnomalyMeklēt("");
                  }}
                >
                  Atiestatīt filtrus
                </button>
                <button
                  className="btn btn--danger"
                  type="button"
                  disabled={loading}
                  onClick={async () => {
                    await fetchJson(`/api/anomalies/clear?source=${encodeURIComponent(source)}`, { method: "POST" });
                    setAnomalyKind("");
                    setAnomalyMeklēt("");
                    await loadTab("anomalies", { forceRefresh: false });
                  }}
                >
                  Notīrīt anomālijas (memory)
                </button>
              </div>
            </div>
          )}

          {tab === "map" && (
            <div className="card" style={{ marginTop: error ? 12 : 0 }}>
              <div className="card__hd">
                <div className="card__title">
                  <h2>Map</h2>
                  <p>Reāllaika transportlīdzekļi + anomālijas (noklikšķini, lai skatītu detalizēti)</p>
                </div>
                <span className="badge badge--warn">live</span>
              </div>
              <div className="card__bd" style={{ padding: 12 }}>
                <MapView
              apiBase={API_BASE}
              source={source}
              refreshSeconds={status?.refresh_seconds ?? 30}
              stopIdFilter={stopIdFiltrs}
              routeIdFilter={routeIdFiltrs}
              tripIdFilter={tripIdFiltrs}
              vehicleIdFilter={vehicleIdFiltrs}
              freeTextFilter={freeTextFiltrs}
            />
              </div>
            </div>
          )}

          {tab === "trace" && (
            <div className="card" style={{ marginTop: error ? 12 : 0 }}>
              <div className="card__hd">
                <div className="card__title">
                  <h2>Trace</h2>
                  <p>Lookup by vehicle_id / trip_id / route_id + direction_id and view stop timeline</p>
                </div>
                <span className="badge badge--ok">rīki</span>
              </div>
              <div className="card__bd" style={{ padding: 12 }}>
                <TraceView apiBase={API_BASE} source={source} />
              </div>
            </div>
          )}

          
          {tab === "monitor" && (
            <div className="card" style={{ marginTop: error ? 12 : 0, marginBottom: 12 }}>
              <div className="card__hd">
                <div className="card__title">
                  <h2>Monitorings</h2>
                  <p>Feed kvalitātes notikumi (piem., trūkstošas kolonnas, tukši lauki, 0 ieraksti)</p>
                </div>
                <span className="badge">{filteredIeraksti.length ? `${filteredIeraksti.length} / ${monitorEvents.length} notik.` : "Nav notikumu"}</span>
              </div>
              <div className="card__bd">
                <div className="row" style={{ gap: 10, flexWrap: "wrap" }}>
                  <div className="field" style={{ minWidth: 160 }}>
                    <div className="label">Periods</div>
                    <select className="select" value={monitorHours} onChange={(e) => setMonitorHours(parseInt(e.target.value, 10))} onBlur={() => loadTab("monitor")}>
                      {[1, 3, 6, 12, 24, 48, 72, 168].map((h) => (
                        <option key={h} value={h}>
                          pēdējās {h}h
                        </option>
                      ))}
                    </select>
                  </div>

                  <div className="field" style={{ minWidth: 220 }}>
                    <div className="label">Datu kopa</div>
                    <select className="select" value={monitorDataset} onChange={(e) => setMonitorDataset(e.target.value)} onBlur={() => loadTab("monitor")}>
                      <option value="">Visas</option>
                      <option value="trip_updates">trip_updates</option>
                      <option value="stop_time_updates">stop_time_updates</option>
                      <option value="vehicle_positions">vehicle_positions</option>
                      <option value="alerts">alerts</option>
                    </select>
                  </div>

                  <div className="field" style={{ minWidth: 220 }}>
                    <div className="label">Notikuma tips</div>
                    <select className="select" value={monitorEventType} onChange={(e) => setMonitorEventType(e.target.value)} onBlur={() => loadTab("monitor")}>
                      <option value="">Visi</option>
                      <option value="dataset_empty">dataset_empty</option>
                      <option value="column_quality">column_quality</option>
                      <option value="info">info</option>
                    </select>
                  </div>

                  <div className="field" style={{ minWidth: 260, flex: 1 }}>
                    <div className="label">Meklēt notikumos</div>
                    <input
                      className="input"
                      placeholder="piem.: stop_time_updates, missing_columns, stop_id..."
                      value={monitorQuery}
                      onChange={(e) => setMonitorQuery(e.target.value)}
                    />
                  </div>
                </div>

                <div className="hr" />

                <div className="row" style={{ gap: 10, flexWrap: "wrap" }}>
                  <button
                    className="btn btn--primary"
                    type="button"
                    disabled={loading}
                    onClick={() => loadTab("monitor", { forceRefresh: false })}
                  >
                    Atjaunot
                  </button>
                  <button
                    className="btn btn--ghost"
                    type="button"
                    onClick={() => {
                      setMonitorHours(24);
                      setMonitorDataset("");
                      setMonitorEventType("");
                      setMonitorQuery("");
                      setTimeout(() => loadTab("monitor", { forceRefresh: false }), 0);
                    }}
                  >
                    Atiestatīt filtrus
                  </button>
                </div>

                <p style={{ marginTop: 10, color: "var(--muted)", fontSize: 12 }}>
                  Notikumi tiek rakstīti backendā mapē <span className="cell-mono">backend/data/monitor/</span> kā <span className="cell-mono">events_*.jsonl</span>.
                </p>
              </div>
            </div>
          )}

          {tab !== "compare" && tab !== "map" && tab !== "trace" && tab !== "analytics" && <Table rows={filteredIeraksti} title={`${title} — source=${source}`} columnLabels={tab === "monitor" ? monitorColumnLabels : undefined} onRowClick={tab === "monitor" ? (r) => { setSelectedRow(r); setShowRowModal(true); } : undefined} />}
        </div>
      </div>

      <footer
        style={{
          marginTop: 18,
          padding: "10px 0",
          textAlign: "center",
          fontSize: 12,
          color: "var(--muted)",
        }}
      >
        © {new Date().getFullYear()} Izstrādāja: Karīna Apsīte-Auniņa
      </footer>
      {showRowModal && selectedRow && (
        <div
          role="dialog"
          aria-modal="true"
          onClick={() => {
            setShowRowModal(false);
            setSelectedRow(null);
          }}
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(0,0,0,0.45)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            padding: 20,
            zIndex: 9999,
          }}
        >
          <div
            className="card"
            onClick={(e) => e.stopPropagation()}
            style={{ width: "min(980px, 96vw)", maxHeight: "86vh", overflow: "auto" }}
          >
            <div className="card__hd">
              <div className="card__title">
                <h2>Rindas detaļas</h2>
                <p style={{ margin: 0, color: "var(--muted)" }}></p>
              </div>
              <button
                className="btn btn--ghost"
                type="button"
                onClick={() => {
                  setShowRowModal(false);
                  setSelectedRow(null);
                }}
              >
                Aizvērt
              </button>
            </div>
            <div className="card__bd">
              <pre style={{ margin: 0, fontSize: 12, whiteSpace: "pre-wrap" }} className="cell-mono">
                {JSON.stringify(selectedRow, null, 2)}
              </pre>
            </div>
          </div>
        </div>
      )}

    </div>
  );
}