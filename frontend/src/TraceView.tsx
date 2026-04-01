import React, { useMemo, useState } from "react";

type TraceResponse = {
  source: string;
  query: any;
  summary?: any;
  matched: {
    vehicles: any[];
    trips: any[];
  };
  resolved_trip_id?: string | null;
  timeline?: {
    ok: boolean;
    meta?: any;
    timeline?: any[];
  } | null;
  positions?: any[];
  rows?: any[];
};

function toQS(params: Record<string, any>): string {
  const usp = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v === undefined || v === null) return;
    const s = String(v).trim();
    if (!s) return;
    usp.set(k, s);
  });
  return usp.toString();
}

function fmt(v: any): string {
  if (v === undefined || v === null) return "—";
  if (typeof v === "number" && !Number.isFinite(v)) return "—";
  const s = String(v).trim();
  return ["", "nan", "none", "null"].includes(s.toLowerCase()) ? "—" : s;
}

function shortId(v: any, max = 24): string {
  const s = fmt(v);
  if (s.length <= max) return s;
  return s.slice(0, max - 1) + "…";
}

function toLocalInputValue(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function formatLocalDateTime(v: string): string {
  if (!v) return "";
  const dt = new Date(v);
  if (Number.isNaN(dt.getTime())) return v;
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${pad(dt.getDate())}.${pad(dt.getMonth() + 1)}.${dt.getFullYear()} ${pad(dt.getHours())}:${pad(dt.getMinutes())}:${pad(dt.getSeconds())}`;
}

function defaultRange(hours: number) {
  const end = new Date();
  const start = new Date(end.getTime() - hours * 60 * 60 * 1000);
  return { start: toLocalInputValue(start), end: toLocalInputValue(end) };
}

export default function TraceView({ apiBase, source }: { apiBase: string; source: string }) {
  const initial = defaultRange(6);
  const [vehicleId, setVehicleId] = useState("");
  const [tripId, setTripId] = useState("");
  const [routeId, setRouteId] = useState("");
  const [directionId, setDirectionId] = useState("");
  const [startTime, setStartTime] = useState(initial.start);
  const [endTime, setEndTime] = useState(initial.end);
  const [limit, setLimit] = useState("500");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<TraceResponse | null>(null);

  const canSearch = useMemo(() => {
    return Boolean(vehicleId.trim() || tripId.trim() || routeId.trim() || directionId.trim());
  }, [vehicleId, tripId, routeId, directionId]);

  async function run(overrides?: Partial<Record<string, string>>) {
    setLoading(true);
    setError(null);
    try {
      const params: Record<string, any> = {
        source,
        vehicle_id: overrides?.vehicle_id ?? vehicleId,
        trip_id: overrides?.trip_id ?? tripId,
        route_id: overrides?.route_id ?? routeId,
        direction_id: overrides?.direction_id ?? directionId,
        start_time: overrides?.start_time ?? startTime,
        end_time: overrides?.end_time ?? endTime,
        limit: overrides?.limit ?? limit,
      };
      const resp = await fetch(`${apiBase}/api/trace?${toQS(params)}`);
      if (!resp.ok) {
        const txt = await resp.text();
        throw new Error(txt || `HTTP ${resp.status}`);
      }
      const json = (await resp.json()) as TraceResponse;
      setData(json);
    } catch (e: any) {
      setError(e?.message || String(e));
    } finally {
      setLoading(false);
    }
  }

  function applyLastHours(hours: number) {
    const r = defaultRange(hours);
    setStartTime(r.start);
    setEndTime(r.end);
    if (canSearch) {
      void run({ start_time: r.start, end_time: r.end });
    }
  }

  const rows = data?.rows ?? [];
  const positions = data?.positions ?? [];
  const timelineRows = data?.timeline?.timeline ?? [];

  return (
    <div className="vstack">
      <div className="panel">
        <div className="panel-title">Izsekošana no DB</div>
        <div className="muted" style={{ marginBottom: 12 }}>
          Meklē vēsturiskos ierakstus no PostgreSQL pēc <b>vehicle_id</b>, <b>trip_id</b>, <b>route_id</b> un <b>direction_id</b> ar laika filtru.
        </div>

        <div className="row">
          <label className="field" style={{ minWidth: 210 }}>
            <span className="label">vehicle_id</span>
            <input className="input" value={vehicleId} onChange={(e) => setVehicleId(e.target.value)} placeholder="piem. 78800" />
          </label>
          <label className="field" style={{ minWidth: 260 }}>
            <span className="label">trip_id</span>
            <input className="input" value={tripId} onChange={(e) => setTripId(e.target.value)} placeholder="piem. TRAM11-..." />
          </label>
          <label className="field" style={{ minWidth: 180 }}>
            <span className="label">route_id</span>
            <input className="input" value={routeId} onChange={(e) => setRouteId(e.target.value)} placeholder="piem. riga_tram_11" />
          </label>
          <label className="field" style={{ minWidth: 140 }}>
            <span className="label">direction_id</span>
            <input className="input" value={directionId} onChange={(e) => setDirectionId(e.target.value)} placeholder="0 vai 1" />
          </label>
        </div>

        <div className="row" style={{ marginTop: 10 }}>
          <label className="field" style={{ minWidth: 220 }}>
            <span className="label">No</span>
            <input className="input" type="datetime-local" value={startTime} onChange={(e) => setStartTime(e.target.value)} />
          </label>
          <label className="field" style={{ minWidth: 220 }}>
            <span className="label">Līdz</span>
            <input className="input" type="datetime-local" value={endTime} onChange={(e) => setEndTime(e.target.value)} />
          </label>
          <label className="field" style={{ minWidth: 120 }}>
            <span className="label">Limits</span>
            <input className="input" value={limit} onChange={(e) => setLimit(e.target.value)} />
          </label>
          <button className="btn" disabled={loading || !canSearch} onClick={() => run()}>{loading ? "Meklē…" : "Meklēt DB"}</button>
          <button className="btn secondary" onClick={() => applyLastHours(6)}>Pēdējās 6h</button>
          <button className="btn secondary" onClick={() => applyLastHours(24)}>Pēdējās 24h</button>
        </div>

        {error ? <div className="badge" style={{ borderColor: "#ef4444", color: "#ef4444", marginTop: 12 }}>Kļūda: {error}</div> : null}
      </div>

      {data ? (
        <div className="content-grid">
          <div className="panel">
            <div className="panel-title">Kopsavilkums</div>
            <div className="row" style={{ gap: 8, flexWrap: "wrap" }}>
              <div className="badge">ieraksti: <b>{fmt(data.summary?.row_count)}</b></div>
              <div className="badge">snapshoti: <b>{fmt(data.summary?.snapshot_count)}</b></div>
              <div className="badge">pirmais: <b>{fmt(formatLocalDateTime(data.summary?.first_recorded_at))}</b></div>
              <div className="badge">pēdējais: <b>{fmt(formatLocalDateTime(data.summary?.last_recorded_at))}</b></div>
              <div className="badge">trip: <b>{fmt(data.summary?.trip_count)}</b></div>
              <div className="badge">vehicle: <b>{fmt(data.summary?.vehicle_count)}</b></div>
            </div>
          </div>

          <div className="panel">
            <div className="panel-title">Atrasti transportlīdzekļi ({data.matched.vehicles.length})</div>
            {data.matched.vehicles.length === 0 ? <div className="muted">Nav atrasti.</div> : (
              <div className="table-wrap"><table className="table"><thead><tr><th>vehicle_id</th><th>label</th><th>route</th><th>trip_id</th><th>last_seen</th><th>hits</th></tr></thead><tbody>
                {data.matched.vehicles.map((r:any,i:number)=><tr key={i}><td>{fmt(r.vehicle_id)}</td><td>{fmt(r.vehicle_label)}</td><td>{fmt(r.route_short_name || r.route_id)}</td><td title={fmt(r.trip_id)}>{shortId(r.trip_id)}</td><td>{fmt(formatLocalDateTime(r.last_seen_at))}</td><td>{fmt(r.hit_count)}</td></tr>)}
              </tbody></table></div>
            )}
          </div>

          <div className="panel">
            <div className="panel-title">Atrasti reisi ({data.matched.trips.length})</div>
            {data.matched.trips.length === 0 ? <div className="muted">Nav atrasti.</div> : (
              <div className="table-wrap"><table className="table"><thead><tr><th>trip_id</th><th>route</th><th>dir</th><th>vehicle_id</th><th>last_seen</th><th>hits</th></tr></thead><tbody>
                {data.matched.trips.map((r:any,i:number)=><tr key={i}><td title={fmt(r.trip_id)}>{shortId(r.trip_id, 30)}</td><td>{fmt(r.route_short_name || r.route_id)}</td><td>{fmt(r.direction_id)}</td><td>{fmt(r.vehicle_id)}</td><td>{fmt(formatLocalDateTime(r.last_seen_at))}</td><td>{fmt(r.hit_count)}</td></tr>)}
              </tbody></table></div>
            )}
          </div>

          <div className="panel span-2">
            <div className="panel-title">Pozīciju vēsture ({positions.length})</div>
            {positions.length === 0 ? <div className="muted">Nav vehicle position ierakstu šim filtram.</div> : (
              <div className="table-wrap"><table className="table"><thead><tr><th>recorded_at</th><th>snapshot_id</th><th>vehicle_id</th><th>trip_id</th><th>route</th><th>lat</th><th>lon</th></tr></thead><tbody>
                {positions.map((r:any,i:number)=><tr key={i}><td>{fmt(formatLocalDateTime(r.recorded_at))}</td><td>{fmt(r.snapshot_id)}</td><td>{fmt(r.vehicle_id)}</td><td title={fmt(r.trip_id)}>{shortId(r.trip_id,28)}</td><td>{fmt(r.route_short_name || r.route_id)}</td><td>{fmt(r.latitude)}</td><td>{fmt(r.longitude)}</td></tr>)}
              </tbody></table></div>
            )}
          </div>

          <div className="panel span-2">
            <div className="panel-title">Pieturu / trip notikumi ({timelineRows.length})</div>
            {timelineRows.length === 0 ? <div className="muted">Nav stop_time_updates ierakstu šim filtram vai atrastajam reisam.</div> : (
              <div className="table-wrap"><table className="table"><thead><tr><th>recorded_at</th><th>snapshot_id</th><th>trip_id</th><th>vehicle_id</th><th>stop_id</th><th>stop_name</th><th>seq</th><th>arrival</th><th>departure</th></tr></thead><tbody>
                {timelineRows.map((r:any,i:number)=><tr key={i}><td>{fmt(formatLocalDateTime(r.recorded_at))}</td><td>{fmt(r.snapshot_id)}</td><td title={fmt(r.trip_id)}>{shortId(r.trip_id,28)}</td><td>{fmt(r.vehicle_id)}</td><td>{fmt(r.stop_id)}</td><td>{fmt(r.stop_name)}</td><td>{fmt(r.stop_sequence)}</td><td>{fmt(formatLocalDateTime(r.arrival_time))}</td><td>{fmt(formatLocalDateTime(r.departure_time))}</td></tr>)}
              </tbody></table></div>
            )}
          </div>

          <div className="panel span-2">
            <div className="panel-title">Visi atrastie ieraksti ({rows.length})</div>
            {rows.length === 0 ? <div className="muted">Nav datu.</div> : (
              <div className="table-wrap"><table className="table"><thead><tr><th>dataset</th><th>recorded_at</th><th>snapshot_id</th><th>entity_id</th><th>trip_id</th><th>vehicle_id</th><th>route</th><th>stop_id</th></tr></thead><tbody>
                {rows.map((r:any,i:number)=><tr key={i}><td>{fmt(r.dataset)}</td><td>{fmt(formatLocalDateTime(r.recorded_at))}</td><td>{fmt(r.snapshot_id)}</td><td title={fmt(r.entity_id)}>{shortId(r.entity_id, 34)}</td><td title={fmt(r.trip_id)}>{shortId(r.trip_id, 28)}</td><td>{fmt(r.vehicle_id)}</td><td>{fmt(r.route_short_name || r.route_id)}</td><td>{fmt(r.stop_id)}</td></tr>)}
              </tbody></table></div>
            )}
          </div>
        </div>
      ) : null}
    </div>
  );
}
