import React, { useEffect, useMemo, useRef, useState } from "react";

declare global {
  interface Window {
    L?: any;
  }
}

type MapOverview = {
  source: string;
  feed_header_timestamp_local: string | null;
  generated_at_local: string;
  static_gtfs_loaded: boolean;
  vehicles: Array<{
    vehicle_id?: string | null;
    vehicle_label?: string | null;
    trip_id?: string | null;
    route_id?: string | null;
    route_short_name?: string | null;
    trip_headsign?: string | null;
    direction_id?: number | null;
    lat?: number | null;
    lon?: number | null;
    bearing?: number | null;
    speed_mps?: number | null;
    timestamp_local?: string | null;
    stop_id?: string | null;
    stop_name?: string | null;
    next_trip_id?: string | null;
  }>;
  anomalies: Array<{
    detection_time_local?: string | null;
    kind?: string | null;
    trip_id?: string | null;
    route_id?: string | null;
    route_short_name?: string | null;
    stop_id?: string | null;
    stop_name?: string | null;
    stop_sequence?: number | null;
    prev_arrival_time_local?: string | null;
    curr_arrival_time_local?: string | null;
    note?: string | null;
    stop_lat?: number | null;
    stop_lon?: number | null;
    vehicle_id?: string | null;
    vehicle_lat?: number | null;
    vehicle_lon?: number | null;
    link?: Array<[number, number]> | null;
  }>;
};

type TripMap = {
  source: string;
  generated_at_local: string;
  trip: {
    trip_id?: string | null;
    route_id?: string | null;
    route_short_name?: string | null;
    route_long_name?: string | null;
    trip_headsign?: string | null;
    direction_id?: number | null;
    shape_id?: string | null;
  };
  polyline: number[][];
  stops: Array<{
    stop_sequence?: number | null;
    stop_id?: string | null;
    stop_name?: string | null;
    stop_lat?: number | null;
    stop_lon?: number | null;
    rt_present?: boolean;
    has_anomaly?: boolean;
  }>;
  rt_stop_time_updates: any[];
  anomalies: any[];
  bounds?: { min_lat: number; min_lon: number; max_lat: number; max_lon: number } | null;
};

type StaticRoute = { route_id: string; route_short_name?: string | null; route_long_name?: string | null };
type StaticStop = { stop_id: string; stop_name?: string | null; stop_lat: number; stop_lon: number };

function isIsoLike(s: string) {
  return /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(s);
}

function fmt(v: any) {
  if (v === null || v === undefined) return "—";
  if (typeof v === "number" && !Number.isFinite(v)) return "—";
  const s = String(v);
  if (["", "nan", "none", "null"].includes(s.trim().toLowerCase())) return "—";
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

async function fetchJson<T>(apiBase: string, path: string): Promise<T> {
  const res = await fetch(`${apiBase}${path}`);
  if (!res.ok) {
    const t = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${t}`);
  }
  return res.json();
}


function norm(s: any) {
  return (s ?? "").toString().trim().toLowerCase();
}
function normId(s: any) {
  const t = (s ?? "").toString().trim();
  if (!t) return "";
  const lower = t.toLowerCase();
  if (/^\d+$/.test(lower)) {
    const z = lower.replace(/^0+/, "");
    return z || "0";
  }
  return lower;
}
function idMatch(value: any, filter: string) {
  const f = (filter ?? "").toString().trim();
  if (!f) return true;
  const fv = normId(f);
  const vv = normId(value);
  if (!vv) return false;
  return vv === fv || vv.includes(fv) || fv.includes(vv);
}


function matchesId(value: any, id: string) {
  if (!id) return true;
  return idMatch(value, id);
}


export default function MapView({
  apiBase,
  source,
  refreshSeconds,
  stopIdFilter,
  routeIdFilter,
  tripIdFilter,
  vehicleIdFilter,
  freeTextFilter,
}: {
  apiBase: string;
  source: string;
  refreshSeconds: number;
  stopIdFilter?: string;
  routeIdFilter?: string;
  tripIdFilter?: string;
  vehicleIdFilter?: string;
  freeTextFilter?: string;
}) {

  const mapDivRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<any>(null);
  const baseLayerRef = useRef<any>(null);

  const vehiclesLayerRef = useRef<any>(null);
  const anomaliesLayerRef = useRef<any>(null);
  const linksLayerRef = useRef<any>(null);
  const tripLayerRef = useRef<any>(null);
  const allStopsLayerRef = useRef<any>(null);

  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [overview, setOverview] = useState<MapOverview | null>(null);

  const filteredOverview = overview;

  const [showTransportlīdzekļi, setShowTransportlīdzekļi] = useState<boolean>(true);
  const [showAnomālijas, setShowAnomālijas] = useState<boolean>(true);
  const [showSaites, setShowSaites] = useState<boolean>(true);
  const [showTrip, setShowTrip] = useState<boolean>(true);
  const [showAllStops, setShowAllStops] = useState<boolean>(false);

  const [minutes, setMinutes] = useState<number>(90);
  const [routeFilter, setRouteFilter] = useState<string>(routeIdFilter ?? "");

  const [routes, setRoutes] = useState<StaticRoute[]>([]);
  const [allStops, setAllStops] = useState<StaticStop[]>([]);

  const [selectedTripId, setSelectedTripId] = useState<string>("");
  const [selectedTrip, setSelectedTrip] = useState<TripMap | null>(null);

  const [staticRoutes, setStaticRoutes] = useState<StaticRoute[]>([]);
  const [staticStops, setStaticStops] = useState<StaticStop[]>([]);

  useEffect(() => {
    setRoutes([]);
    setAllStops([]);
    setSelectedTrip(null);
  }, [source]);

  const [sekotTransportlīdzeklis, setFollowTransportlīdzeklis] = useState<boolean>(false);
  const [selectedTransportlīdzeklisId, setSelectedTransportlīdzeklisId] = useState<string>("");

  useEffect(() => {
    setRouteFilter(routeIdFilter ?? "");
  }, [routeIdFilter]);

  const canUseLeaflet = useMemo(() => typeof window !== "undefined" && !!window.L, []);

  // Init Leaflet map once
  useEffect(() => {
    if (!canUseLeaflet) return;
    if (!mapDivRef.current) return;
    if (mapRef.current) return;

    const L = window.L;

    const map = L.map(mapDivRef.current, {
      zoomControl: true,
      preferCanvas: true,
    });
    mapRef.current = map;

    // Riga-ish default
    map.setView([56.9496, 24.1052], 12);

    const base = L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 20,
      attribution: "&copy; OpenStreetMap contributors",
    });
    base.addTo(map);
    baseLayerRef.current = base;

    vehiclesLayerRef.current = L.layerGroup().addTo(map);
    anomaliesLayerRef.current = L.layerGroup().addTo(map);
    linksLayerRef.current = L.layerGroup().addTo(map);
    allStopsLayerRef.current = L.layerGroup().addTo(map);
    tripLayerRef.current = L.layerGroup().addTo(map);

    return () => {
      try {
        map.remove();
      } catch {
        // ignore
      }
      mapRef.current = null;
    };
  }, [canUseLeaflet]);

  // Load overview now + on interval
  async function loadOverview(doRefresh: boolean) {
    setError(null);
    if (doRefresh) setLoading(true);
    try {
      const rf = routeFilter.trim() || (routeIdFilter ?? "").trim();
      const params = new URLSearchParams({
        source,
        minutes: String(minutes),
      });
      if (rf) params.set("route_id", rf);
      if ((tripIdFilter ?? "").trim()) params.set("trip_id", tripIdFilter!.trim());
      if ((vehicleIdFilter ?? "").trim()) params.set("vehicle_id", vehicleIdFilter!.trim());
      if ((stopIdFilter ?? "").trim()) params.set("stop_id", stopIdFilter!.trim());
      if ((freeTextFilter ?? "").trim()) params.set("q", freeTextFilter!.trim());
      const data = await fetchJson<MapOverview>(apiBase, `/api/map/overview?${params.toString()}`);
      setOverview(data);
    } catch (e: any) {
      setError(e?.message ?? String(e));
    } finally {
      if (doRefresh) setLoading(false);
    }
  }

  useEffect(() => {
    loadOverview(true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [source]);

  useEffect(() => {
    loadOverview(true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [source, minutes, routeFilter, routeIdFilter, tripIdFilter, vehicleIdFilter, stopIdFilter, freeTextFilter]);

  // Load static route list (optional convenience) once static GTFS is available
  useEffect(() => {
    async function loadRoutes() {
      if (!overview?.static_gtfs_loaded) return;
      if (routes.length) return;
      try {
        const r = await fetchJson<StaticRoute[]>(apiBase, `/api/static/routes?limit=5000`);
        setRoutes(r);
      } catch {
        // ignore
      }
    }
    loadRoutes();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [overview?.static_gtfs_loaded, source]);

  // Load static stops once user asks for them (layer is optional)
  useEffect(() => {
    async function loadStops() {
      if (!showAllStops) return;
      if (!overview?.static_gtfs_loaded) return;
      if (allStops.length) return;
      setError(null);
      try {
        const s = await fetchJson<StaticStop[]>(apiBase, `/api/static/stops?limit=10000`);
        setAllStops(s);
      } catch (e: any) {
        setError(e?.message ?? String(e));
      }
    }
    loadStops();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [showAllStops, overview?.static_gtfs_loaded, source]);

  useEffect(() => {
    if (!mapRef.current) return;
    const ms = Math.max(5, refreshSeconds) * 1000;
    const id = window.setInterval(() => loadOverview(false), ms);
    return () => window.clearInterval(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [refreshSeconds, source, minutes, routeFilter, routeIdFilter, tripIdFilter, vehicleIdFilter, stopIdFilter, freeTextFilter]);

  // When selecting trip, fetch trip data once, and refresh it lightly when overview refreshes
  useEffect(() => {
    async function loadTrip() {
      if (!selectedTripId) {
        setSelectedTrip(null);
        return;
      }
      setError(null);
      try {
        const q = new URLSearchParams({ source, trip_id: selectedTripId });
        const data = await fetchJson<TripMap>(apiBase, `/api/map/trip?${q.toString()}`);
        setSelectedTrip(data);
      } catch (e: any) {
        setError(e?.message ?? String(e));
      }
    }
    loadTrip();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedTripId, source, overview?.generated_at_local]);

  useEffect(() => {
    if (!sekotTransportlīdzeklis || !selectedTransportlīdzeklisId || !mapRef.current || !filteredOverview) return;
    const v = (filteredOverview.vehicles ?? []).find((x) => String(x.vehicle_id ?? "") === String(selectedTransportlīdzeklisId));
    if (!v || v.lat == null || v.lon == null) return;
    try {
      mapRef.current.panTo([v.lat, v.lon]);
    } catch {
      // ignore
    }
  }, [sekotTransportlīdzeklis, selectedTransportlīdzeklisId, filteredOverview?.generated_at_local, filteredOverview?.vehicles?.length]);

  // Render layers whenever data changes
  useEffect(() => {
    if (!canUseLeaflet) return;
    const L = window.L;
    const map = mapRef.current;
    if (!map) return;

    const vehiclesLayer = vehiclesLayerRef.current;
    const anomaliesLayer = anomaliesLayerRef.current;
    const linksLayer = linksLayerRef.current;
    const allStopsLayer = allStopsLayerRef.current;
    const tripLayer = tripLayerRef.current;
    if (!vehiclesLayer || !anomaliesLayer || !linksLayer || !allStopsLayer || !tripLayer) return;

    vehiclesLayer.clearLayers();
    anomaliesLayer.clearLayers();
    linksLayer.clearLayers();
    allStopsLayer.clearLayers();
    tripLayer.clearLayers();

    // Reisa pārklājums
    if (showTrip && selectedTrip && selectedTrip.polyline && selectedTrip.polyline.length >= 2) {
      const poly = L.polyline(selectedTrip.polyline, {
        color: "#5b8cff",
        weight: 4,
        opacity: 0.9,
      });
      poly.addTo(tripLayer);

      // Fit bounds once (if user just selected a trip)
      if (selectedTrip.bounds) {
        const b = selectedTrip.bounds;
        const bounds = L.latLngBounds([b.min_lat, b.min_lon], [b.max_lat, b.max_lon]);
        try {
          map.fitBounds(bounds.pad(0.15));
        } catch {
          // ignore
        }
      }

      // Stops along trip
      for (const s of selectedTrip.stops ?? []) {
        if (s.stop_lat == null || s.stop_lon == null) continue;
        const isBad = !!s.has_anomaly;
        const isRt = !!s.rt_present;
        const color = isBad ? "#fb7185" : isRt ? "#2dd4bf" : "#94a3b8";
        const m = L.circleMarker([s.stop_lat, s.stop_lon], {
          radius: isBad ? 7 : 5,
          color,
          weight: 2,
          fillColor: color,
          fillOpacity: 0.85,
        });
        const popup = `<div style="font-family: ui-monospace, monospace; font-size:12px;">
          <div><b>${(s.stop_name ?? "") || (s.stop_id ?? "")}</b></div>
          <div>seq: ${s.stop_sequence ?? "—"}</div>
          <div>RT: ${isRt ? "yes" : "no"} · anomaly: ${isBad ? "yes" : "no"}</div>
        </div>`;
        m.bindPopup(popup);
        m.addTo(tripLayer);
      }
    }

    // Optional: all static stops (for orientation)
    if (showAllStops && allStops && allStops.length) {
      for (const s of allStops) {
        if (s.stop_lat == null || s.stop_lon == null) continue;
        const m = L.circleMarker([s.stop_lat, s.stop_lon], {
          radius: 3,
          color: "#94a3b8",
          weight: 1,
          fillColor: "#94a3b8",
          fillOpacity: 0.35,
        });
        // light popup (only on click)
        m.bindPopup(
          `<div style="font-size:12px; line-height:1.25; font-family: ui-monospace, monospace;">${
            (s.stop_name ?? s.stop_id).toString()
          }<br/>stop_id: ${s.stop_id}</div>`
        );
        m.addTo(allStopsLayer);
      }
    }

    // Transportlīdzekļi
    if (showTransportlīdzekļi && filteredOverview) {
      for (const v of filteredOverview.vehicles ?? []) {
        if (v.lat == null || v.lon == null) continue;
        const isSelected = selectedTransportlīdzeklisId && v.vehicle_id && String(v.vehicle_id) === String(selectedTransportlīdzeklisId);
        const color = isSelected ? "#2dd4bf" : "#5b8cff";
        const m = L.circleMarker([v.lat, v.lon], {
          radius: isSelected ? 10 : 8,
          color,
          weight: 2,
          fillColor: color,
          fillOpacity: 0.9,
        });

        const title = `${v.route_short_name ?? v.route_id ?? ""} · ${v.vehicle_label ?? v.vehicle_id ?? "vehicle"}`;
        const popup = `<div style="font-size:12px; line-height:1.25;">
          <div><b>${title}</b></div>
          <div style="font-family: ui-monospace, monospace; margin-top:4px;">
            vehicle_id: ${v.vehicle_id ?? "—"}<br/>
            trip_id: ${v.trip_id ?? "—"}<br/>
            next_trip_id: ${v.next_trip_id ?? "—"}<br/>
            headsign: ${(v.trip_headsign ?? "").toString()}<br/>
            ts: ${fmt(v.timestamp_local ?? "—")}<br/>
            stop: ${(v.stop_name ?? v.stop_id ?? "—").toString()}
          </div>
          <div style="margin-top:8px;">
            <button id="selTrip" class="leaflet-action-btn">Rādīt reisu</button>
          </div>
        </div>`;
        m.bindPopup(popup);

        m.on("popupopen", (e: any) => {
          // attach click handler to the button inside popup
          const el = e?.popup?._contentNode as HTMLElement | undefined;
          if (!el) return;
          const btn = el.querySelector("#selTrip") as HTMLButtonElement | null;
          if (!btn) return;
          btn.onclick = () => {
            if (v.trip_id) {
              setSelectedTripId(String(v.trip_id));
              setSelectedTransportlīdzeklisId(String(v.vehicle_id ?? ""));
              setShowTrip(true);
            }
          };
        });

        m.on("click", () => {
          // Keep track of selection for sekot mode
          setSelectedTransportlīdzeklisId(String(v.vehicle_id ?? ""));
          if (sekotTransportlīdzeklis) {
            try {
              map.panTo([v.lat, v.lon]);
            } catch {
              // ignore
            }
          }
        });
        m.addTo(vehiclesLayer);
      }
    }

    // Anomālijas
    if (showAnomālijas && filteredOverview) {
      for (const a of filteredOverview.anomalies ?? []) {
        if (a.stop_lat == null || a.stop_lon == null) continue;
        const kind = (a.kind ?? "").toString();
        const color = kind.includes("jump") ? "#fbbf24" : "#fb7185";
        const m = L.circleMarker([a.stop_lat, a.stop_lon], {
          radius: kind.includes("jump") ? 8 : 9,
          color,
          weight: 2,
          fillColor: color,
          fillOpacity: 0.85,
        });
        const popup = `<div style="font-size:12px; line-height:1.25;">
          <div><b>${(a.stop_name ?? a.stop_id ?? "Stop").toString()}</b></div>
          <div style="font-family: ui-monospace, monospace; margin-top:4px;">
            kind: ${kind}<br/>
            route: ${(a.route_short_name ?? a.route_id ?? "—").toString()}<br/>
            trip: ${(a.trip_id ?? "—").toString()}<br/>
            prev: ${fmt(a.prev_arrival_time_local ?? "—")}<br/>
            curr: ${fmt(a.curr_arrival_time_local ?? "—")}<br/>
            at: ${fmt(a.detection_time_local ?? "—")}<br/>
            note: ${(a.note ?? "").toString()}
          </div>
          <div style="margin-top:8px;">
            <button id="selTrip2" class="leaflet-action-btn">Rādīt reisu</button>
          </div>
        </div>`;
        m.bindPopup(popup);
        m.on("popupopen", (e: any) => {
          const el = e?.popup?._contentNode as HTMLElement | undefined;
          if (!el) return;
          const btn = el.querySelector("#selTrip2") as HTMLButtonElement | null;
          if (!btn) return;
          btn.onclick = () => {
            if (a.trip_id) {
              setSelectedTripId(String(a.trip_id));
              setShowTrip(true);
            }
          };
        });
        m.addTo(anomaliesLayer);

        if (showSaites && a.link && a.link.length === 2) {
          const l = L.polyline(a.link, {
            color,
            weight: 2,
            opacity: 0.6,
            dashArray: "4 8",
          });
          l.addTo(linksLayer);
        }
      }
    }
  }, [canUseLeaflet, filteredOverview, selectedTrip, showTransportlīdzekļi, showAnomālijas, showSaites, showTrip, showAllStops, allStops, sekotTransportlīdzeklis, selectedTransportlīdzeklisId]);

  const kpiTransportlīdzekļi = filteredOverview?.vehicles?.length ?? 0;
  const kpiAnomālijas = filteredOverview?.anomalies?.length ?? 0;

  return (
    <div className="map-grid">
      <div className="map-panel">
        <div className="map-panel__hd">
          <div>
            <div style={{ fontWeight: 700 }}>Karte</div>
            <div style={{ fontSize: 12, color: "var(--muted)" }}>
              Tiešraides karte ar servera puses filtriem. Noklikšķini uz transportlīdzekļa/anomālijas → <b>Rādīt reisu</b>.
            </div>
          </div>
          {loading ? <span className="badge badge--warn">Ielāde…</span> : <span className="badge badge--ok">Tiešsaistē</span>}
        </div>

        {!canUseLeaflet && (
          <div className="alert alert--bad" style={{ marginTop: 10 }}>
            <p className="alert__title">Leaflet nav ielādēts</p>
            <p className="alert__text">
              Šis Map tabs izmanto Leaflet (CDN). Ja esi uzņēmuma tīklā ar bloķētu CDN, pievieno Leaflet failus lokāli vai
              atļauj piekļuvi <span className="cell-mono">unpkg.com</span>.
            </p>
          </div>
        )}

        {error && (
          <div className="alert alert--bad" style={{ marginTop: 10 }}>
            <p className="alert__title">Kļūda</p>
            <p className="alert__text" style={{ fontFamily: "var(--mono)", whiteSpace: "pre-wrap" }}>
              {error}
            </p>
          </div>
        )}

        <div className="hr" style={{ marginTop: 14 }} />

        <div className="kpis">
          <div className="kpi">
            <div className="kpi__label">Transportlīdzekļi</div>
            <div className="kpi__value">{kpiTransportlīdzekļi}</div>
          </div>
          <div className="kpi">
            <div className="kpi__label">Anomālijas (pēdējās {minutes} min)</div>
            <div className="kpi__value">{kpiAnomālijas}</div>
          </div>
        </div>

        <div className="hr" />

        {routes.length > 0 && (
          <div className="field">
            <div className="label">Maršruts (statiskie GTFS)</div>
            <select
              className="select"
              value={routeFilter}
              onChange={(e) => {
                setRouteFilter(e.target.value);
                // force an immediate refresh with the new filter
                setTimeout(() => loadOverview(true), 0);
              }}
            >
              <option value="">Visi maršruti</option>
              {routes
                .slice()
                .sort((a, b) => (a.route_short_name ?? a.route_id).localeCompare(b.route_short_name ?? b.route_id))
                .map((r) => (
                  <option key={r.route_id} value={r.route_id}>
                    {(r.route_short_name ?? r.route_id).toString()}
                    {r.route_long_name ? ` — ${r.route_long_name}` : ""}
                  </option>
                ))}
            </select>
          </div>
        )}

        <div className="field">
          <div className="label">Maršruta filtrs (route_id manuāli)</div>
          <input
            className="input"
            placeholder="neobligāti: route_id"
            value={routeFilter}
            onChange={(e) => setRouteFilter(e.target.value)}
          />
        </div>

        <div className="row" style={{ gap: 10, flexWrap: "wrap" }}>
          <div className="field" style={{ minWidth: 160 }}>
            <div className="label">Anomāliju logs</div>
            <select
              className="select"
              value={minutes}
              onChange={(e) => {
                setMinutes(parseInt(e.target.value, 10));
              }}
            >
              {[30, 60, 90, 120, 240].map((m) => (
                <option key={m} value={m}>
                  last {m} min
                </option>
              ))}
            </select>
          </div>

          <div className="field" style={{ minWidth: 180 }}>
            <div className="label">Sekot transportlīdzeklim</div>
            <label className="badge" style={{ cursor: "pointer", display: "inline-flex", alignItems: "center" }}>
              <input
                type="checkbox"
                checked={sekotTransportlīdzeklis}
                onChange={(e) => setFollowTransportlīdzeklis(e.target.checked)}
                style={{ marginRight: 8 }}
              />
              sekot
            </label>
          </div>
        </div>

        <div className="hr" />

        <div className="row" style={{ gap: 10, flexWrap: "wrap" }}>
          <label className="badge" style={{ cursor: "pointer" }}>
            <input type="checkbox" checked={showTransportlīdzekļi} onChange={(e) => setShowTransportlīdzekļi(e.target.checked)} style={{ marginRight: 8 }} />
            Transportlīdzekļi
          </label>
          <label className="badge" style={{ cursor: "pointer" }}>
            <input type="checkbox" checked={showAnomālijas} onChange={(e) => setShowAnomālijas(e.target.checked)} style={{ marginRight: 8 }} />
            Anomālijas
          </label>
          <label className="badge" style={{ cursor: "pointer" }}>
            <input type="checkbox" checked={showSaites} onChange={(e) => setShowSaites(e.target.checked)} style={{ marginRight: 8 }} />
            Saites
          </label>
          <label className="badge" style={{ cursor: "pointer" }}>
            <input type="checkbox" checked={showTrip} onChange={(e) => setShowTrip(e.target.checked)} style={{ marginRight: 8 }} />
            Reisa pārklājums
          </label>
          <label className="badge" style={{ cursor: "pointer" }}>
            <input type="checkbox" checked={showAllStops} onChange={(e) => setShowAllStops(e.target.checked)} style={{ marginRight: 8 }} />
            Visas pieturas
          </label>
        </div>

        <div className="hr" />

        <div className="alert" style={{ margin: 0 }}>
          <p className="alert__title">Plūsmas galvene (vietējais laiks)</p>
          <p className="alert__text" style={{ fontFamily: "var(--mono)" }}>
            {overview?.feed_header_timestamp_local ? fmt(overview.feed_header_timestamp_local) : "—"}
          </p>
          <p className="alert__text" style={{ fontFamily: "var(--mono)", marginTop: 8 }}>
            Karte atjaunota: {overview?.generated_at_local ? fmt(overview.generated_at_local) : "—"}
          </p>
        </div>

        <div className="hr" />

        <div className="row" style={{ gap: 10, flexWrap: "wrap" }}>
          <button className="btn btn--primary" onClick={() => loadOverview(true)} disabled={loading}>
            Atjaunot karti
          </button>
          <button
            className="btn btn--ghost"
            onClick={() => {
              setSelectedTripId("");
              setSelectedTrip(null);
              setSelectedTransportlīdzeklisId("");
            }}
            type="button"
          >
            Notīrīt izvēli
          </button>
        </div>

        {selectedTripId && (
          <div style={{ marginTop: 14 }}>
            <div className="hr" />
            <div className="alert" style={{ margin: 0 }}>
              <p className="alert__title">Izvēlētais reiss</p>
              <p className="alert__text" style={{ fontFamily: "var(--mono)" }}>
                {selectedTrip?.trip?.route_short_name ?? selectedTrip?.trip?.route_id ?? "—"} · {selectedTrip?.trip?.trip_headsign ?? ""}
                <br />
                trip_id: {selectedTripId}
                <br />
                virziens: {selectedTrip?.trip?.direction_id ?? "—"}
                <br />
                RT pieturas: {selectedTrip?.rt_stop_time_updates?.length ?? 0}
              </p>
              {!overview?.static_gtfs_loaded && (
                <p className="alert__text" style={{ marginTop: 10, color: "var(--warn)" }}>
                  Statiskie GTFS nav ielādēti — pieturu koordinātes un maršruta līnijas nebūs pieejamas. Iestati <b>STATIC_GTFS_PATH</b> backendā.
                </p>
              )}
            </div>
          </div>
        )}
      </div>

      <div className="map-canvas">
        <div className="map-canvas__inner" ref={mapDivRef} />
        <div className="map-legend">
          <div className="map-legend__row">
            <span className="dot dot--vehicle" /> Transportlīdzeklis
          </div>
          <div className="map-legend__row">
            <span className="dot dot--stop" /> Pietura (statiskā)
          </div>
          <div className="map-legend__row">
            <span className="dot dot--bad" /> Trūkst / pazudis
          </div>
          <div className="map-legend__row">
            <span className="dot dot--warn" /> Lēciens (nāk. ierašanās)
          </div>
          <div className="map-legend__row">
            <span className="dot dot--ok" /> RT pietura ir
          </div>
          <div className="map-legend__row" style={{ marginTop: 6, color: "var(--muted)" }}>
            Padoms: click anomaly → “Rādīt reisu” to see where RT stops disappeared.
          </div>
        </div>
      </div>
    </div>
  );
}
