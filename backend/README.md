# GTFS-RT Decoder API (FastAPI)

## Quick start
```bash
cd backend
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/Mac: source .venv/bin/activate
pip install -r requirements.txt

# Optional:
# set GTFS_RT_URL=https://mobis-web-01.rigassatiksme.lv/gtfs_realtime.pb
# set STATIC_GTFS_PATH=path/to/gtfs_static.zip   (stops/routes/trips/shapes for map & enrichment)
# set TZ_NAME=Europe/Riga
# set GTFS_RT_USER=...
# set GTFS_RT_PASS=...
# set VERIFY_TLS=false   (only if you really must)
uvicorn main:app --reload --port 8000
```

Open:
- http://127.0.0.1:8000/docs  (Swagger)

## Endpoints
- GET `/api/status`
- POST `/api/refresh`
- GET `/api/trip-updates`
- GET `/api/stop-time-updates?trip_id=...&route_id=...`
- GET `/api/vehicles?route_id=...`
- GET `/api/alerts`
- GET `/api/anomalies`
- POST `/api/anomalies/clear`
- GET `/api/metrics/routes`
- GET `/api/download/{csv_name}`
- GET `/api/download/workbook.xlsx`

Map / static GTFS:
- GET `/api/static/routes`
- GET `/api/static/stops`
- GET `/api/map/overview`
- GET `/api/map/trip?trip_id=...`


## Multi-source
Set `GTFS_RT_SOURCES` as a JSON object to add/override sources.
Example (PowerShell):
```powershell
$env:GTFS_RT_SOURCES='{"mobis":"https://mobis-web-01.rigassatiksme.lv/gtfs_realtime.pb","saraksti":"https://saraksti.rigassatiksme.lv/gtfs_realtime.pb?gtfsstatic.4tD6jVzykWbSvp3"}'
```
Then use `source=` query param on API endpoints, or use the dashboard Compare tab.
