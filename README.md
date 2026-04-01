# GTFS unified dashboard + PostgreSQL history

Šis ir viens apvienots projekts, kura pamatā ir `gtfs_rt_dashboard`, bet ar iebūvētu PostgreSQL vēstures glabāšanu un profesionālāku analītikas slāni.

## Kas strādā šajā versijā

- live GTFS-RT monitorings no protobuf (PB) feediem,
- frontend tabi: Reisi, Pieturas, Transportlīdzekļi, Karte, Izsekošana, Anomālijas, Monitorings, Vēsture, Analītika,
- route enrichment no statiskā GTFS pēc `trip_id`,
- `route_id` / `route_short_name` rādīšana bez `NaN` UI,
- PostgreSQL snapshot + vēsture visiem datasetiem,
- history query pēc `stop_id`, `route_id`, `trip_id`, `vehicle_id` un laika intervāla,
- source-aware DQI loģika, kas nesoda par laukiem, kurus realtime avots faktiski nedod,
- gap analytics, kas balstās uz `REFRESH_SECONDS` un backend background polling,
- top aktivitātes KPI un vienkārši chart skati analītikā,
- frontend / backend / SQL shēmas saskaņošana.

## Svarīgākais par gap un polling

Šajā projektā background polling jāvada backendam, nevis frontendam.

- backend pēc noklusējuma pollē PB feedus ik pēc `REFRESH_SECONDS=5`,
- frontend auto-refresh tikai pārlasa API datus un **neizraisa** atkārtotu feed importu,
- tas novērš mākslīgus gap un dubultus refresh ciklus.

## Ātrā palaišana

```bash
docker compose up --build
```

Pēc palaišanas:

- Frontend: `http://localhost:5173`
- Backend docs: `http://localhost:8000/docs`
- PostgreSQL: `localhost:55432`

## Kas ir svarīgs svaigai DB uzstādīšanai

Svaigam projektam pietiek ar `docker compose up --build`.

Backend startā automātiski izpilda:

- `backend/sql/01_init_history_schema.sql`

Šī shēma jau satur visas tabulas un indeksus, kas vajadzīgi šai gala versijai.

## Ja tev jau ir veca DB no iepriekšējām versijām

Ir 2 varianti.

### Variants A — drošākais (ieteicams)

Ja veco vēsturi nevajag saglabāt:

```bash
docker compose down -v
docker compose up --build
```

Tas izveidos tīru PostgreSQL volume un svaigu pareizu shēmu.

### Variants B — saglabāt esošo DB un uzlikt upgrade

Palaid DBeaver un izpildi:

- `backend/sql/03_upgrade_existing_db_to_unified_schema.sql`

Ja vajag tikai minimālu papildinājumu `stop_time_updates_history` tabulai, vari izmantot arī:

- `backend/sql/02_add_stop_vehicle_columns.sql`

Pilni soļi ir failā:

- `DBEAVER_STEPS.md`

## Svarīgie faili

- `backend/main.py` — FastAPI backend, live polling, enrich, API
- `backend/history_store.py` — PostgreSQL glabāšana, history query, analytics
- `backend/sql/01_init_history_schema.sql` — svaigā unified shēma
- `backend/sql/03_upgrade_existing_db_to_unified_schema.sql` — idempotent upgrade esošai DB
- `backend/validate_schema_alignment.py` — pārbauda, ka SQL shēma atbilst backend sagaidītajām kolonnām
- `WINDOWS_SERVER_DEPLOY.md` — deploy uz Windows Server ar Docker/Linus VM pieeju
- `DBEAVER_STEPS.md` — precīzi DBeaver soļi

## Praktiskie API endpointi

### Live/current

- `GET /api/status?source=mobis`
- `POST /api/refresh?source=mobis`
- `GET /api/trip-updates?source=mobis`
- `GET /api/stop-time-updates?source=mobis`
- `GET /api/vehicles?source=mobis`
- `GET /api/alerts?source=mobis`
- `GET /api/map/overview?source=mobis`
- `GET /api/trace?source=mobis&trip_id=...`

### History / analytics

- `GET /api/history?source=mobis&dataset=all&start_time=...&end_time=...&stop_id=...&route_id=...&trip_id=...&vehicle_id=...`
- `GET /api/analytics/overview?source=mobis&hours=24`

## Lokālie pārbaudes skripti

### SQL shēmas validācija

```bash
cd backend
python validate_schema_alignment.py
```

### Frontend build

```bash
cd frontend
npm ci
npm run build
```

### PB importa tests

```bash
cd backend
python import_pb_files.py --dry-run ../samples/gtfs_realtime_7.pb ../samples/gtfs_realtime_8.pb
```

## Piezīme par statisko GTFS

`backend/static_gtfs.zip` ir izmantots route / stop / trip enrich vajadzībām.
Ja `route_id` realtime avotā nav dots, backend mēģina to aizpildīt no statiskā GTFS caur `trip_id`.
Ja arī tad route nav atrisināms, frontend rāda `—`.
