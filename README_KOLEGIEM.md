# GTFS RT monitorings - palaišana kolēģiem

1. Atzipot projektu jaunā mapē.
2. Atvērt termināli mapē, kur ir `docker-compose.yml`.
3. Palaist:

```bash
docker compose down -v
docker compose up --build
```

4. Atvērt pārlūkā:
- Frontend: `http://localhost:5173`
- Backend API docs: `http://localhost:8000/docs`

## Svarīgi
- Datubāze darbojas lokāli Docker konteinerī uz jūsu datora.
- `docker compose down -v` izdzēsīs šī projekta lokālos DB datus.
- Ja gribat saglabāt jau savāktos datus, lietojiet tikai `docker compose down`.
