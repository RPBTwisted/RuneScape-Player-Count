# RuneScape Player Count

A Python toolkit for scraping and serving RuneScape (RS3 & OSRS) player population data. Collects total and per-world player counts, stores them in SQLite, and exposes a FastAPI REST API with time-series aggregation and filtering.

## Project Structure

| File | Description |
|---|---|
| `runescapeplayercount.py` | Scraper — collects player counts from official Jagex endpoints |
| `models.py` | SQLAlchemy database models and schema |
| `queries.py` | Reusable query functions with time-series aggregation |
| `api.py` | FastAPI REST API |

## Setup

```bash
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # macOS/Linux
pip install -r requirements.txt
```

## Usage

### Collecting Data

Run the scraper to collect a snapshot of current player counts:

```bash
python runescapeplayercount.py
```

This scrapes the official RuneScape and Old School RuneScape sites and stores:
- Total player counts for RS3 and OSRS
- Per-world population with region, type (F2P/Members), and activity

Set this up on a scheduler (e.g. cron, Task Scheduler) to collect data at regular intervals.

### Running the API

```bash
uvicorn api:app --reload
```

Interactive API docs available at `http://localhost:8000/docs`.

## API Endpoints

All time-series endpoints support these query parameters:
- `start` / `end` — date range (ISO 8601, defaults to last 24 hours)
- `granularity` — `5min`, `15min`, `30min`, `hourly`, `daily`, `weekly`, `monthly`
- `agg` — `average` or `peak`

| Endpoint | Description |
|---|---|
| `GET /api/player-count?game=OSRS` | Player count over time for RS3 or OSRS |
| `GET /api/player-count/combined` | RS3 + OSRS combined total |
| `GET /api/player-count/by-type` | F2P vs Members breakdown |
| `GET /api/player-count/by-region` | Breakdown by server region |
| `GET /api/player-count/by-world/{world}` | History for a specific world |
| `GET /api/player-count/by-activity` | Players grouped by world activity |
| `GET /api/worlds/snapshot` | Latest per-world population snapshot |

## Data Sources

- **RS3 + OSRS combined**: `runescape.com/player_count.js`
- **OSRS per-world**: `oldschool.runescape.com/slu`
- **RS3 count**: derived as combined minus OSRS
