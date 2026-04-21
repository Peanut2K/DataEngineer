# Environmental Data Pipeline

End-to-end Data Engineering project using **Kafka (KRaft)**, **MongoDB**, **Apache Airflow**, and **Metabase** following the **Medallion architecture** (Bronze → Silver → Gold).

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│               Apache Airflow (Scheduler)                 │
│   DAG: environmental_pipeline_hourly  (@hourly)          │
│   DAG: flood_pipeline_daily           (@daily)           │
└────────────┬────────────────────────────────────────────┘
             │ triggers
             ▼
┌─────────────────────────────────────────────────────────┐
│            Producers (push to Kafka)                    │
│  air_quality_producer │ weather_producer │ flood_producer│
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│               Kafka (KRaft, no Zookeeper)               │
│  air_quality_raw │ weather_raw │ flood_raw              │
└──────────────────────┬──────────────────────────────────┘
                       │  bronze-consumer (always-on)
                       ▼
┌─────────────────────────────────────────────────────────┐
│              MongoDB — BRONZE Layer (raw)               │
└──────────────────────┬──────────────────────────────────┘
                       │  bronze_to_silver.py
                       ▼
┌─────────────────────────────────────────────────────────┐
│              MongoDB — SILVER Layer (cleaned)           │
└──────────────────────┬──────────────────────────────────┘
                       │  silver_to_gold.py
                       ▼
┌─────────────────────────────────────────────────────────┐
│         MongoDB — GOLD Layer (Star Schema)              │
│  fact_environmental │ dim_location │ dim_time           │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
            Metabase Dashboard (port 3000)
```

---

## Quick Start (fresh clone)

```bash
# 1. Copy environment file
cp .env.example .env
# (Optional) add API keys to .env for real data

# 2. Start everything — fully automated
docker compose up -d
```

On first start, Docker will automatically:
- **`airflow-init`** — migrate Airflow DB, create admin user
- **`pipeline-init`** — create MongoDB collections + indexes
- **`metabase-setup`** — configure Metabase admin, connect MongoDB, build dashboard + choropleth maps

Wait ~3 minutes, then open:

| Service        | URL                              | Credentials                             |
|----------------|----------------------------------|-----------------------------------------|
| Metabase       | http://localhost:3000            | admin@dataengineer.local / Admin1234!   |
| Airflow        | http://localhost:8080            | admin / admin                           |
| Mongo Express  | http://localhost:8081            | (no auth)                               |

Enable the Airflow DAGs in the UI to start the pipeline.

---

## Reset Everything

```bash
docker compose down -v   # removes all volumes (wipes data)
docker compose up -d     # fresh start — auto-setup runs again
```

---

## Project Structure

```
DataEngineer/
│
├── producers/                    # Kafka producers
│   ├── air_quality_producer.py   #   OpenAQ API / mock → Kafka
│   ├── weather_producer.py       #   OpenWeather API / mock → Kafka
│   └── flood_producer.py         #   CSV / mock → Kafka
│
├── consumers/
│   └── bronze_consumer.py        # Kafka → MongoDB Bronze (always-on)
│
├── transformations/
│   ├── bronze_to_silver.py       # Clean + validate + join
│   └── silver_to_gold.py         # Risk scoring + Star Schema
│
├── dags/                         # Airflow DAGs (scheduling)
│   ├── environmental_pipeline_dag.py   # @hourly: AQ + Weather
│   └── flood_pipeline_dag.py           # @daily:  Flood
│
├── warehouse/
│   ├── schema.py                 # Collection + index setup (idempotent)
│   └── queries.py                # BI-ready aggregation queries
│
├── utils/
│   ├── logger.py                 # Centralised logging factory
│   ├── retry.py                  # @with_retry decorator
│   ├── data_quality.py           # Validation helpers
│   └── kafka_admin.py            # Topic management
│
├── config/
│   └── settings.py               # All configuration (env-driven)
│
├── data/
│   ├── flood_risk.csv            # Flood risk reference data
│   └── thailand_all.geojson     # Thailand 77-province boundaries
│
├── nginx/
│   └── geojson.conf              # Serves GeoJSON for Metabase maps
│
├── logs/
│   └── airflow/                  # Airflow task logs (bind mount)
│
├── .env.example                  # Template — copy to .env
├── .gitignore
├── docker-compose.yml            # Full stack definition
├── Dockerfile.airflow            # Extends airflow:2.10.5 with deps
├── Dockerfile.pipeline           # python:3.11-slim for pipeline services
├── requirements.txt              # Python dependencies
└── setup_metabase.py             # Metabase auto-setup script
```

---

## Risk Score Formula

```
risk_score = (pm25_score × 0.5) + (temp_score × 0.2) + (flood_score × 0.3)
```

| Risk Level | Score Range |
|------------|-------------|
| Low        | ≤ 30        |
| Moderate   | 31 – 60     |
| High       | 61 – 80     |
| Critical   | 81+         |

---

## Services

| Container          | Image                      | Port  | Purpose                          |
|--------------------|----------------------------|-------|----------------------------------|
| kafka              | apache/kafka:3.9.0         | 9092  | Message broker (KRaft)           |
| mongodb            | mongo:7.0                  | 27017 | Data warehouse                   |
| postgres           | postgres:15                | —     | Airflow metadata DB              |
| airflow-webserver  | (Dockerfile.airflow)       | 8080  | Airflow UI                       |
| airflow-scheduler  | (Dockerfile.airflow)       | —     | DAG executor                     |
| airflow-init       | (Dockerfile.airflow)       | —     | One-time Airflow setup           |
| pipeline-init      | (Dockerfile.pipeline)      | —     | One-time MongoDB schema setup    |
| bronze-consumer    | (Dockerfile.pipeline)      | —     | Kafka → Bronze (always-on)       |
| metabase           | metabase/metabase:v0.52.5  | 3000  | BI dashboard                     |
| metabase-setup     | (Dockerfile.pipeline)      | —     | One-time Metabase setup          |
| geojson-server     | nginx:alpine               | 8899  | GeoJSON file server              |
| mongo-express      | mongo-express:1.0.2        | 8081  | MongoDB admin UI                 |


---

## Architecture Overview

```
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│  OpenAQ API │   │ OpenWeather │   │  Mock CSV   │
│  (PM2.5)    │   │  (Temp)     │   │(Flood Risk) │
└──────┬──────┘   └──────┬──────┘   └──────┬──────┘
       │                 │                 │
       ▼                 ▼                 ▼
┌─────────────────────────────────────────────────┐
│              Kafka (KRaft mode)                 │
│  air_quality_raw | weather_raw | flood_raw      │
└───────────────────────┬─────────────────────────┘
                        │  Bronze Consumer
                        ▼
┌─────────────────────────────────────────────────┐
│           MongoDB — BRONZE Layer                │
│  bronze_air_quality | bronze_weather            │
│  bronze_flood                                   │
└───────────────────────┬─────────────────────────┘
                        │  bronze_to_silver.py
                        ▼
┌─────────────────────────────────────────────────┐
│           MongoDB — SILVER Layer                │
│  silver_air_quality | silver_weather            │
│  silver_flood | silver_environmental_joined     │
└───────────────────────┬─────────────────────────┘
                        │  silver_to_gold.py
                        ▼
┌─────────────────────────────────────────────────┐
│           MongoDB — GOLD Layer (Star Schema)    │
│  fact_environmental                             │
│  dim_location | dim_time                        │
└─────────────────────────────────────────────────┘
                        │
                        ▼
              BI Dashboard / Queries
```

---

## Project Structure

```
DataEngineer/
│
├── producers/
│   ├── air_quality_producer.py   # OpenAQ API / mock → Kafka
│   ├── weather_producer.py       # OpenWeather API / mock → Kafka
│   └── flood_producer.py         # CSV / mock → Kafka
│
├── consumers/
│   └── bronze_consumer.py        # Kafka → MongoDB Bronze
│
├── transformations/
│   ├── bronze_to_silver.py       # Clean + validate + join
│   └── silver_to_gold.py         # Risk scoring + Star Schema
│
├── warehouse/
│   ├── schema.py                 # Collection + index setup
│   └── queries.py                # BI-ready aggregation queries
│
├── utils/
│   ├── logger.py                 # Centralised logging factory
│   ├── retry.py                  # @with_retry decorator
│   ├── data_quality.py           # Validation rules
│   └── kafka_admin.py            # Topic management
│
├── config/
│   └── settings.py               # All configuration (env-driven)
│
├── data/                         # CSV data files (auto-generated)
├── logs/                         # Daily log files per module
│
├── docker-compose.yml            # Kafka (KRaft) + MongoDB + Mongo Express
├── requirements.txt
├── .env.example
├── orchestrator.py               # Main scheduler entry point
└── README.md
```

---

## Data Sources

| Source | Type | Fields | Frequency | Fallback |
|--------|------|--------|-----------|---------|
| OpenAQ | REST API | PM2.5 (µg/m³) | Hourly | Mock random data |
| OpenWeather | REST API | Temperature (°C) | Hourly | Mock random data |
| Flood Risk | CSV / Mock | Flood risk level | Daily | Auto-generated mock |

**5 Provinces:** Bangkok, ChiangMai, Phuket, KhonKaen, Ayutthaya

---

## Risk Scoring Model

### Component Scores

| PM2.5 (µg/m³) | Score | | Temperature (°C) | Score | | Flood Risk | Score |
|---|---|---|---|---|---|---|---|
| 0 – 25 | 10 | | < 20 or > 40 | 80 | | Low | 20 |
| 26 – 50 | 30 | | 20 – 25 | 20 | | Medium | 50 |
| 51 – 100 | 60 | | 26 – 30 | 40 | | High | 80 |
| 101 – 150 | 80 | | 31 – 35 | 60 | | Very High | 100 |
| 151+ | 100 | | 36 – 40 | 75 | | | |

### Weighted Formula

```
risk_score = (pm25_score × 0.5) + (temp_score × 0.2) + (flood_score × 0.3)
```

### Risk Levels

| Score Range | Level |
|-------------|-------|
| 0 – 30 | Low |
| 31 – 60 | Moderate |
| 61 – 80 | High |
| 81 – 100 | Critical |

---

## Quick Start

### 1. Prerequisites

- Docker Desktop
- Python 3.9+

### 2. Start Infrastructure

```bash
docker-compose up -d
```

Services started:
- **Kafka** → `localhost:9092`
- **MongoDB** → `localhost:27017`
- **Mongo Express** → `http://localhost:8081` (browser UI)

### 3. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment

```bash
cp .env.example .env
# Edit .env — API keys are optional; mock data is used without them
```

### 5. Run the Full Pipeline

```bash
python orchestrator.py
```

The orchestrator will:
1. Set up MongoDB collections and indexes
2. Create Kafka topics
3. Run an initial pipeline pass immediately
4. Schedule recurring runs automatically

---

## Running Individual Components

```bash
# Run a specific producer
python -m producers.air_quality_producer
python -m producers.weather_producer
python -m producers.flood_producer

# Run the consumer only
python -m consumers.bronze_consumer

# Run transformations only
python -m transformations.bronze_to_silver
python -m transformations.silver_to_gold

# Set up schema only
python -m warehouse.schema

# Run BI queries / report
python -m warehouse.queries
```

---

## MongoDB Schema (Gold Layer)

### `fact_environmental`

| Field | Type | Description |
|-------|------|-------------|
| province | string | FK → dim_location |
| date | string (YYYY-MM-DD) | FK → dim_time |
| pm25 | double | PM2.5 in µg/m³ |
| temperature | double | Temperature in °C |
| humidity | double | Relative humidity % |
| flood_risk | string | Low / Medium / High / Very High |
| pm25_score | int | 10 / 30 / 60 / 80 / 100 |
| temp_score | int | 20 / 40 / 60 / 75 / 80 |
| flood_score | int | 20 / 50 / 80 / 100 |
| risk_score | double | Weighted composite score |
| risk_level | string | Low / Moderate / High / Critical |

### `dim_location`

| Field | Type |
|-------|------|
| province | string (unique) |
| latitude | double |
| longitude | double |
| country | string |

### `dim_time`

| Field | Type |
|-------|------|
| date | string (unique) |
| year | int |
| quarter | int |
| month | int |
| month_name | string |
| week_of_year | int |
| day | int |
| day_of_week | string |
| is_weekend | bool |

---

## Sample MongoDB Queries (BI)

```javascript
// Top provinces by current risk score
db.fact_environmental.aggregate([
  { $sort: { date: -1 } },
  { $group: {
      _id: "$province",
      risk_score: { $first: "$risk_score" },
      risk_level: { $first: "$risk_level" }
  }},
  { $sort: { risk_score: -1 } }
])

// Critical alerts
db.fact_environmental.aggregate([
  { $sort: { date: -1 } },
  { $group: {
      _id: "$province",
      risk_level: { $first: "$risk_level" },
      risk_score: { $first: "$risk_score" }
  }},
  { $match: { risk_level: { $in: ["Critical", "High"] } } }
])

// Average PM2.5 per province
db.fact_environmental.aggregate([
  { $group: {
      _id: "$province",
      avg_pm25: { $avg: "$pm25" },
      max_pm25: { $max: "$pm25" }
  }},
  { $sort: { avg_pm25: -1 } }
])

// Risk trend for Bangkok (last 7 records)
db.fact_environmental.find(
  { province: "Bangkok" },
  { date: 1, risk_score: 1, risk_level: 1, pm25: 1 }
).sort({ date: -1 }).limit(7)
```

---

## Data Quality Rules

| Rule | Applied At | Details |
|------|-----------|---------|
| No null key fields | Silver | timestamp/province/pm25 (air), temperature (weather), date/flood_risk (flood) |
| PM2.5 ≥ 0 | Silver | Negative values are dropped |
| Temperature −10 to 60°C | Silver | Out-of-range values are dropped |
| No duplicates | Bronze (index) | MongoDB unique index on `message_id` |

---

## Incremental Load & Idempotency

| Mechanism | Where | How |
|-----------|-------|-----|
| Last timestamp state | Producers | `logs/*_state.json` persists the latest processed timestamp |
| Last date state | Flood producer | Skips if today already produced |
| Idempotency key | All producers | `message_id = MD5(timestamp + province + value)` |
| Duplicate prevention | Bronze consumer | MongoDB unique index on `message_id` |
| Upsert merge | Silver & Gold | `UpdateOne(filter, $set, upsert=True)` on composite keys |

---

## Logs

Log files are created in `logs/` with the format `YYYY-MM-DD_<module>.log`.

Each log line format:
```
2026-04-21 14:30:00 | air_quality_producer     | INFO     | Sent 5 messages to 'air_quality_raw'
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `MONGODB_URI` | `mongodb://admin:password@localhost:27017` | MongoDB connection string |
| `MONGODB_DATABASE` | `environmental_db` | Target database name |
| `OPENAQ_API_KEY` | _(empty)_ | OpenAQ API key (optional) |
| `OPENWEATHER_API_KEY` | _(empty)_ | OpenWeather API key (optional) |
| `LOG_LEVEL` | `INFO` | Python logging level |
| `LOG_DIR` | `logs` | Log file directory |
