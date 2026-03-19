# AIS Big Data Test using GeoParquet and Apache Sedona

A pipeline for ingesting raw AIS (Automatic Identification System) maritime data, 
transforming it into partitioned GeoParquet files, and querying with Apache Sedona.

![AIS Sedona Test Screenshot](assets/ais-sedona-screenshot.jpeg)

## What it does

```
Raw AIS (NMEA / CSV) → Parse & Decode → Validate & Clean → Enrich → GeoParquet
                                                                   └→ Build Tracks → Tracks GeoParquet
```

Three output tables:

- **Positions** — one row per AIS message, partitioned by `date=` and `h3_r3=` (H3 resolution 3, ~500 km cells)
- **Vessel Metadata** — slowly-changing dimension, one row per MMSI
- **Tracks** — derived voyage segments (LineString geometries), partitioned by month

## Technology

| Layer | Stack |
|---|---|
| Ingestion | Plain Java — `aismessages` + JTS + Apache Parquet + H3-Java |
| Query | Apache Spark 4.0 + Apache Sedona 1.8.1 |
| API | Quarkus 3.32.1 (Spark Connect client) |
| UI | React 18 + OpenLayers 9 |

## Data layout

```
data/
  positions/date=YYYY-MM-DD/h3_r3=<cell>/part-*.parquet
  vessels/part-*.parquet
  tracks/date=YYYY-MM/part-*.parquet
```

## Modules

| Module | Description |
|---|---|
| [`ais-io`](ais-io/) | Plain-Java ingestion pipeline — parse, validate, write GeoParquet |
| [`ais-backend`](ais-backend/) | Quarkus REST API — import endpoints + Sedona queries |
| [`ais-frontend`](ais-frontend/) | React + OpenLayers map UI |

## Quick start

```bash
# Build both Java modules
cd ais-io && mvn package -q && cd ..
cd ais-backend && mvn package -DskipTests && cd ..
cd ais-frontend && npm run build && cd ..

# Start Spark + backend + frontend
docker compose up
```

- Frontend: http://localhost:3000
- Backend API: http://localhost:8080
- Backend Swagger UI: http://localhost:8080/q/swagger-ui/
- Spark UI: http://localhost:4040
