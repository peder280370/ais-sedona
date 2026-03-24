# Apache Iceberg Integration Plan

## Overview

This document analyses what it would take to replace the current raw-GeoParquet storage layer with Apache Iceberg tables, covering motivation, required changes per module, advantages, and disadvantages.

---

## Why Consider Iceberg?

The current pipeline has several structural weaknesses that Iceberg directly addresses:

| Current Problem | Iceberg Solution |
|---|---|
| No ACID — partial import crash leaves corrupt partitions | Atomic snapshot commits |
| Vessels deduplicated at query time (fragile, slow) | `MERGE INTO` upserts |
| Small-file accumulation across repeated imports | `rewriteDataFiles` compaction |
| Schema changes require full rewrite | Schema evolution (add/rename/drop columns) |
| Partition strategy changes are destructive | Partition evolution without rewrites |
| No audit trail or replay capability | Time travel via snapshot history |
| Temp views go stale silently | Catalog-managed table metadata |

---

## Architecture After Integration

```
ais-io (fat JAR)
  ├── IcebergPositionWriter   → Hadoop Catalog → /data/iceberg/ais/positions/
  ├── IcebergVesselWriter     → Hadoop Catalog → /data/iceberg/ais/vessels/
  └── IcebergTrackWriter      → Hadoop Catalog → /data/iceberg/ais/tracks/

Spark Connect (apache/sedona:1.8.1)
  └── Iceberg catalog: spark.sql.catalog.ais (HadoopCatalog, warehouse=/data/iceberg)

ais-backend (Quarkus)
  └── SedonaQueryService — reads via ais.positions, ais.vessels, ais.tracks
```

The `/data` shared Docker volume continues to hold everything. No new services are required for the Hadoop Catalog approach.

---

## Required Changes

### 1. Dependency Updates

#### `ais-io/pom.xml` — add

```xml
<!-- Iceberg Java API (no Spark required for writes) -->
<dependency>
  <groupId>org.apache.iceberg</groupId>
  <artifactId>iceberg-api</artifactId>
  <version>1.9.1</version>
</dependency>
<dependency>
  <groupId>org.apache.iceberg</groupId>
  <artifactId>iceberg-core</artifactId>
  <version>1.9.1</version>
</dependency>
<dependency>
  <groupId>org.apache.iceberg</groupId>
  <artifactId>iceberg-parquet</artifactId>
  <version>1.9.1</version>
</dependency>
<dependency>
  <groupId>org.apache.iceberg</groupId>
  <artifactId>iceberg-hadoop</artifactId>
  <version>1.9.1</version>
</dependency>
```

Remove: `parquet-avro`, `avro` (Iceberg's own Parquet writer replaces Avro-backed writing).
Keep: `hadoop-client`, `jts-core`, `h3`.

#### `ais-backend/pom.xml` — add

```xml
<dependency>
  <groupId>org.apache.iceberg</groupId>
  <artifactId>iceberg-spark-runtime-4.0_2.13</artifactId>
  <version>1.9.1</version>
</dependency>
```

---

### 2. Schema Changes

Two new columns are added to **positions** and **tracks** to replace per-file GeoParquet `bbox` metadata with Iceberg column-level min/max statistics. These enable file-level spatial pruning without GeoParquet's proprietary file-footer metadata.

#### Positions (Iceberg schema)

```
mmsi          : long      (required)
timestamp_us  : timestamp (required, micros, UTC)
geometry      : binary    (required, WKB Point)
min_lon       : double    (NEW — equals point longitude)
max_lon       : double    (NEW — equals point longitude)
min_lat       : double    (NEW — equals point latitude)
max_lat       : double    (NEW — equals point latitude)
sog           : float     (optional)
cog           : float     (optional)
heading       : int       (optional)
nav_status    : int       (optional)
rot           : float     (optional)
msg_type      : int       (optional)
h3_r3         : string    (NEW — stored as data column, used as partition key)
```

> `h3_r3` moves from being a Hive directory name to an actual stored column so Iceberg can use it as an identity partition transform.

#### Tracks (Iceberg schema)

```
mmsi          : long      (required)
voyage_id     : string    (required)
geometry      : binary    (required, WKB LineString)
min_lon       : double    (NEW — bounding box of LineString)
max_lon       : double    (NEW)
min_lat       : double    (NEW)
max_lat       : double    (NEW)
start_time    : timestamp (required, micros)
end_time      : timestamp (optional, micros)
point_count   : int       (required)
avg_sog       : float     (optional)
distance_nm   : float     (optional)
```

#### Vessels (Iceberg schema — unchanged columns)

```
mmsi          : long      (required, identity partition)
imo           : long      (optional)
vessel_name   : string    (optional)
callsign      : string    (optional)
ship_type     : int       (optional)
ship_type_desc: string    (optional)
length_m      : float     (optional)
beam_m        : float     (optional)
draught_m     : float     (optional)
destination   : string    (optional)
last_seen_us  : timestamp (required, micros)
```

---

### 3. Partition Specs

```java
// Positions
PartitionSpec.builderFor(schema)
    .day("timestamp_us", "date")   // hidden → date=YYYY-MM-DD directory
    .identity("h3_r3")             // hidden → h3_r3=<cell> directory
    .build();

// Tracks
PartitionSpec.builderFor(schema)
    .month("start_time", "date")   // hidden → date=YYYY-MM directory
    .build();

// Vessels — no partitioning (small table, cached in memory)
PartitionSpec.unpartitioned();
```

---

### 4. `ais-io` Writer Changes

Replace three classes with Iceberg equivalents:

#### `GeoParquetWriter` → `IcebergPositionWriter`

```java
// Sketch — replaces Avro-backed buffered write with Iceberg append
Configuration hadoopConf = new Configuration();
HadoopCatalog catalog = new HadoopCatalog(hadoopConf, "/data/iceberg");
Table table = catalog.loadTable(TableIdentifier.of("ais", "positions"));

// Per-partition file appender (batched by date+h3)
FileAppender<Record> appender = Parquet.write(outputFile)
    .schema(table.schema())
    .createWriterFunc(GenericParquetWriter::buildWriter)
    .build();

// On close(): register files as an atomic AppendFiles commit
AppendFiles append = table.newAppend();
append.appendFile(dataFile);
append.commit();
```

Key differences from `GeoParquetWriter`:
- No Avro schema — uses Iceberg `Schema` directly
- No manual GeoParquet footer metadata
- h3_r3 written as a data column alongside geometry
- bbox columns (min/max lon/lat) computed and stored per record
- Commit is atomic — crash before `commit()` leaves no trace in the table

#### `VesselMetadataWriter` → `IcebergVesselWriter`

Append-only write (same as today). The latest-win deduplication moves to a periodic Spark `MERGE INTO` job (see maintenance section), or remains at query time via `ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY last_seen_us DESC)`.

#### `TrackWriter` → `IcebergTrackWriter`

Same pattern as `IcebergPositionWriter`: compute WKB LineString bbox from JTS geometry, store as min/max columns, append files atomically.

---

### 5. `ais-backend` Changes

#### `SedonaQueryService`

**Spark session config additions:**

```java
.config("spark.sql.catalog.ais", "org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.ais.type", "hadoop")
.config("spark.sql.catalog.ais.warehouse", "/data/iceberg")
.config("spark.sql.extensions",
    "org.apache.sedona.sql.SedonaSqlExtensions," +
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
```

**View registration — before:**

```java
spark.sql("CREATE TEMPORARY VIEW ais_positions USING geoparquet OPTIONS (path '/data/positions')");
```

**View registration — after:**

```java
// Thin alias so existing SQL queries need minimal changes
spark.sql("CREATE OR REPLACE TEMPORARY VIEW ais_positions AS SELECT * FROM ais.positions");
spark.sql("CREATE OR REPLACE TEMPORARY VIEW ais_tracks  AS SELECT * FROM ais.tracks");
spark.sql("CREATE OR REPLACE TEMPORARY VIEW vessel_metadata AS SELECT * FROM ais.vessels");
```

**Spatial query update — key change:**

Sedona's `USING geoparquet` auto-registers the geometry column. With Iceberg, geometry is a plain `binary` column; Sedona functions require explicit `ST_GeomFromWKB()`:

```sql
-- Before
WHERE ST_Within(geometry, ST_PolygonFromEnvelope(:minLon, :minLat, :maxLon, :maxLat))

-- After: explicit WKB parsing + bbox pre-filter for file pruning
WHERE max_lon >= :minLon AND min_lon <= :maxLon
  AND max_lat >= :minLat AND min_lat <= :maxLat
  AND ST_Within(ST_GeomFromWKB(geometry), ST_PolygonFromEnvelope(:minLon, :minLat, :maxLon, :maxLat))
```

The bbox pre-filter lets Iceberg's manifest statistics prune files before Sedona evaluates the precise spatial predicate — partially compensating for the loss of per-file GeoParquet bbox metadata.

**New maintenance endpoint (optional):**

```java
// POST /api/maintenance/compact
SparkActions.get(spark).rewriteDataFiles(table)
    .option("target-file-size-bytes", String.valueOf(512L * 1024 * 1024))
    .execute();
SparkActions.get(spark).expireSnapshots(table)
    .expireOlderThan(System.currentTimeMillis() - 7 * 86400_000L)
    .execute();
```

---

### 6. `docker-compose.yml` Changes

Add Iceberg runtime package and catalog config to the Spark command:

```yaml
command: >
  /opt/spark/sbin/start-connect-server.sh --wait
  --packages org.apache.spark:spark-connect_2.13:4.0.1,
             org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.9.1
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
  --conf spark.kryo.registrator=org.apache.sedona.core.serde.SedonaKryoRegistrator
  --conf spark.sql.extensions=org.apache.sedona.sql.SedonaSqlExtensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.ais=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.ais.type=hadoop
  --conf spark.sql.catalog.ais.warehouse=/data/iceberg
  --conf spark.sql.files.maxPartitionBytes=536870912
  --conf spark.sql.parquet.filterPushdown=true
  --conf spark.sql.parquet.mergeSchema=false
```

No new services required for Hadoop Catalog.

---

### 7. Data Migration

Existing raw GeoParquet data in `/data/positions/`, `/data/vessels/`, `/data/tracks/` is not automatically read by Iceberg. Migration options:

**Option A — Re-import**: Drop and re-ingest from source files. Cleanest but requires source data to still be available.

**Option B — Migrate via Spark**: Read existing Parquet files and write into Iceberg tables:

```sql
-- One-time migration job
INSERT INTO ais.positions
SELECT mmsi, timestamp_us, geometry,
       ST_X(ST_GeomFromWKB(geometry)) AS min_lon,
       ST_X(ST_GeomFromWKB(geometry)) AS max_lon,
       ST_Y(ST_GeomFromWKB(geometry)) AS min_lat,
       ST_Y(ST_GeomFromWKB(geometry)) AS max_lat,
       sog, cog, heading, nav_status, rot, msg_type,
       regexp_extract(input_file_name(), 'h3_r3=([^/]+)', 1) AS h3_r3
FROM parquet.`/data/positions`;
```

**Option C — File registration**: Register existing Parquet files into Iceberg metadata without moving data (`AddFiles` procedure). Avoids rewrite but GeoParquet footer metadata is ignored.

---

## Optional Enhancement: Nessie Catalog

For teams wanting branch-based workflows or multi-writer safety, replace the Hadoop Catalog with [Project Nessie](https://projectnessie.org/):

```yaml
# docker-compose.yml — add service
nessie:
  image: ghcr.io/projectnessie/nessie:latest
  ports:
    - "19120:19120"
  environment:
    NESSIE_VERSION_STORE_TYPE: ROCKS  # persistent on volume
  volumes:
    - nessie_data:/nessie/data
```

Spark config:
```conf
spark.sql.catalog.ais.catalog-impl=org.apache.iceberg.nessie.NessieCatalog
spark.sql.catalog.ais.uri=http://nessie:19120/api/v2
spark.sql.catalog.ais.ref=main
```

Nessie enables staging imports on a feature branch and merging after validation — no impact on production reads.

---

## Advantages

### Correctness & Reliability

1. **ACID commits**: A `GeoParquetWriter` crash mid-write currently leaves orphan files in partitions that Spark will silently read. Iceberg's snapshot model makes uncommitted files invisible.
2. **Concurrent import safety**: Two simultaneous DMA imports for different dates can each commit atomically without file collisions.
3. **Vessel upserts**: `MERGE INTO` replaces the fragile last-win file accumulation. A 6-month dataset with daily imports currently results in hundreds of vessel files that Spark must scan and deduplicate; Iceberg collapses this to one logical row per MMSI.

### Operational

4. **Small file compaction**: `rewriteDataFiles` merges many small files into target-sized (512 MB) files, improving scan performance after many incremental imports.
5. **Snapshot expiry**: `expireSnapshots` cleans up old metadata and orphan files; the current system has no cleanup mechanism.
6. **Partition evolution**: Add h3 resolution 7 as a secondary spatial partition without rewriting historical data.
7. **Schema evolution**: Add `true_heading`, `source_id`, or new AIS message fields to existing tables without a full rewrite.

### Query Capability

8. **Time travel**: Debug production queries by querying `FOR SYSTEM_TIME AS OF '...'`. Audit which positions were visible at a specific snapshot.
9. **Incremental reads**: Future Flink or batch jobs can read only positions committed since the last checkpoint via `IncrementalAppendScan`.
10. **Ecosystem**: Tables are readable from Trino, DuckDB (via `iceberg` extension), Flink, Hive, and any Iceberg-aware tool — without Sedona/Spark as a required intermediary.

---

## Disadvantages

### GeoParquet Spatial Pruning Loss

11. **No per-file bbox metadata**: GeoParquet 1.1.0 embeds a `bbox` per Parquet file, which Sedona uses to skip files outside the query viewport before reading. Iceberg's column statistics serve a similar role, but require the explicit `min_lon/max_lon/min_lat/max_lat` columns and a rewritten spatial predicate. The pruning is still file-level but mediated by Iceberg manifests rather than GeoParquet footers.

12. **Geometry column requires explicit parsing**: `USING geoparquet` auto-registers the geometry column with Sedona. With Iceberg, every spatial query must use `ST_GeomFromWKB(geometry)` explicitly. This is a mechanical change but touches every query in `SedonaQueryService`.

13. **GeoParquet-native tools can't read Iceberg directly**: Tools like QGIS, GDAL, or pandas/geopandas that read raw GeoParquet files will not work directly against the Iceberg warehouse layout. They require Iceberg-aware connectors.

### Complexity

14. **ais-io writer rewrite**: The current `GeoParquetWriter` is ~200 lines of straightforward Avro + Parquet code. The Iceberg equivalent requires understanding catalog management, schema creation, partition spec, file appenders, and commit lifecycle. The blast radius of a bug is higher (a bad commit could mark files as deleted).

15. **Dependency size**: Iceberg adds ~15–20 MB of additional JARs to the ais-io fat JAR (iceberg-api, iceberg-core, iceberg-parquet, iceberg-hadoop plus transitive deps). The fat JAR is already large; this increases startup time.

16. **Spark 4.0 + Iceberg 1.9.x compatibility**: Iceberg 1.9.x is the first series with full Spark 4.0 support. The Sedona + Iceberg extension combination (`SedonaSqlExtensions` + `IcebergSparkSessionExtensions`) has limited production history. Integration issues are possible.

17. **Metadata accumulation**: Iceberg creates manifest files, manifest lists, and snapshot metadata on every commit. Without regular `expireSnapshots` + `rewriteManifests` maintenance, the metadata directory grows unbounded. The current system has zero metadata overhead.

18. **Vessel MERGE requires Spark**: `ais-io` is a standalone fat JAR with no Spark dependency. Proper vessel upserts (MERGE INTO) must run via Spark, meaning either: (a) the backend exposes a post-import MERGE endpoint, or (b) ais-io continues appending and a periodic Spark job deduplicates. Neither is as clean as the current simple overwrite model.

---

## Risk Assessment

| Risk | Severity | Mitigation |
|---|---|---|
| Iceberg 1.9 + Spark 4.0 + Sedona 1.8.1 compatibility | High | Prototype with a single table first; pin exact versions |
| GeoParquet spatial pruning regression | Medium | Add bbox columns + benchmark before/after |
| ais-io writer complexity increase | Medium | Incremental replacement (one table at a time) |
| Fat JAR size growth | Low | Shade/exclude unused Iceberg transitive deps |
| Metadata bloat | Low | Schedule `expireSnapshots` from day one |

---

## Implementation Order

1. **Prototype**: Convert `positions` table only; benchmark spatial queries vs current GeoParquet
2. **ais-io positions writer**: `IcebergPositionWriter` replacing `GeoParquetWriter`
3. **ais-backend query update**: Thin view aliases + `ST_GeomFromWKB` + bbox pre-filters
4. **Docker Spark config**: Add Iceberg runtime package and catalog conf
5. **Tracks writer**: `IcebergTrackWriter` replacing `TrackWriter`
6. **Vessels writer**: `IcebergVesselWriter` + Spark MERGE endpoint in backend
7. **Maintenance endpoint**: Compact + expire snapshots
8. **Data migration**: Choose Option A/B/C based on data availability
9. **(Optional)** Replace Hadoop Catalog with Nessie for branch-based import staging

---

## Summary Verdict

Iceberg integration is **high value for a production, frequently-updated dataset** (daily DMA imports, multiple concurrent users). The primary wins are ACID correctness, compaction, and schema/partition evolution. The primary cost is the loss of native GeoParquet file-level spatial pruning — mitigated but not fully replaced by bbox column statistics — and significantly more complex write-path code in `ais-io`.

For a **single-user, batch-import** use case with infrequent schema changes, the current raw GeoParquet approach is simpler and adequate. For a system that needs concurrent imports, growing datasets, and long-term maintainability, Iceberg is the right direction.
