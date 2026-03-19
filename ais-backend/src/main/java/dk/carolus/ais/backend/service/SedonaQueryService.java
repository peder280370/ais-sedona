package dk.carolus.ais.backend.service;

import dk.carolus.ais.backend.model.GeoJsonFeatureCollection;
import dk.carolus.ais.backend.model.PositionRecord;
import dk.carolus.ais.backend.model.VesselRecord;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.AnalysisException;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@ApplicationScoped
public class SedonaQueryService {

    @ConfigProperty(name = "spark.connect.url")
    String sparkConnectUrl;

    @ConfigProperty(name = "ais.data.dir")
    String dataDir;

    private static final int POSITIONS_AT_LOOKBACK_HOURS = 1;

    private SparkSession spark;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    void init() {
        try {
            spark = SparkSession.builder().remote(sparkConnectUrl).getOrCreate();
            registerViews();
        } catch (Exception e) {
            // Spark Connect not reachable at startup — query methods return empty collections
        }
    }

    private void registerViews() {
        try {
            spark.sql("CREATE OR REPLACE TEMPORARY VIEW ais_positions USING geoparquet OPTIONS (path '%s/positions')".formatted(dataDir));
            spark.sql("CREATE OR REPLACE TEMPORARY VIEW vessel_metadata USING parquet OPTIONS (path '%s/vessels')".formatted(dataDir));
            spark.sql("CREATE OR REPLACE TEMPORARY VIEW ais_tracks USING geoparquet OPTIONS (path '%s/tracks')".formatted(dataDir));
        } catch (Exception e) {
            log.error("Failed to register Spark views", e);
        }
    }

    public List<PositionRecord> queryPositions(
            Double minLon, Double minLat, Double maxLon, Double maxLat,
            Long mmsi, String startDate, String endDate, int limit) {
        registerViews();
        try {
            StringBuilder sql = new StringBuilder(
                "SELECT mmsi, timestamp_us AS ts, " +
                "ST_AsText(geometry) AS geom_wkt, sog, cog, heading, nav_status, rot, msg_type " +
                "FROM ais_positions WHERE 1=1");
            if (minLon != null && minLat != null && maxLon != null && maxLat != null) {
                sql.append(String.format(java.util.Locale.ROOT,
                        " AND ST_Within(geometry, ST_PolygonFromEnvelope(%f, %f, %f, %f))",
                        minLon, minLat, maxLon, maxLat));
            }
            if (mmsi != null) sql.append(" AND mmsi = ").append(mmsi);
            if (startDate != null) {
                sql.append(" AND timestamp_us >= CAST('").append(startDate).append("' AS TIMESTAMP)");
                sql.append(" AND date >= '").append(startDate).append("'");
            }
            if (endDate != null) {
                sql.append(" AND timestamp_us <= CAST('").append(endDate).append("' AS TIMESTAMP)");
                sql.append(" AND date <= '").append(endDate).append("'");
            }
            sql.append(" LIMIT ").append(limit);

            List<Row> rows = spark.sql(sql.toString()).collectAsList();
            List<PositionRecord> result = new ArrayList<>();
            for (Row row : rows) {
                result.add(new PositionRecord(
                    row.getLong(0),
                    row.get(1) != null ? row.get(1).toString() : null,
                    row.getString(2),
                    row.isNullAt(3) ? null : row.getFloat(3),
                    row.isNullAt(4) ? null : row.getFloat(4),
                    row.isNullAt(5) ? null : row.getInt(5),
                    row.isNullAt(6) ? null : row.getInt(6),
                    row.isNullAt(7) ? null : row.getFloat(7),
                    row.isNullAt(8) ? null : row.getInt(8),
                    null, null, null, null
                ));
            }
            return result;
        } catch (Exception e) {
            log.error("Error querying positions", e);
            return List.of();
        }
    }

    public List<PositionRecord> queryPositionsAt(
            Double minLon, Double minLat, Double maxLon, Double maxLat,
            Long mmsi, String atTime, int limit) {
        registerViews();
        try {
            // atTime is ISO datetime e.g. "2024-01-15T12:30:00" — replace T with space for Spark
            String atTimestamp = atTime.replace('T', ' ');
            String atDate = atTime.length() >= 10 ? atTime.substring(0, 10) : atTime;

            // Compute the timestamp 1 hour before atTime for the lower bound
            java.time.LocalDateTime atDt = java.time.LocalDateTime.parse(
                    atTime, java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            java.time.LocalDateTime fromDt = atDt.minusHours(POSITIONS_AT_LOOKBACK_HOURS);
            String fromTimestamp = fromDt.toString().replace('T', ' ');
            String fromDate = fromDt.toLocalDate().toString();

            StringBuilder inner = new StringBuilder(
                "SELECT mmsi, timestamp_us AS ts, " +
                "ST_AsText(geometry) AS geom_wkt, sog, cog, heading, nav_status, rot, msg_type, " +
                "ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY timestamp_us DESC) AS rn " +
                "FROM ais_positions WHERE 1=1");
            if (minLon != null && minLat != null && maxLon != null && maxLat != null) {
                inner.append(String.format(java.util.Locale.ROOT,
                        " AND ST_Within(geometry, ST_PolygonFromEnvelope(%f, %f, %f, %f))",
                        minLon, minLat, maxLon, maxLat));
            }
            if (mmsi != null) inner.append(" AND mmsi = ").append(mmsi);
            inner.append(" AND timestamp_us <= CAST('").append(atTimestamp).append("' AS TIMESTAMP)");
            inner.append(" AND timestamp_us >= CAST('").append(fromTimestamp).append("' AS TIMESTAMP)");
            inner.append(" AND date <= '").append(atDate).append("'");
            inner.append(" AND date >= '").append(fromDate).append("'");

            String sql = "SELECT mmsi, ts, geom_wkt, sog, cog, heading, nav_status, rot, msg_type" +
                " FROM (" + inner + ") WHERE rn = 1 LIMIT " + limit;

            List<Row> rows = spark.sql(sql).collectAsList();
            List<PositionRecord> result = new ArrayList<>();
            for (Row row : rows) {
                result.add(new PositionRecord(
                    row.getLong(0),
                    row.get(1) != null ? row.get(1).toString() : null,
                    row.getString(2),
                    row.isNullAt(3) ? null : row.getFloat(3),
                    row.isNullAt(4) ? null : row.getFloat(4),
                    row.isNullAt(5) ? null : row.getInt(5),
                    row.isNullAt(6) ? null : row.getInt(6),
                    row.isNullAt(7) ? null : row.getFloat(7),
                    row.isNullAt(8) ? null : row.getInt(8),
                    null, null, null, null
                ));
            }
            return result;
        } catch (Exception e) {
            log.error("Error querying positions at time", e);
            return List.of();
        }
    }

    public GeoJsonFeatureCollection queryTracks(
            Double minLon, Double minLat, Double maxLon, Double maxLat,
            Long mmsi, String startDate, String endDate) {
        registerViews();
        try {
            StringBuilder sql = new StringBuilder(
                "SELECT mmsi, voyage_id, ST_AsGeoJSON(geometry) AS geom_json, " +
                "start_time, end_time, " +
                "point_count, avg_sog, distance_nm " +
                "FROM ais_tracks WHERE 1=1");
            if (minLon != null && minLat != null && maxLon != null && maxLat != null) {
                sql.append(String.format(java.util.Locale.ROOT,
                        " AND ST_Intersects(geometry, ST_PolygonFromEnvelope(%f, %f, %f, %f))",
                        minLon, minLat, maxLon, maxLat));
            }
            if (mmsi != null) sql.append(" AND mmsi = ").append(mmsi);
            if (startDate != null) sql.append(" AND date >= '").append(startDate).append("'");
            if (endDate != null) sql.append(" AND date <= '").append(endDate).append("'");

            List<Row> rows = spark.sql(sql.toString()).collectAsList();
            List<Map<String, Object>> features = new ArrayList<>();
            for (Row row : rows) {
                Map<String, Object> feature = new HashMap<>();
                feature.put("type", "Feature");
                String geomJson = row.getString(2);
                Object geometry = null;
                if (geomJson != null) {
                    try {
                        geometry = objectMapper.readValue(geomJson, new TypeReference<Map<String, Object>>() {});
                    } catch (Exception e) {
                        log.warn("Failed to parse geometry JSON: {}", geomJson, e);
                    }
                }
                feature.put("geometry", geometry);
                Map<String, Object> props = new HashMap<>();
                props.put("mmsi", row.getLong(0));
                props.put("voyage_id", row.getString(1));
                props.put("start_time", row.get(3) != null ? row.get(3).toString() : null);
                props.put("end_time", row.get(4) != null ? row.get(4).toString() : null);
                props.put("point_count", row.isNullAt(5) ? null : row.getInt(5));
                props.put("avg_sog", row.isNullAt(6) ? null : row.getFloat(6));
                props.put("distance_nm", row.isNullAt(7) ? null : row.getFloat(7));
                feature.put("properties", props);
                features.add(feature);
            }
            return new GeoJsonFeatureCollection(features);
        } catch (Exception e) {
            log.error("Error querying tracks", e);
            return new GeoJsonFeatureCollection(List.of());
        }
    }

    public List<VesselRecord> queryVessels(Long mmsi, String name) {
        registerViews();
        try {
            StringBuilder sql = new StringBuilder(
                "SELECT mmsi, imo, vessel_name, callsign, ship_type, ship_type_desc, " +
                "length_m, beam_m, draught_m, destination, " +
                "last_seen_us AS last_seen " +
                "FROM vessel_metadata WHERE 1=1");
            if (mmsi != null) sql.append(" AND mmsi = ").append(mmsi);
            if (name != null && !name.isBlank())
                sql.append(" AND UPPER(vessel_name) LIKE UPPER('%").append(name.replace("'", "''")).append("%')");

            List<Row> rows = spark.sql(sql.toString()).collectAsList();
            List<VesselRecord> result = new ArrayList<>();
            for (Row row : rows) {
                result.add(new VesselRecord(
                    row.getLong(0),
                    row.isNullAt(1) ? null : row.getLong(1),
                    row.isNullAt(2) ? null : row.getString(2),
                    row.isNullAt(3) ? null : row.getString(3),
                    row.isNullAt(4) ? null : row.getInt(4),
                    row.isNullAt(5) ? null : row.getString(5),
                    row.isNullAt(6) ? null : row.getFloat(6),
                    row.isNullAt(7) ? null : row.getFloat(7),
                    row.isNullAt(8) ? null : row.getFloat(8),
                    row.isNullAt(9) ? null : row.getString(9),
                    row.get(10) != null ? row.get(10).toString() : null
                ));
            }
            return result;
        } catch (Exception e) {
            log.error("Error querying vessels", e);
            return List.of();
        }
    }
}
