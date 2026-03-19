package dk.carolus.ais.io.pipeline;

import dk.carolus.ais.io.model.AisPosition;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import com.uber.h3core.H3Core;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.WKBWriter;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes validated {@link AisPosition} records to partitioned GeoParquet files.
 *
 * <p>Output layout:
 * <pre>
 *   &lt;outputDir&gt;/positions/date=YYYY-MM-DD/h3_r3=&lt;cell&gt;/part-00000.parquet
 * </pre>
 *
 * <p>Positions are partitioned first by UTC date, then by H3 resolution-3 cell
 * (~500 km edge cells), enabling efficient time-range + spatial filter push-down
 * in Apache Sedona and other Parquet-aware engines.
 *
 * <p>Each Parquet file carries the {@code geo} file-level metadata key required by
 * the <a href="https://geoparquet.org/releases/v1.1.0/">GeoParquet 1.1.0 spec</a>,
 * including a {@code bbox} field computed from the actual data in each partition file.
 */
@Slf4j
public class GeoParquetWriter implements Closeable {

    // SRID 4326 (WGS 84)
    private static final GeometryFactory GEOM_FACTORY =
            new GeometryFactory(new PrecisionModel(), 4326);
    private static final int PART_FILE_NUMBER = 0;
    private static final int H3_RESOLUTION = 3;

    private final Path outputDir;
    private final Schema schema;
    private final H3Core h3;
    private final WKBWriter wkbWriter =
            new WKBWriter(2, org.locationtech.jts.io.ByteOrderValues.LITTLE_ENDIAN);

    /** Buffers records per partition key (date/h3Cell) until close(). */
    private final Map<String, PartitionBuffer> buffers = new LinkedHashMap<>();
    private long totalWritten = 0;

    /**
     * @param outputDir root output directory; positions will be written under
     *                  {@code outputDir/positions/date=.../h3_r3=.../part-00000.parquet}
     */
    public GeoParquetWriter(Path outputDir) throws IOException {
        this.outputDir = outputDir;
        this.schema = loadSchema();
        this.h3 = H3Core.newInstance();
    }

    /**
     * Buffers a single position record. Call {@link #close()} when done to flush all
     * partition files with correct per-file bbox metadata.
     */
    public void write(AisPosition p) throws IOException {
        String date   = p.getDatePartition();
        String h3Cell = h3.latLngToCellAddress(p.getLat(), p.getLon(), H3_RESOLUTION);
        String key    = date + "/" + h3Cell;

        PartitionBuffer buf = buffers.computeIfAbsent(key, k -> new PartitionBuffer());
        buf.update(p.getLon(), p.getLat());
        buf.records.add(toRecord(p));
        totalWritten++;
    }

    /** Returns the total number of records written so far. */
    public long getTotalWritten() {
        return totalWritten;
    }

    /**
     * Flushes all buffered partitions to Parquet files, each with a {@code bbox} field
     * computed from the actual data. Clears all buffers.
     */
    @Override
    public void close() throws IOException {
        int partitions = buffers.size();
        IOException first = null;
        for (Map.Entry<String, PartitionBuffer> entry : buffers.entrySet()) {
            String[] parts = entry.getKey().split("/", 2);
            String date   = parts[0];
            String h3Cell = parts[1];
            PartitionBuffer buf = entry.getValue();
            try {
                writePartition(date, h3Cell, buf);
            } catch (IOException e) {
                if (first == null) first = e;
            }
        }
        buffers.clear();
        log.info("Wrote {} total records across {} date/H3 partitions", totalWritten, partitions);
        if (first != null) throw first;
    }

    /**
     * Convenience batch write — streams all positions then closes this writer.
     *
     * @return number of records written
     */
    public long write(List<AisPosition> positions) throws IOException {
        for (AisPosition p : positions) {
            write(p);
        }
        close();
        return totalWritten;
    }

    // ---- Internal ---------------------------------------------------------

    private void writePartition(String date, String h3Cell, PartitionBuffer buf)
            throws IOException {
        Path partDir = outputDir.resolve("positions")
                .resolve("date=" + date)
                .resolve("h3_r3=" + h3Cell);
        Files.createDirectories(partDir);
        File outFile = partDir.resolve(String.format("part-%05d.parquet", PART_FILE_NUMBER)).toFile();

        Map<String, String> extraMeta = new HashMap<>();
        extraMeta.put("geo", buildGeoMetadata(buf.minLon, buf.minLat, buf.maxLon, buf.maxLat));

        log.info("  Opening partition date={}/h3_r3={} → {}", date, h3Cell, outFile.getPath());
        try (ParquetWriter<GenericRecord> writer =
                     AvroParquetWriter.<GenericRecord>builder(new LocalOutputFile(outFile))
                             .withSchema(schema)
                             .withCompressionCodec(CompressionCodecName.SNAPPY)
                             .withExtraMetaData(extraMeta)
                             .build()) {
            for (GenericRecord rec : buf.records) {
                writer.write(rec);
            }
        }
    }

    private GenericRecord toRecord(AisPosition p) {
        GenericRecord rec = new GenericData.Record(schema);

        rec.put("mmsi", p.getMmsi());
        rec.put("timestamp_us", p.getTimestamp().toEpochMilli() * 1_000L); // ms → µs

        // Geometry: WGS84 Point encoded as WKB (lon, lat order per GeoParquet convention)
        Point point = GEOM_FACTORY.createPoint(new Coordinate(p.getLon(), p.getLat()));
        rec.put("geometry", ByteBuffer.wrap(wkbWriter.write(point)));

        rec.put("sog", p.getSog());
        rec.put("cog", p.getCog());
        rec.put("heading", p.getHeading());
        rec.put("nav_status", p.getNavStatus());
        rec.put("rot", p.getRot());
        rec.put("msg_type", p.getMsgType());

        return rec;
    }

    // ---- GeoParquet metadata ---------------------------------------------

    /**
     * Builds the {@code geo} JSON metadata string per the GeoParquet 1.1.0 specification,
     * including the {@code bbox} field for the geometry column.
     *
     * @param minLon minimum longitude of all geometries in this file
     * @param minLat minimum latitude of all geometries in this file
     * @param maxLon maximum longitude of all geometries in this file
     * @param maxLat maximum latitude of all geometries in this file
     */
    static String buildGeoMetadata(double minLon, double minLat, double maxLon, double maxLat) {
        return "{"
                + "\"version\":\"1.1.0\","
                + "\"primary_column\":\"geometry\","
                + "\"columns\":{"
                + "\"geometry\":{"
                + "\"encoding\":\"WKB\","
                + "\"geometry_types\":[\"Point\"],"
                + String.format(java.util.Locale.ROOT,
                        "\"bbox\":[%f,%f,%f,%f],", minLon, minLat, maxLon, maxLat)
                + "\"crs\":{"
                + "\"$schema\":\"https://proj.org/schemas/v0.4/projjson.schema.json\","
                + "\"type\":\"GeographicCRS\","
                + "\"name\":\"WGS 84\","
                + "\"datum_ensemble\":{"
                + "\"name\":\"World Geodetic System 1984 ensemble\","
                + "\"members\":[{\"name\":\"World Geodetic System 1984 (Transit)\","
                + "\"id\":{\"authority\":\"EPSG\",\"code\":1166}}],"
                + "\"ellipsoid\":{\"name\":\"WGS 84\","
                + "\"semi_major_axis\":6378137,\"inverse_flattening\":298.257223563},"
                + "\"accuracy\":\"2.0\","
                + "\"id\":{\"authority\":\"EPSG\",\"code\":6326}},"
                + "\"coordinate_system\":{\"subtype\":\"ellipsoidal\","
                + "\"axis\":["
                + "{\"name\":\"Geodetic latitude\",\"abbreviation\":\"Lat\","
                + "\"direction\":\"north\",\"unit\":\"degree\"},"
                + "{\"name\":\"Geodetic longitude\",\"abbreviation\":\"Lon\","
                + "\"direction\":\"east\",\"unit\":\"degree\"}"
                + "]},"
                + "\"id\":{\"authority\":\"EPSG\",\"code\":4326}"
                + "}"   // end crs
                + "}"   // end geometry column
                + "}"   // end columns
                + "}";  // end geo
    }

    // ---- Schema loading --------------------------------------------------

    private static Schema loadSchema() throws IOException {
        try (InputStream is = GeoParquetWriter.class.getResourceAsStream("/avro/ais_position.avsc")) {
            if (is == null) {
                throw new IOException("Avro schema resource not found: /avro/ais_position.avsc");
            }
            return new Schema.Parser().parse(is);
        }
    }

    // ---- Per-partition buffer --------------------------------------------

    private static final class PartitionBuffer {
        final List<GenericRecord> records = new ArrayList<>();
        double minLon =  Double.MAX_VALUE;
        double minLat =  Double.MAX_VALUE;
        double maxLon = -Double.MAX_VALUE;
        double maxLat = -Double.MAX_VALUE;

        void update(double lon, double lat) {
            if (lon < minLon) minLon = lon;
            if (lat < minLat) minLat = lat;
            if (lon > maxLon) maxLon = lon;
            if (lat > maxLat) maxLat = lat;
        }
    }
}
