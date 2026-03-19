package dk.carolus.ais.io.pipeline;

import dk.carolus.ais.io.model.AisTrack;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.io.WKBReader;

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
 * Writes derived {@link AisTrack} records to monthly-partitioned GeoParquet files.
 *
 * <p>Output layout:
 * <pre>
 *   &lt;outputDir&gt;/tracks/date=YYYY-MM/part-00000.parquet
 * </pre>
 *
 * <p>Each Parquet file carries the {@code geo} file-level metadata key required by
 * the GeoParquet 1.1.0 spec, with {@code geometry_types} set to {@code ["LineString"]}
 * and a {@code bbox} field computed from the actual track geometries in each partition.
 */
@Slf4j
public class TrackWriter {

    private static final int PART_FILE_NUMBER = 0;

    private final Path outputDir;
    private final Schema schema;
    private final WKBReader wkbReader = new WKBReader();

    /**
     * @param outputDir root output directory; tracks will be written under
     *                  {@code outputDir/tracks/date=YYYY-MM/part-00000.parquet}
     */
    public TrackWriter(Path outputDir) throws IOException {
        this.outputDir = outputDir;
        this.schema = loadSchema();
    }

    /**
     * Groups tracks by month partition and writes one Parquet file per month.
     *
     * @return number of records written
     */
    public long write(List<AisTrack> tracks) throws IOException {
        Map<String, List<AisTrack>> byMonth = new LinkedHashMap<>();
        for (AisTrack t : tracks) {
            byMonth.computeIfAbsent(t.getMonthPartition(), k -> new ArrayList<>()).add(t);
        }

        long total = 0;
        for (Map.Entry<String, List<AisTrack>> entry : byMonth.entrySet()) {
            total += writePartition(entry.getKey(), entry.getValue());
        }
        log.info("Wrote {} total track records across {} month partitions", total, byMonth.size());
        return total;
    }

    // ---- Internal ---------------------------------------------------------

    private long writePartition(String month, List<AisTrack> tracks) throws IOException {
        Path partDir = outputDir.resolve("tracks").resolve("date=" + month);
        Files.createDirectories(partDir);
        File outFile = partDir.resolve(String.format("part-%05d.parquet", PART_FILE_NUMBER)).toFile();

        Envelope bbox = computeBbox(tracks);
        Map<String, String> extraMeta = new HashMap<>();
        extraMeta.put("geo", buildGeoMetadata(bbox));

        try (ParquetWriter<GenericRecord> writer =
                     AvroParquetWriter.<GenericRecord>builder(new LocalOutputFile(outFile))
                             .withSchema(schema)
                             .withCompressionCodec(CompressionCodecName.SNAPPY)
                             .withExtraMetaData(extraMeta)
                             .build()) {

            for (AisTrack t : tracks) {
                writer.write(toRecord(t));
            }
        }

        log.info("  date={} → {} ({} records)", month, outFile.getPath(), tracks.size());
        return tracks.size();
    }

    private GenericRecord toRecord(AisTrack t) {
        GenericRecord rec = new GenericData.Record(schema);

        rec.put("mmsi",        t.getMmsi());
        rec.put("voyage_id",   t.getVoyageId());
        rec.put("geometry",    ByteBuffer.wrap(t.getGeometryWkb()));
        rec.put("start_time",  t.getStartTime().toEpochMilli() * 1_000L);
        rec.put("end_time",    t.getEndTime().toEpochMilli() * 1_000L);
        rec.put("point_count", t.getPointCount());
        rec.put("avg_sog",     t.getAvgSog());
        rec.put("distance_nm", t.getDistanceNm());

        return rec;
    }

    /**
     * Computes the union of all track geometry envelopes by parsing each WKB.
     * Falls back to world bbox if parsing fails for a track.
     */
    private Envelope computeBbox(List<AisTrack> tracks) {
        Envelope bbox = new Envelope();
        for (AisTrack t : tracks) {
            try {
                bbox.expandToInclude(wkbReader.read(t.getGeometryWkb()).getEnvelopeInternal());
            } catch (Exception e) {
                log.warn("Failed to parse WKB for track {}/{}, using world bbox", t.getMmsi(), t.getVoyageId());
                return new Envelope(-180, 180, -90, 90);
            }
        }
        return bbox;
    }

    // ---- GeoParquet metadata ---------------------------------------------

    private static String buildGeoMetadata(Envelope bbox) {
        return "{"
                + "\"version\":\"1.1.0\","
                + "\"primary_column\":\"geometry\","
                + "\"columns\":{"
                + "\"geometry\":{"
                + "\"encoding\":\"WKB\","
                + "\"geometry_types\":[\"LineString\"],"
                + String.format(java.util.Locale.ROOT,
                        "\"bbox\":[%f,%f,%f,%f],",
                        bbox.getMinX(), bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY())
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
        try (InputStream is = TrackWriter.class.getResourceAsStream("/avro/ais_track.avsc")) {
            if (is == null) {
                throw new IOException("Avro schema resource not found: /avro/ais_track.avsc");
            }
            return new Schema.Parser().parse(is);
        }
    }
}
