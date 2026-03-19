package dk.carolus.ais.io.pipeline;

import dk.carolus.ais.io.model.VesselMetadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Writes {@link VesselMetadata} records to a GeoParquet-compatible Parquet file.
 *
 * <p>Output layout:
 * <pre>
 *   &lt;outputDir&gt;/vessels/part-00000.parquet
 * </pre>
 *
 * <p>The vessels table is not spatially partitioned (no geometry) — one flat file
 * per run.  Downstream consumers should deduplicate by {@code mmsi}, keeping the
 * most-recently-seen record.
 */
@Slf4j
public class VesselMetadataWriter {

    private final Path outputDir;
    private final Schema schema;

    /**
     * @param outputDir root output directory; the file will be written to
     *                  {@code outputDir/vessels/part-00000.parquet}
     */
    public VesselMetadataWriter(Path outputDir) throws IOException {
        this.outputDir = outputDir;
        this.schema = loadSchema();
    }

    /**
     * Writes all vessel metadata records to a single Parquet file.
     *
     * @return number of records written
     */
    public long write(List<VesselMetadata> vessels) throws IOException {
        if (vessels.isEmpty()) {
            log.info("No vessel metadata records to write");
            return 0;
        }

        var vesselDir = outputDir.resolve("vessels");
        Files.createDirectories(vesselDir);
        var outFile = vesselDir.resolve("part-00000.parquet").toFile();

        try (var writer = AvroParquetWriter.<GenericRecord>builder(new LocalOutputFile(outFile))
                             .withSchema(schema)
                             .withCompressionCodec(CompressionCodecName.SNAPPY)
                             .build()) {

            for (var v : vessels) {
                writer.write(toRecord(v));
            }
        }

        log.info("Wrote {} vessel metadata records to {}", vessels.size(), outFile.getPath());
        return vessels.size();
    }

    // -----------------------------------------------------------------------

    private GenericRecord toRecord(VesselMetadata v) {
        var rec = new GenericData.Record(schema);

        rec.put("mmsi", v.mmsi());
        rec.put("imo", v.imo());
        rec.put("vessel_name", v.vesselName());
        rec.put("callsign", v.callsign());
        rec.put("ship_type", v.shipType());
        rec.put("ship_type_desc", v.shipTypeDesc());
        rec.put("length_m", v.lengthM());
        rec.put("beam_m", v.beamM());
        rec.put("draught_m", v.draughtM());
        rec.put("destination", v.destination());
        rec.put("last_seen_us",
                v.lastSeen() != null ? v.lastSeen().toEpochMilli() * 1_000L : null);

        return rec;
    }

    private static Schema loadSchema() throws IOException {
        try (var is = VesselMetadataWriter.class
                .getResourceAsStream("/avro/vessel_metadata.avsc")) {
            if (is == null) {
                throw new IOException("Avro schema resource not found: /avro/vessel_metadata.avsc");
            }
            return new Schema.Parser().parse(is);
        }
    }
}
