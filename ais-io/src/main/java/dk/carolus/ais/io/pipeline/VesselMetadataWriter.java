package dk.carolus.ais.io.pipeline;

import dk.carolus.ais.io.model.VesselMetadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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

        Path vesselDir = outputDir.resolve("vessels");
        Files.createDirectories(vesselDir);
        File outFile = vesselDir.resolve("part-00000.parquet").toFile();

        try (ParquetWriter<GenericRecord> writer =
                     AvroParquetWriter.<GenericRecord>builder(new LocalOutputFile(outFile))
                             .withSchema(schema)
                             .withCompressionCodec(CompressionCodecName.SNAPPY)
                             .build()) {

            for (VesselMetadata v : vessels) {
                writer.write(toRecord(v));
            }
        }

        log.info("Wrote {} vessel metadata records to {}", vessels.size(), outFile.getPath());
        return vessels.size();
    }

    // -----------------------------------------------------------------------

    private GenericRecord toRecord(VesselMetadata v) {
        GenericRecord rec = new GenericData.Record(schema);

        rec.put("mmsi", v.getMmsi());
        rec.put("imo", v.getImo());
        rec.put("vessel_name", v.getVesselName());
        rec.put("callsign", v.getCallsign());
        rec.put("ship_type", v.getShipType());
        rec.put("ship_type_desc", v.getShipTypeDesc());
        rec.put("length_m", v.getLengthM());
        rec.put("beam_m", v.getBeamM());
        rec.put("draught_m", v.getDraughtM());
        rec.put("destination", v.getDestination());
        rec.put("last_seen_us",
                v.getLastSeen() != null ? v.getLastSeen().toEpochMilli() * 1_000L : null);

        return rec;
    }

    private static Schema loadSchema() throws IOException {
        try (InputStream is = VesselMetadataWriter.class
                .getResourceAsStream("/avro/vessel_metadata.avsc")) {
            if (is == null) {
                throw new IOException("Avro schema resource not found: /avro/vessel_metadata.avsc");
            }
            return new Schema.Parser().parse(is);
        }
    }
}
