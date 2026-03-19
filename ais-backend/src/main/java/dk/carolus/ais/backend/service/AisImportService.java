package dk.carolus.ais.backend.service;

import dk.carolus.ais.backend.model.ImportResult;
import dk.carolus.ais.io.Main;
import dk.carolus.ais.io.model.AisPosition;
import dk.carolus.ais.io.model.VesselMetadata;
import dk.carolus.ais.io.pipeline.*;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.zip.ZipInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

@ApplicationScoped
public class AisImportService {

    @ConfigProperty(name = "ais.data.dir")
    String dataDir;

    // Guards VesselMetadataWriter (writes to a fixed filename)
    private final ReentrantLock writeLock = new ReentrantLock();

    // Pre-accumulation interval for track building: 60 s is coarser than
    // TrackBuilder.DOWNSAMPLE_INTERVAL_SEC (10 s) but reduces the per-MMSI
    // in-memory buffer by ~6x for large files. TrackBuilder re-downsamples
    // to 10 s on its own sorted input, so track quality is unaffected.
    private static final long TRACK_DOWNSAMPLE_SEC = 60L;

    public ImportResult importFile(InputStream inputStream, String filename, String format, LocalDate date) throws Exception {
        if (filename != null && filename.toLowerCase().endsWith(".zip")) {
            ZipInputStream zis = new ZipInputStream(inputStream);
            zis.getNextEntry();
            inputStream = zis;
        }
        long start = System.currentTimeMillis();
        Instant fallback = date != null
                ? date.atStartOfDay(ZoneOffset.UTC).toInstant()
                : LocalDate.now(ZoneOffset.UTC).atStartOfDay().toInstant(ZoneOffset.UTC);

        Path outDir = Paths.get(dataDir);
        long positionsWritten = 0;
        long vesselRecords = 0;
        long tracksBuilt = 0;

        if ("nmea".equalsIgnoreCase(format)) {
            // NMEA files are typically much smaller — keep the batch path unchanged
            var parser = new NmeaParser();
            var result = parser.parse(inputStream, "upload", fallback);
            var positions = result.positions();
            var vessels = result.vessels();

            var validator = new Validator();
            var valid = validator.validate(positions);

            writeLock.lock();
            try {
                if (!valid.isEmpty()) {
                    positionsWritten = new GeoParquetWriter(outDir).write(valid);
                }
                if (!vessels.isEmpty()) {
                    vesselRecords = new VesselMetadataWriter(outDir).write(vessels);
                }
                var trackResult = new TrackBuilder().build(valid);
                if (!trackResult.tracks().isEmpty()) {
                    tracksBuilt = new TrackWriter(outDir).write(trackResult.tracks());
                }
            } finally {
                writeLock.unlock();
            }

        } else {
            // CSV: streaming pipeline — never buffers the full file in memory.
            // Positions are validated and written to Parquet one record at a time.
            // Vessel metadata is merged into a per-MMSI map (one entry per vessel, small).
            // Only downsampled positions are kept per MMSI for track building.
            writeLock.lock();
            try {
                var validator = new Validator();
                var vesselMap = new HashMap<Long, VesselMetadata>();
                var trackPositions = new HashMap<Long, List<AisPosition>>();
                var lastKeptTs = new HashMap<Long, Instant>();

                try (GeoParquetWriter posWriter = new GeoParquetWriter(outDir)) {
                    try {
                        new CsvParser().streamFull(inputStream, "upload",
                                pos -> {
                                    if (validator.accept(pos)) {
                                        try { posWriter.write(pos); }
                                        catch (IOException e) { throw new UncheckedIOException(e); }
                                        // Inline downsampling: accumulate only one position per
                                        // MMSI per TRACK_DOWNSAMPLE_SEC for track building
                                        Instant last = lastKeptTs.get(pos.mmsi());
                                        if (last == null || Duration.between(last, pos.timestamp())
                                                .getSeconds() >= TRACK_DOWNSAMPLE_SEC) {
                                            trackPositions.computeIfAbsent(
                                                    pos.mmsi(), k -> new ArrayList<>()).add(pos);
                                            lastKeptTs.put(pos.mmsi(), pos.timestamp());
                                        }
                                    }
                                },
                                vessel -> vesselMap.merge(vessel.mmsi(), vessel,
                                        CsvParser::mergeMetadata)
                        );
                    } catch (UncheckedIOException e) {
                        throw e.getCause();
                    }
                    positionsWritten = posWriter.getTotalWritten();
                }

                if (!vesselMap.isEmpty()) {
                    vesselRecords = new VesselMetadataWriter(outDir)
                            .write(new ArrayList<>(vesselMap.values()));
                }

                var trackResult = new TrackBuilder().buildFromGrouped(trackPositions);
                if (!trackResult.tracks().isEmpty()) {
                    tracksBuilt = new TrackWriter(outDir).write(trackResult.tracks());
                }

            } finally {
                writeLock.unlock();
            }
        }

        long durationMs = System.currentTimeMillis() - start;
        return new ImportResult(positionsWritten, vesselRecords, tracksBuilt, durationMs);
    }

    public ImportResult importDma(String date) throws Exception {
        long start = System.currentTimeMillis();
        try (InputStream is = Main.openDmaInputStream(date, 0)) {
            ImportResult result = importFile(is, null, "csv", LocalDate.parse(date));
            return new ImportResult(result.positionsWritten(), result.vesselRecords(),
                    result.tracksBuilt(), System.currentTimeMillis() - start);
        }
    }
}
