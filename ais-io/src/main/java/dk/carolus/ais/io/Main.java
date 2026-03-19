package dk.carolus.ais.io;

import dk.carolus.ais.io.model.AisPosition;
import dk.carolus.ais.io.model.VesselMetadata;
import dk.carolus.ais.io.pipeline.CsvParser;
import dk.carolus.ais.io.pipeline.GeoParquetWriter;
import dk.carolus.ais.io.pipeline.NmeaParser;
import dk.carolus.ais.io.pipeline.TrackBuilder;
import dk.carolus.ais.io.pipeline.TrackWriter;
import dk.carolus.ais.io.pipeline.Validator;
import dk.carolus.ais.io.pipeline.VesselMetadataWriter;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Entry point for the Phase 2 AIS ingestion pipeline.
 *
 * <p>Usage:
 * <pre>
 *   java -jar ais-io.jar &lt;input&gt; &lt;output-dir&gt; [--format csv|nmea] [--date YYYY-MM-DD] [--limit N]
 * </pre>
 *
 * <p>Arguments:
 * <ul>
 *   <li>{@code input}       – CSV or NMEA sentence file (plain or ZIP-compressed),
 *                             or a date string ({@code YYYY-MM-DD}) when {@code --format dma}</li>
 *   <li>{@code output-dir}  – root output directory</li>
 *   <li>{@code --format}    – {@code csv}, {@code nmea}, or {@code dma};
 *                             auto-detected from extension if omitted.
 *                             {@code dma}: first argument is a date; data is downloaded from
 *                             {@code http://aisdata.ais.dk/aisdk-<date>.zip}</li>
 *   <li>{@code --date}      – fallback date ({@code YYYY-MM-DD}) for NMEA messages without a
 *                             tag-block timestamp; defaults to today</li>
 *   <li>{@code --limit N}   – process at most N data lines (0 = no limit; useful for sampling)</li>
 * </ul>
 *
 * <p>Output layout:
 * <pre>
 *   &lt;output-dir&gt;/positions/date=YYYY-MM-DD/h3_r3=&lt;cell&gt;/part-00000.parquet
 *   &lt;output-dir&gt;/vessels/part-00000.parquet
 * </pre>
 */
@Slf4j
public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            log.error("Usage: ais-io <input> <output-dir> [--format csv|nmea|dma] [--date YYYY-MM-DD] [--limit N]");
            System.exit(1);
        }

        String inputArg = args[0];
        Path outputDir = Paths.get(args[1]);

        // Parse all optional flags first, before any path validation
        String format = null;
        Instant fallbackTimestamp = LocalDate.now(ZoneOffset.UTC).atStartOfDay().toInstant(ZoneOffset.UTC);
        int limit = 0;

        for (int i = 2; i < args.length - 1; i++) {
            if ("--format".equals(args[i])) {
                format = args[++i].toLowerCase();
            } else if ("--date".equals(args[i])) {
                fallbackTimestamp = LocalDate.parse(args[++i])
                        .atStartOfDay(ZoneOffset.UTC).toInstant();
            } else if ("--limit".equals(args[i])) {
                limit = Integer.parseInt(args[++i]);
            }
        }

        if ("dma".equals(format)) {
            try {
                LocalDate.parse(inputArg);
            } catch (DateTimeParseException e) {
                log.error("--format dma requires a date argument (yyyy-MM-dd), got: {}", inputArg);
                System.exit(1);
            }
        } else {
            Path inputFile = Paths.get(inputArg);
            if (!Files.isRegularFile(inputFile)) {
                log.error("Input file not found: {}", inputFile);
                System.exit(1);
            }
            if (format == null) format = detectFormat(inputFile);
        }

        Files.createDirectories(outputDir);

        log.info("=== AIS Ingester — Phase 2 ===");
        if ("dma".equals(format)) {
            log.info("Input:  DMA download for {} (limit={})", inputArg, limit > 0 ? limit : "none");
        } else {
            log.info("Input:  {} (format={}, limit={})", Paths.get(inputArg).toAbsolutePath(), format, limit > 0 ? limit : "none");
        }
        log.info("Output: {}", outputDir.toAbsolutePath());

        List<AisPosition> positions;
        List<VesselMetadata> vesselMetadata;

        try (InputStream is = "dma".equals(format)
                ? openDmaInputStream(inputArg, limit)
                : openInputStream(Paths.get(inputArg), format, limit)) {
            if ("nmea".equals(format)) {
                // --- NMEA path ---
                log.info("--- Step 1: Parsing NMEA ---");
                NmeaParser parser = new NmeaParser();
                NmeaParser.ParseResult parsed = parser.parse(is, inputArg, fallbackTimestamp);
                positions = parsed.getPositions();
                vesselMetadata = parsed.getVessels();

            } else {
                // --- CSV / DMA path ---
                log.info("--- Step 1: Parsing CSV ---");
                CsvParser parser = new CsvParser();
                CsvParser.ParseResult parsed = parser.parseFull(is, inputArg);
                positions = parsed.getPositions();
                vesselMetadata = parsed.getVessels();
            }
        }

        // Validate & deduplicate positions
        log.info("--- Step 2: Validating positions ---");
        Validator validator = new Validator();
        List<AisPosition> valid = validator.validate(positions);

        // Write positions GeoParquet (date + H3 partitioned)
        if (!valid.isEmpty()) {
            log.info("--- Step 3: Writing positions GeoParquet ---");
            GeoParquetWriter posWriter = new GeoParquetWriter(outputDir);
            long written = posWriter.write(valid);
            log.info("Positions: {} records written", written);
        } else {
            log.warn("No valid position records — skipping positions write");
        }

        // Write vessel metadata
        if (!vesselMetadata.isEmpty()) {
            log.info("--- Step 4: Writing vessel metadata ---");
            VesselMetadataWriter vesselWriter = new VesselMetadataWriter(outputDir);
            long written = vesselWriter.write(vesselMetadata);
            log.info("Vessels: {} records written", written);
        } else {
            log.info("No vessel metadata records — position-only input");
        }

        // Build and write voyage tracks
        log.info("--- Step 5: Building voyage tracks ---");
        TrackBuilder trackBuilder = new TrackBuilder();
        TrackBuilder.BuildResult trackResult = trackBuilder.build(valid);
        log.info("Tracks: {} built, {} downsampled, {} skipped single-point",
                trackResult.getTracks().size(),
                trackResult.getDownsampledPositions(),
                trackResult.getSkippedSinglePoint());
        if (!trackResult.getTracks().isEmpty()) {
            log.info("--- Step 5b: Writing tracks GeoParquet ---");
            long written = new TrackWriter(outputDir).write(trackResult.getTracks());
            log.info("Tracks: {} records written", written);
        }

        log.info("=== Done ===");
    }

    /**
     * Downloads the DMA AIS ZIP for the given date and returns an {@link InputStream}
     * positioned at the first CSV entry inside the archive.
     *
     * <p>URL pattern: {@code http://aisdata.ais.dk/aisdk-<date>.zip}
     */
    public static InputStream openDmaInputStream(String date, int limit) throws IOException {
        String url = "http://aisdata.ais.dk/aisdk-" + date + ".zip";
        log.info("Downloading DMA AIS data from {}", url);
        ZipInputStream zis = new ZipInputStream(URI.create(url).toURL().openStream());
        ZipEntry entry;
        while ((entry = zis.getNextEntry()) != null) {
            if (!entry.isDirectory() && entry.getName().toLowerCase().endsWith(".csv")) {
                log.info("ZIP: selected entry '{}'", entry.getName());
                if (limit <= 0) return zis;
                try { return applyLimit(zis, limit); } finally { zis.close(); }
            }
        }
        zis.close();
        throw new IOException("No CSV entry found in DMA ZIP for date " + date);
    }

    /**
     * Opens an {@link InputStream} for the given file, transparently handling ZIP archives
     * and optional line limiting.
     *
     * <p>If the file is a ZIP archive, the first entry whose name matches the expected
     * format extension is selected. If {@code limit > 0}, only the first {@code limit}
     * non-blank lines are returned (header line counts toward the limit for CSV files).
     */
    static InputStream openInputStream(Path file, String format, int limit) throws IOException {
        boolean isZip = file.getFileName().toString().toLowerCase().endsWith(".zip");
        InputStream raw = isZip ? extractZipEntry(file, format) : Files.newInputStream(file);

        if (limit <= 0) return raw;  // stream directly, no buffering

        try {
            return applyLimit(raw, limit);
        } finally {
            raw.close();
        }
    }

    /**
     * Opens the ZIP file and returns an {@link InputStream} positioned at the first
     * entry whose name matches the format (csv → {@code .csv}; nmea → {@code .nmea},
     * {@code .aivdm}, {@code .txt}).  Falls back to the first non-directory entry.
     *
     * <p>The caller is responsible for closing the returned stream.
     */
    private static InputStream extractZipEntry(Path zipFile, String format) throws IOException {
        // Pass 1: find target entry name (no entry data read)
        String selected = null;
        String firstNonDir = null;
        try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zipFile))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.isDirectory()) continue;
                if (firstNonDir == null) firstNonDir = entry.getName();
                if (matchesFormat(entry.getName(), format)) { selected = entry.getName(); break; }
            }
        }
        if (selected == null) selected = firstNonDir;
        if (selected == null) throw new IOException("ZIP archive contains no usable entries: " + zipFile);

        if (selected.equals(firstNonDir) && !matchesFormat(selected, format)) {
            log.info("ZIP: no entry matched format '{}', using first entry '{}'", format, selected);
        }

        // Pass 2: stream the selected entry directly
        String target = selected;
        ZipInputStream zis = new ZipInputStream(Files.newInputStream(zipFile));
        ZipEntry entry;
        while ((entry = zis.getNextEntry()) != null) {
            if (entry.getName().equals(target)) {
                log.info("ZIP: selected entry '{}'", entry.getName());
                return zis; // caller closes
            }
        }
        zis.close();
        throw new IOException("ZIP entry not found on second pass: " + target);
    }

    private static boolean matchesFormat(String entryName, String format) {
        String lower = entryName.toLowerCase();
        return "csv".equals(format)
                ? lower.endsWith(".csv")
                : (lower.endsWith(".nmea") || lower.endsWith(".aivdm") || lower.endsWith(".txt"));
    }

    /**
     * Reads at most {@code maxLines} lines from {@code is} (UTF-8) and returns them
     * as a bounded {@link ByteArrayInputStream}.  Only the requested lines are read.
     */
    private static InputStream applyLimit(InputStream is, int maxLines) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        int count = 0;
        String line;
        while (count < maxLines && (line = reader.readLine()) != null) {
            if (count > 0) sb.append('\n');
            sb.append(line);
            count++;
        }
        // reader intentionally not closed — caller manages the original stream
        return new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Guesses the input format from the file extension (or ZIP entry name).
     * For ZIP files, first strips the {@code .zip} suffix and inspects the outer name;
     * if that yields no recognisable extension, opens the ZIP and checks the first entry.
     * Falls back to {@code "nmea"} for any unrecognised extension.
     */
    static String detectFormat(Path file) throws IOException {
        String filename = file.getFileName().toString();
        String lower = filename.toLowerCase();
        if (lower.endsWith(".zip")) {
            String inner = lower.substring(0, lower.length() - 4);
            if (inner.endsWith(".csv")) return "csv";
            // Outer name gives no hint — peek at the first ZIP entry name
            try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(file))) {
                ZipEntry entry;
                while ((entry = zis.getNextEntry()) != null) {
                    if (!entry.isDirectory()) {
                        return detectFormatFromName(entry.getName());
                    }
                }
            }
            return "nmea"; // ZIP is empty — default
        }
        return detectFormatFromName(filename);
    }

    /** Infers format from a plain filename (no ZIP handling). */
    private static String detectFormatFromName(String filename) {
        if (filename.toLowerCase().endsWith(".csv")) return "csv";
        return "nmea"; // .nmea, .aivdm, .txt, or anything else
    }
}
