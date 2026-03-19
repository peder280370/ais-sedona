package dk.carolus.ais.io.pipeline;

import dk.carolus.ais.io.model.AisPosition;
import dk.carolus.ais.io.model.VesselMetadata;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Parses pre-decoded AIS CSV files into {@link AisPosition} and {@link VesselMetadata} objects.
 *
 * <p>Column names are matched case-insensitively. Accepted synonyms:
 * <ul>
 *   <li>mmsi</li>
 *   <li>timestamp | basedatetime | time | datetime</li>
 *   <li>lat | latitude</li>
 *   <li>lon | lng | longitude</li>
 *   <li>sog (optional)</li>
 *   <li>cog (optional)</li>
 *   <li>heading (optional)</li>
 *   <li>nav_status | navstatus | status (optional)</li>
 *   <li>rot (optional)</li>
 *   <li>msg_type | msgtype | type (optional)</li>
 * </ul>
 *
 * <p>Vessel metadata columns (all optional, silently absent if not present):
 * <ul>
 *   <li>imo</li>
 *   <li>callsign</li>
 *   <li>name</li>
 *   <li>ship type | shiptype</li>
 *   <li>width (beam)</li>
 *   <li>length</li>
 *   <li>draught</li>
 *   <li>destination</li>
 *   <li>a, b, c, d (GPS antenna offsets; used as length/beam fallback)</li>
 * </ul>
 *
 * <p>Supported timestamp formats:
 * {@code 2024-01-15T10:30:00Z}, {@code 2024-01-15T10:30:00},
 * {@code 2024-01-15 10:30:00}, {@code 2024/01/15 10:30:00},
 * {@code 14/03/2026 00:00:00} (DMA format)
 *
 * <p>DMA column name synonyms:
 * <ul>
 *   <li>{@code # Timestamp} → timestamp (leading {@code #} stripped)</li>
 *   <li>{@code Navigational status} → nav_status (text values silently nulled)</li>
 * </ul>
 */
@Slf4j
public class CsvParser {

    private static final List<DateTimeFormatter> TIMESTAMP_FORMATS = List.of(
            DateTimeFormatter.ISO_INSTANT,
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
    );

    // ---- Public result type -----------------------------------------------

    /**
     * Combined result of a full CSV parse: position records plus one
     * {@link VesselMetadata} record per unique MMSI (deduplicated, best-known fields).
     */
    public record ParseResult(List<AisPosition> positions, List<VesselMetadata> vessels) {}

    // ---- Entry points -------------------------------------------------------

    /**
     * Full parse: returns both position records and deduplicated vessel metadata.
     */
    public ParseResult parseFull(Path csvFile) throws IOException {
        try (var is = Files.newInputStream(csvFile)) {
            return parseFull(is, csvFile.toString());
        }
    }

    /**
     * Full parse: returns both position records and deduplicated vessel metadata.
     */
    public ParseResult parseFull(InputStream is, String sourceName) throws IOException {
        var positions = new ArrayList<AisPosition>();
        var metaMap = new HashMap<Long, VesselMetadata>();
        var lineNum = 0L;
        var skipped = 0L;

        try (var reader = new BufferedReader(
                new InputStreamReader(is, StandardCharsets.UTF_8))) {

            var headerLine = reader.readLine();
            if (headerLine == null) {
                log.warn("Empty file: {}", sourceName);
                return new ParseResult(positions, List.of());
            }
            lineNum++;

            var colIndex = parseHeader(headerLine);
            if (!validateRequiredColumns(colIndex, sourceName)) {
                return new ParseResult(positions, List.of());
            }

            String line;
            while ((line = reader.readLine()) != null) {
                lineNum++;
                line = line.trim();
                if (line.isEmpty()) continue;

                try {
                    var cols = splitCsvLine(line);
                    var pos = buildPosition(cols, colIndex);
                    if (pos != null) {
                        positions.add(pos);
                        var meta = buildMetadata(cols, colIndex, pos.mmsi(), pos.timestamp());
                        if (meta != null) {
                            metaMap.merge(meta.mmsi(), meta, CsvParser::mergeMetadata);
                        }
                    }
                } catch (Exception e) {
                    log.debug("Skipping line {}: {} ({})", lineNum, e.getMessage(), line);
                    skipped++;
                }
            }
        }

        var vessels = new ArrayList<>(metaMap.values());
        log.info("Parsed {} positions and {} vessel records from {} ({} lines skipped)",
                positions.size(), vessels.size(), sourceName, skipped);
        return new ParseResult(positions, vessels);
    }

    /**
     * Streaming parse: calls {@code onPosition} for each parsed position and
     * {@code onVessel} for each vessel metadata record, one at a time, without
     * buffering the full file. Vessel records may be duplicated per MMSI — callers
     * that need one record per vessel should merge using {@link #mergeMetadata}.
     */
    public void streamFull(InputStream is, String sourceName,
                           Consumer<AisPosition> onPosition,
                           Consumer<VesselMetadata> onVessel) throws IOException {
        var lineNum = 0L;
        var posCount = 0L;
        var vesselCount = 0L;
        var skipped = 0L;

        try (var reader = new BufferedReader(
                new InputStreamReader(is, StandardCharsets.UTF_8))) {

            var headerLine = reader.readLine();
            if (headerLine == null) {
                log.warn("Empty file: {}", sourceName);
                return;
            }
            lineNum++;

            var colIndex = parseHeader(headerLine);
            if (!validateRequiredColumns(colIndex, sourceName)) {
                return;
            }

            String line;
            while ((line = reader.readLine()) != null) {
                lineNum++;
                line = line.trim();
                if (line.isEmpty()) continue;

                try {
                    var cols = splitCsvLine(line);
                    var pos = buildPosition(cols, colIndex);
                    if (pos != null) {
                        onPosition.accept(pos);
                        posCount++;
                        var meta = buildMetadata(cols, colIndex, pos.mmsi(), pos.timestamp());
                        if (meta != null) {
                            onVessel.accept(meta);
                            vesselCount++;
                        }
                    }
                } catch (Exception e) {
                    log.debug("Skipping line {}: {} ({})", lineNum, e.getMessage(), line);
                    skipped++;
                }
            }
        }

        log.info("Streamed {} positions and {} vessel records from {} ({} lines skipped)",
                posCount, vesselCount, sourceName, skipped);
    }

    /** Convenience method: returns only positions (backward compatible). */
    public List<AisPosition> parse(Path csvFile) throws IOException {
        return parseFull(csvFile).positions();
    }

    /** Convenience method: returns only positions (backward compatible). */
    public List<AisPosition> parse(InputStream is, String sourceName) throws IOException {
        return parseFull(is, sourceName).positions();
    }

    // ---- Header / column resolution ----------------------------------------

    private Map<String, Integer> parseHeader(String header) {
        var index = new HashMap<String, Integer>();
        var cols = splitCsvLine(header);
        for (int i = 0; i < cols.length; i++) {
            index.put(cols[i].trim().replaceAll("^#\\s*", "").toLowerCase(Locale.ROOT), i);
        }
        return index;
    }

    private boolean validateRequiredColumns(Map<String, Integer> idx, String source) {
        if (resolveColumn(idx, "mmsi") < 0) {
            log.warn("Skipping {}: missing required column 'mmsi'", source);
            return false;
        }
        if (resolveColumn(idx, "timestamp", "basedatetime", "time", "datetime") < 0) {
            log.warn("Skipping {}: missing required timestamp column", source);
            return false;
        }
        if (resolveColumn(idx, "lat", "latitude") < 0) {
            log.warn("Skipping {}: missing required column 'lat'", source);
            return false;
        }
        if (resolveColumn(idx, "lon", "lng", "longitude") < 0) {
            log.warn("Skipping {}: missing required column 'lon'", source);
            return false;
        }
        return true;
    }

    // ---- Position building --------------------------------------------------

    private AisPosition buildPosition(String[] cols, Map<String, Integer> idx) {
        var mmsi        = parseLong(cols, resolveColumn(idx, "mmsi"));
        var ts          = parseTimestamp(cols, resolveColumn(idx, "timestamp", "basedatetime", "time", "datetime"));
        var lat         = parseDouble(cols, resolveColumn(idx, "lat", "latitude"));
        var lon         = parseDouble(cols, resolveColumn(idx, "lon", "lng", "longitude"));
        var sog         = parseFloat(cols, resolveColumn(idx, "sog"));
        var cog         = parseFloat(cols, resolveColumn(idx, "cog"));
        var heading     = parseInt(cols, resolveColumn(idx, "heading"));
        var navStatus   = parseInt(cols, resolveColumn(idx, "nav_status", "navstatus", "status", "navigational status"));
        var rot         = parseFloat(cols, resolveColumn(idx, "rot"));
        var msgType     = parseInt(cols, resolveColumn(idx, "msg_type", "msgtype", "type"));

        return new AisPosition(mmsi, ts, lat, lon, sog, cog, heading, navStatus, rot, msgType);
    }

    // ---- Vessel metadata building ------------------------------------------

    private VesselMetadata buildMetadata(String[] cols, Map<String, Integer> idx,
                                         long mmsi, Instant ts) {
        var imoIdx      = resolveColumn(idx, "imo");
        var callsignIdx = resolveColumn(idx, "callsign");
        var nameIdx     = resolveColumn(idx, "name");
        var typeIdx     = resolveColumn(idx, "ship type", "shiptype");
        var widthIdx    = resolveColumn(idx, "width");
        var lengthIdx   = resolveColumn(idx, "length");
        var draughtIdx  = resolveColumn(idx, "draught");
        var destIdx     = resolveColumn(idx, "destination");
        var sizeAIdx    = resolveColumn(idx, "a");
        var sizeBIdx    = resolveColumn(idx, "b");
        var sizeCIdx    = resolveColumn(idx, "c");
        var sizeDIdx    = resolveColumn(idx, "d");

        // If none of the metadata columns are present in this file, skip entirely
        if (imoIdx < 0 && callsignIdx < 0 && nameIdx < 0 && typeIdx < 0
                && widthIdx < 0 && lengthIdx < 0 && draughtIdx < 0 && destIdx < 0) {
            return null;
        }

        var imo          = parseImo(cols, imoIdx);
        var vesselName   = nullIfUnknown(cell(cols, nameIdx));
        var callsign     = nullIfUnknown(cell(cols, callsignIdx));
        var shipTypeRaw  = nullIfUnknown(cell(cols, typeIdx));
        Integer shipType     = null;
        String  shipTypeDesc = null;
        if (shipTypeRaw != null) {
            try {
                shipType = (int) Double.parseDouble(shipTypeRaw);
            } catch (NumberFormatException e) {
                shipTypeDesc = shipTypeRaw;
            }
        }
        var lengthM      = parseFloat(cols, lengthIdx);
        var beamM        = parseFloat(cols, widthIdx);
        var draughtM     = parseFloat(cols, draughtIdx);
        var destination  = nullIfUnknown(cell(cols, destIdx));

        // Fallback: derive length/beam from GPS antenna offsets A+B / C+D
        if (lengthM == null) {
            var a = parseFloat(cols, sizeAIdx);
            var b = parseFloat(cols, sizeBIdx);
            if (a != null && b != null) lengthM = a + b;
        }
        if (beamM == null) {
            var c = parseFloat(cols, sizeCIdx);
            var d = parseFloat(cols, sizeDIdx);
            if (c != null && d != null) beamM = c + d;
        }

        // Skip rows with no useful metadata (e.g. base stations: all Unknown/empty)
        if (imo == null && vesselName == null && callsign == null && shipTypeDesc == null
                && lengthM == null && beamM == null && draughtM == null && destination == null) {
            return null;
        }

        return new VesselMetadata(mmsi, imo, vesselName, callsign,
                shipType, shipTypeDesc, lengthM, beamM, draughtM, destination, ts);
    }

    /**
     * Merges two metadata records for the same MMSI.
     * For each field, the incoming (newer) non-null value wins over the existing value.
     * {@code lastSeen} is always taken from the incoming record (latest timestamp).
     */
    public static VesselMetadata mergeMetadata(VesselMetadata existing, VesselMetadata incoming) {
        return new VesselMetadata(
                existing.mmsi(),
                firstNonNull(incoming.imo(),          existing.imo()),
                firstNonNull(incoming.vesselName(),   existing.vesselName()),
                firstNonNull(incoming.callsign(),     existing.callsign()),
                firstNonNull(incoming.shipType(),     existing.shipType()),
                firstNonNull(incoming.shipTypeDesc(), existing.shipTypeDesc()),
                firstNonNull(incoming.lengthM(),      existing.lengthM()),
                firstNonNull(incoming.beamM(),        existing.beamM()),
                firstNonNull(incoming.draughtM(),     existing.draughtM()),
                firstNonNull(incoming.destination(),  existing.destination()),
                incoming.lastSeen()
        );
    }

    private static <T> T firstNonNull(T a, T b) {
        return a != null ? a : b;
    }

    // ---- Column resolution -------------------------------------------------

    private int resolveColumn(Map<String, Integer> idx, String... names) {
        for (var name : names) {
            Integer i = idx.get(name);
            if (i != null) return i;
        }
        return -1;
    }

    // ---- Value parsers -----------------------------------------------------

    private long parseLong(String[] cols, int colIdx) {
        return Long.parseLong(cell(cols, colIdx));
    }

    private double parseDouble(String[] cols, int colIdx) {
        return Double.parseDouble(cell(cols, colIdx));
    }

    private Float parseFloat(String[] cols, int colIdx) {
        if (colIdx < 0) return null;
        var v = cell(cols, colIdx);
        if (v.isEmpty()) return null;
        try {
            return Float.parseFloat(v);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Integer parseInt(String[] cols, int colIdx) {
        if (colIdx < 0) return null;
        var v = cell(cols, colIdx);
        if (v.isEmpty()) return null;
        try {
            // Tolerate float-formatted integers (e.g. "1.0")
            return (int) Double.parseDouble(v);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Long parseImo(String[] cols, int colIdx) {
        if (colIdx < 0) return null;
        var v = nullIfUnknown(cell(cols, colIdx));
        if (v == null) return null;
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Instant parseTimestamp(String[] cols, int colIdx) {
        var raw = cell(cols, colIdx);
        for (var fmt : TIMESTAMP_FORMATS) {
            try {
                // Try direct ISO instant first
                if (raw.endsWith("Z") || raw.contains("+")) {
                    return Instant.from(fmt.parse(raw));
                }
                // Otherwise treat as UTC local datetime
                return LocalDateTime.parse(raw, fmt).toInstant(ZoneOffset.UTC);
            } catch (DateTimeParseException ignored) {
                // try next format
            }
        }
        throw new IllegalArgumentException("Cannot parse timestamp: " + raw);
    }

    private String cell(String[] cols, int colIdx) {
        if (colIdx < 0 || colIdx >= cols.length) return "";
        return cols[colIdx].trim();
    }

    /**
     * Returns {@code null} for blank, "unknown", "undefined", or "n/a" values
     * (case-insensitive); otherwise returns the value unchanged.
     */
    private static String nullIfUnknown(String v) {
        if (v == null || v.isEmpty()) return null;
        return switch (v.toLowerCase(Locale.ROOT)) {
            case "unknown", "undefined", "n/a" -> null;
            default -> v;
        };
    }

    // ---- CSV tokenizer (handles double-quoted fields) ----------------------

    static String[] splitCsvLine(String line) {
        var tokens = new ArrayList<String>();
        var sb = new StringBuilder();
        var inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    sb.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                tokens.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        tokens.add(sb.toString());
        return tokens.toArray(new String[0]);
    }
}
