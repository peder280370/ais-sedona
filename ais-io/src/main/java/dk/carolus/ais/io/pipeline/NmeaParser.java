package dk.carolus.ais.io.pipeline;

import dk.carolus.ais.io.model.AisPosition;
import dk.carolus.ais.io.model.VesselMetadata;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import dk.tbsalling.aismessages.ais.messages.AISMessage;
import dk.tbsalling.aismessages.ais.messages.ClassBCSStaticDataReport;
import dk.tbsalling.aismessages.ais.messages.Metadata;
import dk.tbsalling.aismessages.ais.messages.PositionReport;
import dk.tbsalling.aismessages.ais.messages.ShipAndVoyageData;
import dk.tbsalling.aismessages.ais.messages.StandardClassBCSPositionReport;
import dk.tbsalling.aismessages.ais.messages.types.AISMessageType;
import dk.tbsalling.aismessages.ais.messages.types.NavigationStatus;
import dk.tbsalling.aismessages.ais.messages.types.ShipType;
import dk.tbsalling.aismessages.nmea.messages.NMEAMessage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses NMEA 0183 AIS sentence files into {@link AisPosition} and
 * {@link VesselMetadata} objects.
 *
 * <p>Supports:
 * <ul>
 *   <li>Single-sentence and multi-sentence messages</li>
 *   <li>NMEA tag block timestamps: {@code \c:&lt;epoch&gt;,...\!AIVDM,...}</li>
 *   <li>Plain {@code !AIVDM} / {@code !AIVDO} lines (no tag block)</li>
 *   <li>Comment lines starting with {@code #} (silently skipped)</li>
 * </ul>
 *
 * <p>Message types handled:
 * <ul>
 *   <li>1/2/3 – Class A position → {@link AisPosition}</li>
 *   <li>18 – Class B position → {@link AisPosition}</li>
 *   <li>5 – Voyage/static data → {@link VesselMetadata}</li>
 *   <li>24 – Class B static (part A + B) → {@link VesselMetadata}</li>
 * </ul>
 */
@Slf4j
public class NmeaParser {

    /** Matches a line with an NMEA tag block prefix: {@code \tags\!AIVDM,...} */
    private static final Pattern TAG_BLOCK_LINE =
            Pattern.compile("^\\\\([^\\\\]*)\\\\(!AI(?:VDM|VDO).+)$");

    /** Extracts the {@code c:} epoch value from a tag block string. */
    private static final Pattern TAG_TIMESTAMP = Pattern.compile("c:(\\d+)");

    // -----------------------------------------------------------------------

    /** Result holder returned by {@link #parse}. */
    @Value
    public static class ParseResult {
        List<AisPosition> positions;
        List<VesselMetadata> vessels;
    }

    // -----------------------------------------------------------------------

    /**
     * Parses a NMEA file.
     *
     * @param file              path to the NMEA sentence file
     * @param fallbackTimestamp used for messages with no tag block timestamp;
     *                          if {@code null}, messages without a timestamp are skipped
     */
    public ParseResult parse(Path file, Instant fallbackTimestamp) throws IOException {
        try (InputStream is = Files.newInputStream(file)) {
            return parse(is, file.toString(), fallbackTimestamp);
        }
    }

    /**
     * Parses NMEA sentences from an {@link InputStream}.
     *
     * @param is                source of NMEA sentences (UTF-8 text, one sentence per line)
     * @param sourceName        label used in log messages
     * @param fallbackTimestamp used when no tag block {@code c:} timestamp is present;
     *                          pass {@code null} to skip timestampless messages
     */
    public ParseResult parse(InputStream is, String sourceName, Instant fallbackTimestamp)
            throws IOException {

        List<AisPosition> positions = new ArrayList<>();
        List<VesselMetadata> vessels = new ArrayList<>();

        // Multi-sentence assembly state:
        // key = "channel|seqId"  →  array of NMEAMessage parts (null slots = not yet received)
        Map<String, NMEAMessage[]> partials = new LinkedHashMap<>();
        Map<String, Instant> partialTimestamps = new LinkedHashMap<>();

        int lineNum = 0, parseErrors = 0, skipped = 0;

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(is, StandardCharsets.UTF_8))) {

            String line;
            while ((line = reader.readLine()) != null) {
                lineNum++;
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;

                // --- Extract tag block timestamp and strip the tag block prefix ---
                Instant ts = fallbackTimestamp;
                String nmeaStr = line;

                Matcher m = TAG_BLOCK_LINE.matcher(line);
                if (m.matches()) {
                    String tags = m.group(1);
                    nmeaStr = m.group(2);
                    Matcher tm = TAG_TIMESTAMP.matcher(tags);
                    if (tm.find()) {
                        long raw = Long.parseLong(tm.group(1));
                        // Values > 1e12 are milliseconds; smaller values are seconds
                        ts = raw > 1_000_000_000_000L
                                ? Instant.ofEpochMilli(raw)
                                : Instant.ofEpochSecond(raw);
                    }
                }

                if (!nmeaStr.startsWith("!AI")) continue;

                // --- Parse raw NMEA fields to drive multi-sentence assembly ---
                // Format: !AIVDM,<total>,<num>,<seqId>,<channel>,<payload>,<fill>*<checksum>
                String[] fields = nmeaStr.split(",", -1);
                if (fields.length < 6) {
                    log.debug("Line {}: too few NMEA fields, skipping", lineNum);
                    skipped++;
                    continue;
                }

                int totalSentences, sentenceNumber;
                try {
                    totalSentences = Integer.parseInt(fields[1].trim());
                    sentenceNumber = Integer.parseInt(fields[2].trim());
                } catch (NumberFormatException e) {
                    log.debug("Line {}: cannot parse sentence counts, skipping", lineNum);
                    skipped++;
                    continue;
                }

                String seqId = fields[3].trim();     // empty for single-sentence messages
                String channel = fields[4].trim();    // A or B

                try {
                    NMEAMessage nmea = NMEAMessage.fromString(nmeaStr);
                    processNmeaSentence(nmea, totalSentences, sentenceNumber, seqId, channel, ts,
                            partials, partialTimestamps, positions, vessels);
                } catch (Exception e) {
                    log.debug("Line {}: decode error: {} ({})", lineNum, e.getMessage(), nmeaStr);
                    parseErrors++;
                }
            }
        }

        log.info("NMEA '{}': {} lines read → {} positions, {} vessel records ({} parse errors, {} skipped)",
                sourceName, lineNum, positions.size(), vessels.size(), parseErrors, skipped);
        return new ParseResult(positions, vessels);
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    private void processNmeaSentence(
            NMEAMessage nmea, int totalSentences, int sentenceNumber,
            String seqId, String channel, Instant ts,
            Map<String, NMEAMessage[]> partials, Map<String, Instant> partialTimestamps,
            List<AisPosition> positions, List<VesselMetadata> vessels) {

        if (totalSentences == 1) {
            // Single-sentence message: decode immediately
            decodeAndHandle(new NMEAMessage[]{nmea}, ts, positions, vessels);
            return;
        }

        // Multi-sentence message: accumulate parts
        String key = channel + "|" + seqId;

        if (sentenceNumber == 1) {
            partials.put(key, new NMEAMessage[totalSentences]);
            if (ts != null) {
                partialTimestamps.put(key, ts);
            }
        }

        NMEAMessage[] parts = partials.get(key);
        if (parts == null || sentenceNumber > parts.length) {
            // Received a continuation without seeing the first part — discard
            return;
        }

        parts[sentenceNumber - 1] = nmea;

        if (sentenceNumber == totalSentences) {
            // Last part received — check that all slots are filled
            boolean complete = Arrays.stream(parts).allMatch(p -> p != null);
            if (complete) {
                Instant msgTs = partialTimestamps.remove(key);
                if (msgTs == null) msgTs = ts; // best-effort fallback
                partials.remove(key);
                decodeAndHandle(parts, msgTs, positions, vessels);
            }
        }
    }

    private void decodeAndHandle(NMEAMessage[] parts, Instant ts,
                                 List<AisPosition> positions, List<VesselMetadata> vessels) {
        try {
            AISMessage msg = (ts != null)
                    ? AISMessage.create(new Metadata("NMEA", ts), parts)
                    : AISMessage.create(parts);
            handleMessage(msg, ts, positions, vessels);
        } catch (Exception e) {
            log.debug("AIS decode error: {}", e.getMessage());
        }
    }

    private void handleMessage(AISMessage msg, Instant ts,
                                List<AisPosition> positions, List<VesselMetadata> vessels) {
        if (ts == null) {
            log.debug("Skipping {} message (no timestamp)", msg.getMessageType());
            return;
        }

        AISMessageType type = msg.getMessageType();

        if (type == AISMessageType.PositionReportClassAScheduled
                || type == AISMessageType.PositionReportClassAAssignedSchedule
                || type == AISMessageType.PositionReportClassAResponseToInterrogation) {

            PositionReport pos = (PositionReport) msg;
            Float lat = pos.getLatitude();
            Float lon = pos.getLongitude();
            if (lat == null || lon == null) return;

            NavigationStatus ns = pos.getNavigationStatus();
            Integer rawRot = pos.getRateOfTurn();

            positions.add(new AisPosition(
                    (long) msg.getSourceMmsi().getMMSI(),
                    ts,
                    lat.doubleValue(),
                    lon.doubleValue(),
                    pos.getSpeedOverGround(),
                    pos.getCourseOverGround(),
                    pos.getTrueHeading(),
                    ns != null ? ns.getCode() : null,
                    rawRot != null ? rawRot.floatValue() : null,
                    type.getCode()
            ));

        } else if (type == AISMessageType.StandardClassBCSPositionReport) {

            StandardClassBCSPositionReport pos = (StandardClassBCSPositionReport) msg;
            Float lat = pos.getLatitude();
            Float lon = pos.getLongitude();
            if (lat == null || lon == null) return;

            positions.add(new AisPosition(
                    (long) msg.getSourceMmsi().getMMSI(),
                    ts,
                    lat.doubleValue(),
                    lon.doubleValue(),
                    pos.getSpeedOverGround(),
                    pos.getCourseOverGround(),
                    pos.getTrueHeading(),
                    null,   // Class B has no navigation status
                    null,   // Class B has no rate of turn
                    type.getCode()
            ));

        } else if (type == AISMessageType.ShipAndVoyageRelatedData) {

            ShipAndVoyageData svd = (ShipAndVoyageData) msg;
            ShipType shipType = svd.getShipType();

            Float length = null;
            if (svd.getToBow() != null && svd.getToStern() != null) {
                int v = svd.getToBow() + svd.getToStern();
                if (v > 0) length = (float) v;
            }
            Float beam = null;
            if (svd.getToPort() != null && svd.getToStarboard() != null) {
                int v = svd.getToPort() + svd.getToStarboard();
                if (v > 0) beam = (float) v;
            }
            Long imo = null;
            if (svd.getImo() != null && svd.getImo().getIMO() != null) {
                int imoVal = svd.getImo().getIMO();
                if (imoVal > 0) imo = (long) imoVal;
            }

            vessels.add(new VesselMetadata(
                    (long) msg.getSourceMmsi().getMMSI(),
                    imo,
                    trimNmea(svd.getShipName()),
                    trimNmea(svd.getCallsign()),
                    shipType != null ? shipType.getCode() : null,
                    shipType != null ? shipType.name() : null,
                    length,
                    beam,
                    svd.getDraught(),
                    trimNmea(svd.getDestination()),
                    ts
            ));

        } else if (type == AISMessageType.ClassBCSStaticDataReport) {

            ClassBCSStaticDataReport cbd = (ClassBCSStaticDataReport) msg;
            long mmsi = (long) msg.getSourceMmsi().getMMSI();
            int partNum = cbd.getPartNumber();

            if (partNum == 0) {
                // Part A: contains ship name only
                vessels.add(new VesselMetadata(
                        mmsi, null, trimNmea(cbd.getShipName()), null,
                        null, null, null, null, null, null, ts));
            } else {
                // Part B: contains ship type, callsign, dimensions
                ShipType shipType = cbd.getShipType();
                Float length = null;
                if (cbd.getToBow() != null && cbd.getToStern() != null) {
                    int v = cbd.getToBow() + cbd.getToStern();
                    if (v > 0) length = (float) v;
                }
                Float beam = null;
                if (cbd.getToPort() != null && cbd.getToStarboard() != null) {
                    int v = cbd.getToPort() + cbd.getToStarboard();
                    if (v > 0) beam = (float) v;
                }
                vessels.add(new VesselMetadata(
                        mmsi, null, null, trimNmea(cbd.getCallsign()),
                        shipType != null ? shipType.getCode() : null,
                        shipType != null ? shipType.name() : null,
                        length, beam, null, null, ts));
            }
        }
        // Other message types (21=AtoN, 9=SAR, etc.) are silently ignored
    }

    /**
     * Trims whitespace and NMEA padding ({@code @}) from a string field.
     * Returns {@code null} if the result is empty.
     */
    static String trimNmea(String s) {
        if (s == null) return null;
        String trimmed = s.replace('@', ' ').trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
