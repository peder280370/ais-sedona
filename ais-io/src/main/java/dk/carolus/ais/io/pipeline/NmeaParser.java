package dk.carolus.ais.io.pipeline;

import dk.carolus.ais.io.model.AisPosition;
import dk.carolus.ais.io.model.VesselMetadata;
import dk.tbsalling.aismessages.ais.messages.*;
import dk.tbsalling.aismessages.ais.messages.types.AISMessageType;
import dk.tbsalling.aismessages.nmea.messages.NMEAMessage;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
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
    public record ParseResult(List<AisPosition> positions, List<VesselMetadata> vessels) {}

    // -----------------------------------------------------------------------

    /**
     * Parses a NMEA file.
     *
     * @param file              path to the NMEA sentence file
     * @param fallbackTimestamp used for messages with no tag block timestamp;
     *                          if {@code null}, messages without a timestamp are skipped
     */
    public ParseResult parse(Path file, Instant fallbackTimestamp) throws IOException {
        try (var is = Files.newInputStream(file)) {
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

        var positions = new ArrayList<AisPosition>();
        var vessels = new ArrayList<VesselMetadata>();

        // Multi-sentence assembly state:
        // key = "channel|seqId"  →  array of NMEAMessage parts (null slots = not yet received)
        var partials = new LinkedHashMap<String, NMEAMessage[]>();
        var partialTimestamps = new LinkedHashMap<String, Instant>();

        int lineNum = 0, parseErrors = 0, skipped = 0;

        try (var reader = new BufferedReader(
                new InputStreamReader(is, StandardCharsets.UTF_8))) {

            String line;
            while ((line = reader.readLine()) != null) {
                lineNum++;
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;

                // --- Extract tag block timestamp and strip the tag block prefix ---
                var ts = fallbackTimestamp;
                var nmeaStr = line;

                var m = TAG_BLOCK_LINE.matcher(line);
                if (m.matches()) {
                    var tags = m.group(1);
                    nmeaStr = m.group(2);
                    var tm = TAG_TIMESTAMP.matcher(tags);
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
                var fields = nmeaStr.split(",", -1);
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

                var seqId = fields[3].trim();     // empty for single-sentence messages
                var channel = fields[4].trim();    // A or B

                try {
                    var nmea = NMEAMessage.fromString(nmeaStr);
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
        var key = channel + "|" + seqId;

        if (sentenceNumber == 1) {
            partials.put(key, new NMEAMessage[totalSentences]);
            if (ts != null) {
                partialTimestamps.put(key, ts);
            }
        }

        var parts = partials.get(key);
        if (parts == null || sentenceNumber > parts.length) {
            // Received a continuation without seeing the first part — discard
            return;
        }

        parts[sentenceNumber - 1] = nmea;

        if (sentenceNumber == totalSentences) {
            // Last part received — check that all slots are filled
            var complete = Arrays.stream(parts).allMatch(Objects::nonNull);
            if (complete) {
                var msgTs = partialTimestamps.remove(key);
                if (msgTs == null) msgTs = ts; // best-effort fallback
                partials.remove(key);
                decodeAndHandle(parts, msgTs, positions, vessels);
            }
        }
    }

    private void decodeAndHandle(NMEAMessage[] parts, Instant ts,
                                 List<AisPosition> positions, List<VesselMetadata> vessels) {
        try {
            var msg = (ts != null)
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

        var type = msg.getMessageType();

        if (type == AISMessageType.PositionReportClassAScheduled
                || type == AISMessageType.PositionReportClassAAssignedSchedule
                || type == AISMessageType.PositionReportClassAResponseToInterrogation) {

            var pos = (PositionReport) msg;
            var lat = pos.getLatitude();
            var lon = pos.getLongitude();
            if (lat == null || lon == null) return;

            var ns = pos.getNavigationStatus();
            var rawRot = pos.getRateOfTurn();

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

            var pos = (StandardClassBCSPositionReport) msg;
            var lat = pos.getLatitude();
            var lon = pos.getLongitude();
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

            var svd = (ShipAndVoyageData) msg;
            var shipType = svd.getShipType();

            var length = sumDimensions(svd.getToBow(), svd.getToStern());
            var beam = sumDimensions(svd.getToPort(), svd.getToStarboard());
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

            var cbd = (ClassBCSStaticDataReport) msg;
            var mmsi = (long) msg.getSourceMmsi().getMMSI();
            var partNum = cbd.getPartNumber();

            if (partNum == 0) {
                // Part A: contains ship name only
                vessels.add(new VesselMetadata(
                        mmsi, null, trimNmea(cbd.getShipName()), null,
                        null, null, null, null, null, null, ts));
            } else {
                // Part B: contains ship type, callsign, dimensions
                var shipType = cbd.getShipType();
                var length = sumDimensions(cbd.getToBow(), cbd.getToStern());
                var beam = sumDimensions(cbd.getToPort(), cbd.getToStarboard());
                vessels.add(new VesselMetadata(
                        mmsi, null, null, trimNmea(cbd.getCallsign()),
                        shipType != null ? shipType.getCode() : null,
                        shipType != null ? shipType.name() : null,
                        length, beam, null, null, ts));
            }
        }
        // Other message types (21=AtoN, 9=SAR, etc.) are silently ignored
    }

    private static Float sumDimensions(Integer a, Integer b) {
        if (a == null || b == null) return null;
        int v = a + b;
        return v > 0 ? (float) v : null;
    }

    /**
     * Trims whitespace and NMEA padding ({@code @}) from a string field.
     * Returns {@code null} if the result is empty.
     */
    static String trimNmea(String s) {
        if (s == null) return null;
        var trimmed = s.replace('@', ' ').trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
