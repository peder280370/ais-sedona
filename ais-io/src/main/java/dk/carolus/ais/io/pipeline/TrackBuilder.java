package dk.carolus.ais.io.pipeline;

import dk.carolus.ais.io.model.AisPosition;
import dk.carolus.ais.io.model.AisTrack;
import lombok.extern.slf4j.Slf4j;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.WKBWriter;

import java.time.Instant;
import java.util.*;

/**
 * Builds voyage track segments from validated AIS positions.
 *
 * <p>Algorithm per MMSI:
 * <ol>
 *   <li>Sort positions by timestamp.</li>
 *   <li>Downsample: keep a position only if it is ≥ 10 s after the last kept position.</li>
 *   <li>Sequential segmentation: split on time gap &gt; 6 h, navigation-status change
 *       (stationary ↔ non-stationary), or port-call detection (≥ 6 slow positions
 *       spanning ≥ 30 min with SOG &lt; 0.5 kn).</li>
 *   <li>Skip segments with fewer than 2 points.</li>
 * </ol>
 */
@Slf4j
public class TrackBuilder {

    static final long DOWNSAMPLE_INTERVAL_SEC    = 10;
    static final long GAP_THRESHOLD_SEC          = 6L * 3600;
    static final float PORT_CALL_MAX_SOG         = 0.5f;
    static final long PORT_CALL_MIN_DURATION_SEC = 30L * 60;
    static final int PORT_CALL_MIN_POINTS        = 6;
    static final int NAV_STATUS_ANCHORED         = 1;
    static final int NAV_STATUS_MOORED           = 5;

    private static final GeometryFactory GEOM_FACTORY =
            new GeometryFactory(new PrecisionModel(), 4326);

    // -----------------------------------------------------------------------

    /** Aggregated result from a single {@link #build} call. */
    public record BuildResult(
            List<AisTrack> tracks,
            int inputPositions,
            int downsampledPositions,
            int skippedSinglePoint) {}

    // -----------------------------------------------------------------------

    /**
     * Builds voyage tracks from all positions in the supplied list.
     *
     * @param positions validated AIS positions (any order, any number of MMSIs)
     * @return aggregated build result
     */
    public BuildResult build(List<AisPosition> positions) {
        // Group by MMSI
        var byMmsi = new LinkedHashMap<Long, List<AisPosition>>();
        for (var p : positions) {
            byMmsi.computeIfAbsent(p.mmsi(), k -> new ArrayList<>()).add(p);
        }
        return buildFromGrouped(byMmsi, positions.size());
    }

    /**
     * Builds tracks from already-grouped positions (avoids re-grouping and a redundant flat copy).
     *
     * @param byMmsi pre-grouped positions; each list may be in any order
     * @return aggregated build result
     */
    public BuildResult buildFromGrouped(Map<Long, List<AisPosition>> byMmsi) {
        var totalInput = 0;
        for (var list : byMmsi.values()) totalInput += list.size();
        return buildFromGrouped(byMmsi, totalInput);
    }

    private BuildResult buildFromGrouped(Map<Long, List<AisPosition>> byMmsi, int inputCount) {
        var allTracks = new ArrayList<AisTrack>();
        var totalDownsampled = 0;
        var totalSkipped = 0;
        var wkbWriter = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);

        for (Map.Entry<Long, List<AisPosition>> entry : byMmsi.entrySet()) {
            var sorted = new ArrayList<>(entry.getValue());
            sorted.sort(Comparator.comparing(AisPosition::timestamp));

            var downsampled = downsample(sorted);
            totalDownsampled += (sorted.size() - downsampled.size());

            int[] skipped = {0};
            var tracks = segment(downsampled, skipped, wkbWriter);
            allTracks.addAll(tracks);
            totalSkipped += skipped[0];
        }

        log.info("TrackBuilder: {} input, {} downsampled, {} tracks, {} skipped single-point",
                inputCount, totalDownsampled, allTracks.size(), totalSkipped);

        return new BuildResult(allTracks, inputCount, totalDownsampled, totalSkipped);
    }

    // ---- Downsampling -----------------------------------------------------

    private static List<AisPosition> downsample(List<AisPosition> sorted) {
        var kept = new ArrayList<AisPosition>();
        Instant lastKept = null;
        for (var p : sorted) {
            if (lastKept == null
                    || java.time.Duration.between(lastKept, p.timestamp()).getSeconds()
                       >= DOWNSAMPLE_INTERVAL_SEC) {
                kept.add(p);
                lastKept = p.timestamp();
            }
        }
        return kept;
    }

    // ---- Segmentation -----------------------------------------------------

    private static List<AisTrack> segment(List<AisPosition> positions,
                                          int[] skippedOut,
                                          WKBWriter wkbWriter) {
        var tracks = new ArrayList<AisTrack>();
        var seg = new ArrayList<AisPosition>();

        // Port-call tracking state (indices into `seg`)
        var portCallStart = -1;  // index of first slow position in current run
        var portCallCount = 0;   // length of current slow run

        for (var curr : positions) {
            if (!seg.isEmpty()) {
                var prev = seg.getLast();
                var gapSec = java.time.Duration.between(
                        prev.timestamp(), curr.timestamp()).getSeconds();

                var split = false;

                // Rule 1: time gap
                if (gapSec > GAP_THRESHOLD_SEC) {
                    split = true;
                }
                // Rule 2: nav-status change (stationary ↔ non-stationary)
                else if (isStationaryStatus(prev) != isStationaryStatus(curr)) {
                    split = true;
                }

                if (split) {
                    emitSegment(seg, tracks, skippedOut, wkbWriter);
                    seg = new ArrayList<>();
                    portCallStart = -1;
                    portCallCount = 0;
                }
            }

            // Add current position to segment
            seg.add(curr);
            var currIdx = seg.size() - 1;

            // Rule 3: port-call detection
            var sog = curr.sog();
            if (sog != null && sog < PORT_CALL_MAX_SOG) {
                if (portCallStart == -1) {
                    portCallStart = currIdx;
                    portCallCount = 1;
                } else {
                    portCallCount++;
                }

                if (portCallCount >= PORT_CALL_MIN_POINTS) {
                    var firstSlow = seg.get(portCallStart);
                    var durationSec = java.time.Duration.between(
                            firstSlow.timestamp(), curr.timestamp()).getSeconds();

                    if (durationSec >= PORT_CALL_MIN_DURATION_SEC) {
                        // Emit pre-port-call segment [0..portCallStart-1]
                        var prePart = new ArrayList<>(seg.subList(0, portCallStart));
                        // New segment starts at portCallStart
                        var newSeg = new ArrayList<>(seg.subList(portCallStart, seg.size()));

                        emitSegment(prePart, tracks, skippedOut, wkbWriter);
                        seg = newSeg;
                        // Reset port-call state — new segment is already past the split point
                        portCallStart = -1;
                        portCallCount = 0;
                    }
                }
            } else {
                portCallStart = -1;
                portCallCount = 0;
            }
        }

        // Finalize last segment
        emitSegment(seg, tracks, skippedOut, wkbWriter);
        return tracks;
    }

    // ---- Segment finalisation --------------------------------------------

    private static void emitSegment(List<AisPosition> seg,
                                    List<AisTrack> tracks,
                                    int[] skippedOut,
                                    WKBWriter wkbWriter) {
        if (seg.size() < 2) {
            if (seg.size() == 1) skippedOut[0]++;
            return;
        }

        Coordinate[] coords = new Coordinate[seg.size()];
        for (var i = 0; i < seg.size(); i++) {
            var p = seg.get(i);
            coords[i] = new Coordinate(p.lon(), p.lat());
        }
        var ls = GEOM_FACTORY.createLineString(coords);
        var wkb = wkbWriter.write(ls);

        var startTime  = seg.getFirst().timestamp();
        var endTime    = seg.getLast().timestamp();
        var mmsi       = seg.getFirst().mmsi();
        var voyageId   = mmsi + "_" + startTime.getEpochSecond();

        var avgSog     = computeAvgSog(seg);
        var distanceNm = computeDistanceNm(seg);

        tracks.add(new AisTrack(mmsi, voyageId, wkb, startTime, endTime,
                seg.size(), avgSog, distanceNm));
    }

    // ---- Helpers ---------------------------------------------------------

    private static boolean isStationaryStatus(AisPosition p) {
        var ns = p.navStatus();
        return ns != null && (ns == NAV_STATUS_ANCHORED || ns == NAV_STATUS_MOORED);
    }

    private static Float computeAvgSog(List<AisPosition> seg) {
        var sum = 0;
        var count = 0;
        for (var p : seg) {
            if (p.sog() != null) {
                sum += p.sog();
                count++;
            }
        }
        return count > 0 ? (float) (sum / count) : null;
    }

    private static Float computeDistanceNm(List<AisPosition> seg) {
        if (seg.size() < 2) return null;
        var totalNm = 0.0;
        for (int i = 1; i < seg.size(); i++) {
            var a = seg.get(i - 1);
            var b = seg.get(i);
            totalNm += haversineNm(a.lat(), a.lon(), b.lat(), b.lon());
        }
        return (float) totalNm;
    }

    /**
     * Returns the great-circle distance between two WGS84 points in nautical miles.
     */
    static double haversineNm(double lat1, double lon1, double lat2, double lon2) {
        final double R = 6_371_000.0; // Earth radius in metres
        var dLat = Math.toRadians(lat2 - lat1);
        var dLon = Math.toRadians(lon2 - lon1);
        var a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                  * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c / 1_852.0; // metres → nautical miles
    }
}
