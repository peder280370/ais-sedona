package dk.carolus.ais.io.pipeline;

import dk.carolus.ais.io.model.AisPosition;
import dk.carolus.ais.io.model.AisTrack;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.io.WKBReader;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TrackBuilderTest {

    // ---- Factory helper ---------------------------------------------------

    private static AisPosition pos(long mmsi, String isoTs, double lat, double lon,
                                   Float sog, Integer navStatus) {
        return new AisPosition(mmsi, Instant.parse(isoTs), lat, lon,
                sog, null, null, navStatus, null, null);
    }

    private final TrackBuilder builder = new TrackBuilder();

    // ---- Basic ------------------------------------------------------------

    @Test
    void singleTrackThreePoints() {
        List<AisPosition> input = List.of(
                pos(123, "2024-01-01T00:00:00Z", 55.0, 10.0, 5f, 0),
                pos(123, "2024-01-01T00:01:00Z", 55.1, 10.1, 5f, 0),
                pos(123, "2024-01-01T00:02:00Z", 55.2, 10.2, 5f, 0)
        );
        var result = builder.build(input);
        assertEquals(1, result.tracks().size());
        assertEquals(3, result.tracks().getFirst().pointCount());
    }

    @Test
    void minTwoPointsProducesTrack() {
        List<AisPosition> input = List.of(
                pos(123, "2024-01-01T00:00:00Z", 55.0, 10.0, 5f, 0),
                pos(123, "2024-01-01T00:01:00Z", 55.1, 10.1, 5f, 0)
        );
        var result = builder.build(input);
        assertEquals(1, result.tracks().size());
        assertEquals(2, result.tracks().getFirst().pointCount());
    }

    @Test
    void singlePointSkipped() {
        List<AisPosition> input = List.of(
                pos(123, "2024-01-01T00:00:00Z", 55.0, 10.0, 5f, 0)
        );
        var result = builder.build(input);
        assertEquals(0, result.tracks().size());
        assertEquals(1, result.skippedSinglePoint());
    }

    // ---- Time gap ---------------------------------------------------------

    @Test
    void timeGapOver6hSplits() {
        List<AisPosition> input = List.of(
                pos(123, "2024-01-01T00:00:00Z", 55.0, 10.0, 5f, 0),
                pos(123, "2024-01-01T00:01:00Z", 55.1, 10.1, 5f, 0),
                pos(123, "2024-01-01T06:01:01Z", 56.0, 11.0, 5f, 0),  // >6h gap
                pos(123, "2024-01-01T06:02:00Z", 56.1, 11.1, 5f, 0)
        );
        var result = builder.build(input);
        assertEquals(2, result.tracks().size());
    }

    @Test
    void exactlyGapThresholdNoSplit() {
        // Exactly 6h = 21600s — must NOT split (condition is strictly >)
        List<AisPosition> input = List.of(
                pos(123, "2024-01-01T00:00:00Z", 55.0, 10.0, 5f, 0),
                pos(123, "2024-01-01T00:01:00Z", 55.1, 10.1, 5f, 0),
                pos(123, "2024-01-01T06:01:00Z", 56.0, 11.0, 5f, 0),  // exactly 6h after 2nd
                pos(123, "2024-01-01T06:02:00Z", 56.1, 11.1, 5f, 0)
        );
        var result = builder.build(input);
        assertEquals(1, result.tracks().size());
        assertEquals(4, result.tracks().getFirst().pointCount());
    }

    // ---- Nav status -------------------------------------------------------

    @Test
    void navStatusSplitNonStationaryToMoored() {
        List<AisPosition> input = List.of(
                pos(123, "2024-01-01T00:00:00Z", 55.0, 10.0, 5f, 0),
                pos(123, "2024-01-01T00:01:00Z", 55.1, 10.1, 5f, 0),
                pos(123, "2024-01-01T00:02:00Z", 55.2, 10.2, 0.1f, 5),  // now moored
                pos(123, "2024-01-01T00:03:00Z", 55.2, 10.2, 0.0f, 5)
        );
        var result = builder.build(input);
        assertEquals(2, result.tracks().size());
    }

    @Test
    void navStatusSplitAnchoredToUnderway() {
        List<AisPosition> input = List.of(
                pos(123, "2024-01-01T00:00:00Z", 55.0, 10.0, 0.1f, 1),  // anchored
                pos(123, "2024-01-01T00:01:00Z", 55.0, 10.0, 0.0f, 1),
                pos(123, "2024-01-01T00:02:00Z", 55.1, 10.1, 5f,   0),  // underway
                pos(123, "2024-01-01T00:03:00Z", 55.2, 10.2, 5f,   0)
        );
        var result = builder.build(input);
        assertEquals(2, result.tracks().size());
    }

    @Test
    void navStatusStaysMoored_noSplit() {
        List<AisPosition> input = List.of(
                pos(123, "2024-01-01T00:00:00Z", 55.0, 10.0, 0.0f, 5),
                pos(123, "2024-01-01T00:01:00Z", 55.0, 10.0, 0.0f, 5),
                pos(123, "2024-01-01T00:02:00Z", 55.0, 10.0, 0.0f, 5)
        );
        var result = builder.build(input);
        assertEquals(1, result.tracks().size());
    }

    @Test
    void nullNavStatusIsNotStationary() {
        // null nav-status should be treated as non-stationary; no split between nulls
        List<AisPosition> input = List.of(
                pos(123, "2024-01-01T00:00:00Z", 55.0, 10.0, 5f, null),
                pos(123, "2024-01-01T00:01:00Z", 55.1, 10.1, 5f, null),
                pos(123, "2024-01-01T00:02:00Z", 55.2, 10.2, 5f, null)
        );
        var result = builder.build(input);
        assertEquals(1, result.tracks().size());
    }

    // ---- Port call --------------------------------------------------------

    @Test
    void portCallSplitFires() {
        // 3 fast positions, then 6 slow positions spanning >30 min
        List<AisPosition> input = List.of(
                pos(123, "2024-01-01T00:00:00Z", 55.0, 10.0, 8f,   0),
                pos(123, "2024-01-01T00:01:00Z", 55.1, 10.1, 8f,   0),
                pos(123, "2024-01-01T00:02:00Z", 55.2, 10.2, 8f,   0),
                // port call starts here
                pos(123, "2024-01-01T00:03:00Z", 55.3, 10.3, 0.1f, 0),
                pos(123, "2024-01-01T00:09:00Z", 55.3, 10.3, 0.1f, 0),
                pos(123, "2024-01-01T00:15:00Z", 55.3, 10.3, 0.1f, 0),
                pos(123, "2024-01-01T00:21:00Z", 55.3, 10.3, 0.1f, 0),
                pos(123, "2024-01-01T00:27:00Z", 55.3, 10.3, 0.1f, 0),
                pos(123, "2024-01-01T00:33:01Z", 55.3, 10.3, 0.1f, 0)  // 6th slow, >30 min span
        );
        var result = builder.build(input);
        // Should have 2 tracks: [0..2] pre-port and [3..8] port-call segment
        assertEquals(2, result.tracks().size());
        assertEquals(3, result.tracks().getFirst().pointCount());
        assertEquals(6, result.tracks().getLast().pointCount());
    }

    @Test
    void portCallNoSplitUnder30Min() {
        // 6 slow points but span is only 29 min — no port-call split
        List<AisPosition> input = List.of(
                pos(123, "2024-01-01T00:00:00Z", 55.0, 10.0, 8f,   0),
                pos(123, "2024-01-01T00:01:00Z", 55.1, 10.1, 8f,   0),
                pos(123, "2024-01-01T00:02:00Z", 55.2, 10.2, 8f,   0),
                pos(123, "2024-01-01T00:03:00Z", 55.3, 10.3, 0.1f, 0),
                pos(123, "2024-01-01T00:09:00Z", 55.3, 10.3, 0.1f, 0),
                pos(123, "2024-01-01T00:15:00Z", 55.3, 10.3, 0.1f, 0),
                pos(123, "2024-01-01T00:21:00Z", 55.3, 10.3, 0.1f, 0),
                pos(123, "2024-01-01T00:27:00Z", 55.3, 10.3, 0.1f, 0),
                pos(123, "2024-01-01T00:32:59Z", 55.3, 10.3, 0.1f, 0)  // 29 min 59 s
        );
        var result = builder.build(input);
        assertEquals(1, result.tracks().size());
        assertEquals(9, result.tracks().getFirst().pointCount());
    }

    @Test
    void portCallNoSplitFewerThan6Points() {
        // 5 slow points spanning >30 min — still not enough
        List<AisPosition> input = List.of(
                pos(123, "2024-01-01T00:00:00Z", 55.0, 10.0, 8f,   0),
                pos(123, "2024-01-01T00:01:00Z", 55.1, 10.1, 8f,   0),
                pos(123, "2024-01-01T00:02:00Z", 55.2, 10.2, 8f,   0),
                pos(123, "2024-01-01T00:03:00Z", 55.3, 10.3, 0.1f, 0),
                pos(123, "2024-01-01T00:11:00Z", 55.3, 10.3, 0.1f, 0),
                pos(123, "2024-01-01T00:19:00Z", 55.3, 10.3, 0.1f, 0),
                pos(123, "2024-01-01T00:27:00Z", 55.3, 10.3, 0.1f, 0),
                pos(123, "2024-01-01T00:35:00Z", 55.3, 10.3, 0.1f, 0)  // only 5 slow
        );
        var result = builder.build(input);
        assertEquals(1, result.tracks().size());
        assertEquals(8, result.tracks().getFirst().pointCount());
    }

    // ---- Downsampling -----------------------------------------------------

    @Test
    void within10sDiscarded() {
        // 3 positions: 0s, 5s, 20s — the 5s one should be dropped
        List<AisPosition> input = List.of(
                pos(123, "2024-01-01T00:00:00Z", 55.0, 10.0, 5f, 0),
                pos(123, "2024-01-01T00:00:05Z", 55.1, 10.1, 5f, 0),  // 5s later — dropped
                pos(123, "2024-01-01T00:00:20Z", 55.2, 10.2, 5f, 0)   // 20s after first — kept
        );
        var result = builder.build(input);
        assertEquals(1, result.tracks().size());
        assertEquals(2, result.tracks().getFirst().pointCount());
        assertEquals(1, result.downsampledPositions());
    }

    // ---- Haversine distance -----------------------------------------------

    @Test
    void haversineDistanceAccuracy() {
        // 1 degree of latitude at the equator = exactly 1 arcminute × 60 = 60.04 nm
        double nm = TrackBuilder.haversineNm(0.0, 0.0, 1.0, 0.0);
        assertEquals(60.04, nm, 0.05);
    }

    // ---- avgSog -----------------------------------------------------------

    @Test
    void avgSogCalculation() {
        List<AisPosition> input = List.of(
                pos(123, "2024-01-01T00:00:00Z", 55.0, 10.0, 10f, 0),
                pos(123, "2024-01-01T00:01:00Z", 55.1, 10.1, 20f, 0),
                pos(123, "2024-01-01T00:02:00Z", 55.2, 10.2, 30f, 0)
        );
        var result = builder.build(input);
        assertEquals(1, result.tracks().size());
        assertEquals(20f, result.tracks().getFirst().avgSog(), 0.01f);
    }

    @Test
    void allNullSogReturnsNullAvgSog() {
        List<AisPosition> input = List.of(
                pos(123, "2024-01-01T00:00:00Z", 55.0, 10.0, null, 0),
                pos(123, "2024-01-01T00:01:00Z", 55.1, 10.1, null, 0)
        );
        var result = builder.build(input);
        assertEquals(1, result.tracks().size());
        assertNull(result.tracks().getFirst().avgSog());
    }

    // ---- voyageId / multiple MMSIs / out-of-order -------------------------

    @Test
    void voyageIdFormat() {
        Instant t0 = Instant.parse("2024-01-01T00:00:00Z");
        List<AisPosition> input = List.of(
                pos(123456789L, "2024-01-01T00:00:00Z", 55.0, 10.0, 5f, 0),
                pos(123456789L, "2024-01-01T00:01:00Z", 55.1, 10.1, 5f, 0)
        );
        var result = builder.build(input);
        assertEquals(1, result.tracks().size());
        String expectedId = "123456789_" + t0.getEpochSecond();
        assertEquals(expectedId, result.tracks().getFirst().voyageId());
    }

    @Test
    void multipleMMSIsProduceSeparateTracks() {
        List<AisPosition> input = List.of(
                pos(111, "2024-01-01T00:00:00Z", 55.0, 10.0, 5f, 0),
                pos(222, "2024-01-01T00:00:00Z", 56.0, 11.0, 5f, 0),
                pos(111, "2024-01-01T00:01:00Z", 55.1, 10.1, 5f, 0),
                pos(222, "2024-01-01T00:01:00Z", 56.1, 11.1, 5f, 0)
        );
        var result = builder.build(input);
        assertEquals(2, result.tracks().size());
        long mmsi0 = result.tracks().getFirst().mmsi();
        long mmsi1 = result.tracks().getLast().mmsi();
        assertNotEquals(mmsi0, mmsi1);
    }

    @Test
    void outOfOrderInputSortedCorrectly() {
        // Positions arrive reversed; result should still be a single valid track
        List<AisPosition> input = List.of(
                pos(123, "2024-01-01T00:02:00Z", 55.2, 10.2, 5f, 0),
                pos(123, "2024-01-01T00:00:00Z", 55.0, 10.0, 5f, 0),
                pos(123, "2024-01-01T00:01:00Z", 55.1, 10.1, 5f, 0)
        );
        var result = builder.build(input);
        assertEquals(1, result.tracks().size());
        // Verify start/end time order
        var t = result.tracks().getFirst();
        assertTrue(t.startTime().isBefore(t.endTime()));
        assertEquals(Instant.parse("2024-01-01T00:00:00Z"), t.startTime());
        assertEquals(Instant.parse("2024-01-01T00:02:00Z"), t.endTime());
    }

    // ---- WKB validity -----------------------------------------------------

    @Test
    void wkbRoundTrip() throws Exception {
        List<AisPosition> input = List.of(
                pos(123, "2024-01-01T00:00:00Z", 55.0, 10.0, 5f, 0),
                pos(123, "2024-01-01T00:01:00Z", 55.1, 10.1, 5f, 0),
                pos(123, "2024-01-01T00:02:00Z", 55.2, 10.2, 5f, 0)
        );
        var result = builder.build(input);
        assertEquals(1, result.tracks().size());

        byte[] wkb = result.tracks().getFirst().geometryWkb();
        assertNotNull(wkb);
        assertTrue(wkb.length > 0);

        var reader = new WKBReader();
        var ls = (LineString) reader.read(wkb);
        assertEquals("LineString", ls.getGeometryType());
        assertEquals(3, ls.getNumPoints());
        assertEquals(10.0, ls.getCoordinateN(0).x, 1e-9);
        assertEquals(55.0, ls.getCoordinateN(0).y, 1e-9);
    }
}
