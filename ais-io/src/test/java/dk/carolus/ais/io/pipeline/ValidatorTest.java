package dk.carolus.ais.io.pipeline;

import dk.carolus.ais.io.model.AisPosition;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ValidatorTest {

    private static AisPosition pos(long mmsi, double lat, double lon, Float sog) {
        return new AisPosition(mmsi, Instant.parse("2024-01-15T10:00:00Z"),
                lat, lon, sog, null, null, null, null, 1);
    }

    private static AisPosition pos(long mmsi, Instant ts, double lat, double lon) {
        return new AisPosition(mmsi, ts, lat, lon, 5.0f, null, null, null, null, 1);
    }

    // ---- MMSI -----------------------------------------------------------

    @Test
    void acceptsValidMmsi() {
        assertTrue(Validator.isValidMmsi(123456789L));
        assertTrue(Validator.isValidMmsi(100_000_000L));
        assertTrue(Validator.isValidMmsi(999_999_999L));
    }

    @Test
    void rejectsOutOfRangeMmsi() {
        assertFalse(Validator.isValidMmsi(99_999_999L));
        assertFalse(Validator.isValidMmsi(1_000_000_000L));
        assertFalse(Validator.isValidMmsi(0L));
    }

    // ---- Coordinates ----------------------------------------------------

    @Test
    void acceptsValidCoords() {
        assertTrue(Validator.isValidCoords(51.5, 2.3));
        assertTrue(Validator.isValidCoords(-90.0, -180.0));
        assertTrue(Validator.isValidCoords(90.0, 180.0));
    }

    @Test
    void rejectsInvalidCoords() {
        assertFalse(Validator.isValidCoords(91.0, 0.0));
        assertFalse(Validator.isValidCoords(0.0, 181.0));
        assertFalse(Validator.isValidCoords(-91.0, 0.0));
    }

    @Test
    void detectsNullIsland() {
        assertTrue(Validator.isNullIsland(0.0, 0.0));
        assertFalse(Validator.isNullIsland(0.0, 1.0));
        assertFalse(Validator.isNullIsland(1.0, 0.0));
    }

    // ---- SOG ------------------------------------------------------------

    @Test
    void rejectsHighSOG() {
        assertTrue(Validator.isSOGExceeded(102.3f));
        assertFalse(Validator.isSOGExceeded(102.2f));
        assertFalse(Validator.isSOGExceeded(null));
    }

    // ---- End-to-end validation ------------------------------------------

    @Test
    void filtersInvalidMmsi() {
        List<AisPosition> input = List.of(
                pos(99_999_999L, 51.5, 2.3, 5.0f),  // invalid MMSI
                pos(123456789L, 51.5, 2.3, 5.0f)     // valid
        );
        Validator v = new Validator();
        List<AisPosition> result = v.validate(input);
        assertEquals(1, result.size());
        assertEquals(123456789L, result.get(0).getMmsi());
        assertEquals(1, v.getStats().invalidMmsi);
    }

    @Test
    void filtersNullIsland() {
        List<AisPosition> input = List.of(
                pos(123456789L, 0.0, 0.0, 5.0f),  // null island
                pos(234567890L, 55.0, 10.0, 5.0f) // valid
        );
        Validator v = new Validator();
        List<AisPosition> result = v.validate(input);
        assertEquals(1, result.size());
        assertEquals(1, v.getStats().nullIsland);
    }

    @Test
    void filtersHighSOG() {
        List<AisPosition> input = List.of(
                pos(123456789L, 51.5, 2.3, 150.0f), // SOG too high
                pos(234567890L, 55.0, 10.0, 12.0f)  // valid
        );
        Validator v = new Validator();
        List<AisPosition> result = v.validate(input);
        assertEquals(1, result.size());
        assertEquals(1, v.getStats().sogExceeded);
    }

    @Test
    void deduplicatesOnMmsiAndTimestamp() {
        Instant ts = Instant.parse("2024-01-15T10:00:00Z");
        List<AisPosition> input = List.of(
                pos(123456789L, ts, 51.5, 2.3),
                pos(123456789L, ts, 51.5, 2.4),  // duplicate (same mmsi+ts)
                pos(123456789L, Instant.parse("2024-01-15T10:05:00Z"), 51.4, 2.3) // different ts
        );
        Validator v = new Validator();
        List<AisPosition> result = v.validate(input);
        assertEquals(2, result.size());
        assertEquals(1, v.getStats().duplicates);
    }

    @Test
    void acceptsHeading511() {
        // heading=511 means "not available" — should be kept, not filtered
        AisPosition p = new AisPosition(123456789L, Instant.parse("2024-01-15T10:00:00Z"),
                51.5, 2.3, 5.0f, 90.0f, 511, 0, 0.0f, 1);
        Validator v = new Validator();
        List<AisPosition> result = v.validate(List.of(p));
        assertEquals(1, result.size(), "heading=511 should be kept");
    }
}
