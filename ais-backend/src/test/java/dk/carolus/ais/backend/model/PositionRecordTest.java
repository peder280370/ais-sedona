package dk.carolus.ais.backend.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PositionRecordTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void constructorAndGetters() {
        var r = new PositionRecord(
            123456789L, "2024-01-15T08:00:00", "POINT (2.3 51.5)",
            12.5f, 180.0f, 180, 0, 0.0f, 1,
            null, null, null, null);
        assertEquals(123456789L, r.mmsi());
        assertEquals("2024-01-15T08:00:00", r.ts());
        assertEquals("POINT (2.3 51.5)", r.geomWkt());
        assertEquals(12.5f, r.sog());
        assertEquals(180.0f, r.cog());
        assertEquals(180, r.heading());
        assertEquals(0, r.navStatus());
        assertEquals(0.0f, r.rot());
        assertEquals(1, r.msgType());
    }

    @Test
    void nullableFieldsAcceptNull() {
        var r = new PositionRecord(
            987654321L, null, "POINT (10.0 55.0)",
            null, null, null, null, null, null,
            null, null, null, null);
        assertNull(r.ts());
        assertNull(r.sog());
        assertNull(r.cog());
        assertNull(r.heading());
        assertNull(r.navStatus());
        assertNull(r.rot());
        assertNull(r.msgType());
    }

    @Test
    void jsonRoundTrip() throws Exception {
        var original = new PositionRecord(
            123456789L, "2024-01-15T08:00:00Z", "POINT (2.3 51.5)",
            8.5f, 270.0f, 270, 0, null, 1,
            null, null, null, null);
        var json = mapper.writeValueAsString(original);
        var deserialized = mapper.readValue(json, PositionRecord.class);
        assertEquals(original.mmsi(), deserialized.mmsi());
        assertEquals(original.ts(), deserialized.ts());
        assertEquals(original.geomWkt(), deserialized.geomWkt());
        assertEquals(original.sog(), deserialized.sog());
        assertNull(deserialized.rot());
    }

    @Test
    void jsonSerializationIncludesNullFields() throws Exception {
        var r = new PositionRecord(
            111222333L, null, "POINT (5.0 60.0)",
            null, null, null, null, null, null,
            null, null, null, null);
        var json = mapper.writeValueAsString(r);
        assertTrue(json.contains("111222333"));
        assertTrue(json.contains("\"sog\":null") || json.contains("\"sog\":null"));
    }
}
