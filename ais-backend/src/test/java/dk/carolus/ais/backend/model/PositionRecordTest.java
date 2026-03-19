package dk.carolus.ais.backend.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PositionRecordTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void constructorAndGetters() {
        PositionRecord r = new PositionRecord(
            123456789L, "2024-01-15T08:00:00", "POINT (2.3 51.5)",
            12.5f, 180.0f, 180, 0, 0.0f, 1,
            null, null, null, null);
        assertEquals(123456789L, r.getMmsi());
        assertEquals("2024-01-15T08:00:00", r.getTs());
        assertEquals("POINT (2.3 51.5)", r.getGeomWkt());
        assertEquals(12.5f, r.getSog());
        assertEquals(180.0f, r.getCog());
        assertEquals(180, r.getHeading());
        assertEquals(0, r.getNavStatus());
        assertEquals(0.0f, r.getRot());
        assertEquals(1, r.getMsgType());
    }

    @Test
    void nullableFieldsAcceptNull() {
        PositionRecord r = new PositionRecord(
            987654321L, null, "POINT (10.0 55.0)",
            null, null, null, null, null, null,
            null, null, null, null);
        assertNull(r.getTs());
        assertNull(r.getSog());
        assertNull(r.getCog());
        assertNull(r.getHeading());
        assertNull(r.getNavStatus());
        assertNull(r.getRot());
        assertNull(r.getMsgType());
    }

    @Test
    void jsonRoundTrip() throws Exception {
        PositionRecord original = new PositionRecord(
            123456789L, "2024-01-15T08:00:00Z", "POINT (2.3 51.5)",
            8.5f, 270.0f, 270, 0, null, 1,
            null, null, null, null);
        String json = mapper.writeValueAsString(original);
        PositionRecord deserialized = mapper.readValue(json, PositionRecord.class);
        assertEquals(original.getMmsi(), deserialized.getMmsi());
        assertEquals(original.getTs(), deserialized.getTs());
        assertEquals(original.getGeomWkt(), deserialized.getGeomWkt());
        assertEquals(original.getSog(), deserialized.getSog());
        assertNull(deserialized.getRot());
    }

    @Test
    void jsonSerializationIncludesNullFields() throws Exception {
        PositionRecord r = new PositionRecord(
            111222333L, null, "POINT (5.0 60.0)",
            null, null, null, null, null, null,
            null, null, null, null);
        String json = mapper.writeValueAsString(r);
        assertTrue(json.contains("111222333"));
        assertTrue(json.contains("\"sog\":null") || json.contains("\"sog\":null"));
    }
}
