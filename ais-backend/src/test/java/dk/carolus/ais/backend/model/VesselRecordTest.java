package dk.carolus.ais.backend.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class VesselRecordTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void constructorAndGetters() {
        VesselRecord v = new VesselRecord(
            219001234L, 9123456L, "MAERSK LINE", "OXDT",
            70, "Cargo", 294.0f, 32.0f, 14.0f, "SINGAPORE", "2024-01-15T08:00:00");
        assertEquals(219001234L, v.getMmsi());
        assertEquals(9123456L, v.getImo());
        assertEquals("MAERSK LINE", v.getVesselName());
        assertEquals("OXDT", v.getCallsign());
        assertEquals(70, v.getShipType());
        assertEquals("Cargo", v.getShipTypeDesc());
        assertEquals(294.0f, v.getLengthM());
        assertEquals(32.0f, v.getBeamM());
        assertEquals(14.0f, v.getDraughtM());
        assertEquals("SINGAPORE", v.getDestination());
        assertEquals("2024-01-15T08:00:00", v.getLastSeen());
    }

    @Test
    void allOptionalFieldsAcceptNull() {
        VesselRecord v = new VesselRecord(
            219001234L, null, null, null,
            null, null, null, null, null, null, null);
        assertEquals(219001234L, v.getMmsi());
        assertNull(v.getImo());
        assertNull(v.getVesselName());
        assertNull(v.getLengthM());
        assertNull(v.getLastSeen());
    }

    @Test
    void jsonRoundTrip() throws Exception {
        VesselRecord original = new VesselRecord(
            219001234L, 9123456L, "TEST VESSEL", "ABCD",
            80, "Tanker", 180.0f, 28.0f, 10.5f, "ROTTERDAM", "2024-01-15T10:00:00");
        String json = mapper.writeValueAsString(original);
        VesselRecord deserialized = mapper.readValue(json, VesselRecord.class);
        assertEquals(original.getMmsi(), deserialized.getMmsi());
        assertEquals(original.getVesselName(), deserialized.getVesselName());
        assertEquals(original.getImo(), deserialized.getImo());
        assertEquals(original.getLengthM(), deserialized.getLengthM());
        assertEquals(original.getDestination(), deserialized.getDestination());
    }
}
