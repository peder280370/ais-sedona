package dk.carolus.ais.backend.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class VesselRecordTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void constructorAndGetters() {
        var v = new VesselRecord(
            219001234L, 9123456L, "MAERSK LINE", "OXDT",
            70, "Cargo", 294.0f, 32.0f, 14.0f, "SINGAPORE", "2024-01-15T08:00:00");
        assertEquals(219001234L, v.mmsi());
        assertEquals(9123456L, v.imo());
        assertEquals("MAERSK LINE", v.vesselName());
        assertEquals("OXDT", v.callsign());
        assertEquals(70, v.shipType());
        assertEquals("Cargo", v.shipTypeDesc());
        assertEquals(294.0f, v.lengthM());
        assertEquals(32.0f, v.beamM());
        assertEquals(14.0f, v.draughtM());
        assertEquals("SINGAPORE", v.destination());
        assertEquals("2024-01-15T08:00:00", v.lastSeen());
    }

    @Test
    void allOptionalFieldsAcceptNull() {
        var v = new VesselRecord(
            219001234L, null, null, null,
            null, null, null, null, null, null, null);
        assertEquals(219001234L, v.mmsi());
        assertNull(v.imo());
        assertNull(v.vesselName());
        assertNull(v.lengthM());
        assertNull(v.lastSeen());
    }

    @Test
    void jsonRoundTrip() throws Exception {
        var original = new VesselRecord(
            219001234L, 9123456L, "TEST VESSEL", "ABCD",
            80, "Tanker", 180.0f, 28.0f, 10.5f, "ROTTERDAM", "2024-01-15T10:00:00");
        var json = mapper.writeValueAsString(original);
        var deserialized = mapper.readValue(json, VesselRecord.class);
        assertEquals(original.mmsi(), deserialized.mmsi());
        assertEquals(original.vesselName(), deserialized.vesselName());
        assertEquals(original.imo(), deserialized.imo());
        assertEquals(original.lengthM(), deserialized.lengthM());
        assertEquals(original.destination(), deserialized.destination());
    }
}
