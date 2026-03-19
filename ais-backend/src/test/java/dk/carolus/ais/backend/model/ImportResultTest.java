package dk.carolus.ais.backend.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ImportResultTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void constructorAndGetters() {
        ImportResult r = new ImportResult(100, 20, 5, 1234);
        assertEquals(100, r.getPositionsWritten());
        assertEquals(20, r.getVesselRecords());
        assertEquals(5, r.getTracksBuilt());
        assertEquals(1234, r.getDurationMs());
    }

    @Test
    void jsonSerializationContainsAllFields() throws Exception {
        ImportResult r = new ImportResult(42, 7, 3, 999);
        String json = mapper.writeValueAsString(r);
        assertTrue(json.contains("\"positionsWritten\":42"));
        assertTrue(json.contains("\"vesselRecords\":7"));
        assertTrue(json.contains("\"tracksBuilt\":3"));
        assertTrue(json.contains("\"durationMs\":999"));
    }

    @Test
    void jsonRoundTrip() throws Exception {
        ImportResult original = new ImportResult(10, 2, 1, 500);
        String json = mapper.writeValueAsString(original);
        ImportResult deserialized = mapper.readValue(json, ImportResult.class);
        assertEquals(original.getPositionsWritten(), deserialized.getPositionsWritten());
        assertEquals(original.getVesselRecords(), deserialized.getVesselRecords());
        assertEquals(original.getTracksBuilt(), deserialized.getTracksBuilt());
        assertEquals(original.getDurationMs(), deserialized.getDurationMs());
    }

    @Test
    void zeroCountsAreValid() {
        ImportResult r = new ImportResult(0, 0, 0, 0);
        assertEquals(0, r.getPositionsWritten());
        assertEquals(0, r.getVesselRecords());
        assertEquals(0, r.getTracksBuilt());
        assertEquals(0, r.getDurationMs());
    }
}
