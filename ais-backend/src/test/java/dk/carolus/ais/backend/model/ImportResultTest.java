package dk.carolus.ais.backend.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ImportResultTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void constructorAndGetters() {
        var r = new ImportResult(100, 20, 5, 1234);
        assertEquals(100, r.positionsWritten());
        assertEquals(20, r.vesselRecords());
        assertEquals(5, r.tracksBuilt());
        assertEquals(1234, r.durationMs());
    }

    @Test
    void jsonSerializationContainsAllFields() throws Exception {
        var r = new ImportResult(42, 7, 3, 999);
        var json = mapper.writeValueAsString(r);
        assertTrue(json.contains("\"positionsWritten\":42"));
        assertTrue(json.contains("\"vesselRecords\":7"));
        assertTrue(json.contains("\"tracksBuilt\":3"));
        assertTrue(json.contains("\"durationMs\":999"));
    }

    @Test
    void jsonRoundTrip() throws Exception {
        var original = new ImportResult(10, 2, 1, 500);
        var json = mapper.writeValueAsString(original);
        var deserialized = mapper.readValue(json, ImportResult.class);
        assertEquals(original.positionsWritten(), deserialized.positionsWritten());
        assertEquals(original.vesselRecords(), deserialized.vesselRecords());
        assertEquals(original.tracksBuilt(), deserialized.tracksBuilt());
        assertEquals(original.durationMs(), deserialized.durationMs());
    }

    @Test
    void zeroCountsAreValid() {
        var r = new ImportResult(0, 0, 0, 0);
        assertEquals(0, r.positionsWritten());
        assertEquals(0, r.vesselRecords());
        assertEquals(0, r.tracksBuilt());
        assertEquals(0, r.durationMs());
    }
}
