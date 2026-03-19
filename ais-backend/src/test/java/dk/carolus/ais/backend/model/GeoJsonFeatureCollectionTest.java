package dk.carolus.ais.backend.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class GeoJsonFeatureCollectionTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void typeIsAlwaysFeatureCollection() {
        var fc = new GeoJsonFeatureCollection(List.of());
        assertEquals("FeatureCollection", fc.type());
    }

    @Test
    void featuresListIsPreserved() {
        var feature = Map.of(
            "type", "Feature",
            "geometry", "{\"type\":\"LineString\",\"coordinates\":[]}",
            "properties", Map.of("mmsi", 123456789L));
        var fc = new GeoJsonFeatureCollection(List.of(feature));
        assertEquals(1, fc.features().size());
        assertEquals("Feature", fc.features().getFirst().get("type"));
    }

    @Test
    void emptyFeaturesListIsValid() {
        var fc = new GeoJsonFeatureCollection(List.of());
        assertNotNull(fc.features());
        assertTrue(fc.features().isEmpty());
    }

    @Test
    void jsonSerializationHasCorrectShape() throws Exception {
        var fc = new GeoJsonFeatureCollection(List.of());
        var json = mapper.writeValueAsString(fc);
        assertTrue(json.contains("\"type\":\"FeatureCollection\""));
        assertTrue(json.contains("\"features\":[]"));
    }

    @Test
    void jsonSerializationWithFeature() throws Exception {
        var fc = new GeoJsonFeatureCollection(List.of(
            Map.of("type", "Feature", "mmsi", 123L)));
        var json = mapper.writeValueAsString(fc);
        assertTrue(json.contains("\"type\":\"FeatureCollection\""));
        assertTrue(json.contains("\"type\":\"Feature\""));
    }
}
