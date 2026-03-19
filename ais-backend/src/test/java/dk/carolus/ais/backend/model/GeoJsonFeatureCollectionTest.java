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
        GeoJsonFeatureCollection fc = new GeoJsonFeatureCollection(List.of());
        assertEquals("FeatureCollection", fc.getType());
    }

    @Test
    void featuresListIsPreserved() {
        Map<String, Object> feature = Map.of(
            "type", "Feature",
            "geometry", "{\"type\":\"LineString\",\"coordinates\":[]}",
            "properties", Map.of("mmsi", 123456789L));
        GeoJsonFeatureCollection fc = new GeoJsonFeatureCollection(List.of(feature));
        assertEquals(1, fc.getFeatures().size());
        assertEquals("Feature", fc.getFeatures().get(0).get("type"));
    }

    @Test
    void emptyFeaturesListIsValid() {
        GeoJsonFeatureCollection fc = new GeoJsonFeatureCollection(List.of());
        assertNotNull(fc.getFeatures());
        assertTrue(fc.getFeatures().isEmpty());
    }

    @Test
    void jsonSerializationHasCorrectShape() throws Exception {
        GeoJsonFeatureCollection fc = new GeoJsonFeatureCollection(List.of());
        String json = mapper.writeValueAsString(fc);
        assertTrue(json.contains("\"type\":\"FeatureCollection\""));
        assertTrue(json.contains("\"features\":[]"));
    }

    @Test
    void jsonSerializationWithFeature() throws Exception {
        GeoJsonFeatureCollection fc = new GeoJsonFeatureCollection(List.of(
            Map.of("type", "Feature", "mmsi", 123L)));
        String json = mapper.writeValueAsString(fc);
        assertTrue(json.contains("\"type\":\"FeatureCollection\""));
        assertTrue(json.contains("\"type\":\"Feature\""));
    }
}
