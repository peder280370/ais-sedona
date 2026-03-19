package dk.carolus.ais.backend.model;

import java.util.List;
import java.util.Map;

public record GeoJsonFeatureCollection(String type, List<Map<String, Object>> features) {

    public GeoJsonFeatureCollection(List<Map<String, Object>> features) {
        this("FeatureCollection", features);
    }
}
