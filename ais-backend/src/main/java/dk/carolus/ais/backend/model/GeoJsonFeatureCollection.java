package dk.carolus.ais.backend.model;

import lombok.Value;
import java.util.List;
import java.util.Map;

@Value
public class GeoJsonFeatureCollection {
    String type = "FeatureCollection";
    List<Map<String, Object>> features;
}
