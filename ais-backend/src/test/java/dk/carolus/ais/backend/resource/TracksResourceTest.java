package dk.carolus.ais.backend.resource;

import dk.carolus.ais.backend.model.GeoJsonFeatureCollection;
import dk.carolus.ais.backend.service.SedonaQueryService;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

@QuarkusTest
class TracksResourceTest {

    @InjectMock
    SedonaQueryService queryService;

    private static Map<String, Object> sampleFeature() {
        Map<String, Object> props = new HashMap<>();
        props.put("mmsi", 123456789L);
        props.put("voyage_id", "v1");
        props.put("point_count", 10L);
        props.put("avg_sog", 8.5);
        props.put("distance_nm", 12.3);

        Map<String, Object> feature = new HashMap<>();
        Map<String, Object> geometry = new HashMap<>();
        geometry.put("type", "LineString");
        geometry.put("coordinates", List.of(List.of(2.3, 51.5), List.of(2.4, 51.6)));

        feature.put("type", "Feature");
        feature.put("geometry", geometry);
        feature.put("properties", props);
        return feature;
    }

    @Test
    void getTracks_noParams_returns200WithFeatureCollection() {
        doReturn(new GeoJsonFeatureCollection(List.of(sampleFeature())))
            .when(queryService).queryTracks(isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), isNull());

        given()
            .when().get("/api/tracks")
            .then()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .body("type", is("FeatureCollection"))
            .body("features.size()", is(1));
    }

    @Test
    void getTracks_emptyResult_returnsEmptyFeatureCollection() {
        doReturn(new GeoJsonFeatureCollection(List.of()))
            .when(queryService).queryTracks(any(), any(), any(), any(), any(), any(), any());

        given()
            .when().get("/api/tracks")
            .then()
            .statusCode(200)
            .body("type", is("FeatureCollection"))
            .body("features", hasSize(0));
    }

    @Test
    void getTracks_withBbox_passesParamsToService() {
        doReturn(new GeoJsonFeatureCollection(List.of()))
            .when(queryService).queryTracks(any(), any(), any(), any(), any(), any(), any());

        given()
            .queryParam("minLon", 8.0)
            .queryParam("minLat", 54.5)
            .queryParam("maxLon", 15.5)
            .queryParam("maxLat", 57.8)
            .when().get("/api/tracks")
            .then()
            .statusCode(200);

        verify(queryService).queryTracks(
            eq(8.0), eq(54.5), eq(15.5), eq(57.8),
            isNull(), isNull(), isNull());
    }

    @Test
    void getTracks_withMmsi_passesParamToService() {
        doReturn(new GeoJsonFeatureCollection(List.of()))
            .when(queryService).queryTracks(any(), any(), any(), any(), any(), any(), any());

        given()
            .queryParam("mmsi", 219001234L)
            .when().get("/api/tracks")
            .then()
            .statusCode(200);

        verify(queryService).queryTracks(
            isNull(), isNull(), isNull(), isNull(),
            eq(219001234L), isNull(), isNull());
    }

    @Test
    void getTracks_withDateRange_passesParamsToService() {
        doReturn(new GeoJsonFeatureCollection(List.of()))
            .when(queryService).queryTracks(any(), any(), any(), any(), any(), any(), any());

        given()
            .queryParam("startDate", "2024-01-01")
            .queryParam("endDate", "2024-01-31")
            .when().get("/api/tracks")
            .then()
            .statusCode(200);

        verify(queryService).queryTracks(
            isNull(), isNull(), isNull(), isNull(),
            isNull(), eq("2024-01-01"), eq("2024-01-31"));
    }

    @Test
    void getTracks_featureHasExpectedProperties() {
        doReturn(new GeoJsonFeatureCollection(List.of(sampleFeature())))
            .when(queryService).queryTracks(any(), any(), any(), any(), any(), any(), any());

        given()
            .when().get("/api/tracks")
            .then()
            .statusCode(200)
            .body("features[0].type", is("Feature"))
            .body("features[0].properties.mmsi", is(123456789));
    }
}
