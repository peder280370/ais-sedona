package dk.carolus.ais.backend.resource;

import dk.carolus.ais.backend.model.PositionRecord;
import dk.carolus.ais.backend.service.SedonaQueryService;
import dk.carolus.ais.backend.service.VesselCacheService;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

@QuarkusTest
class PositionsResourceTest {

    @InjectMock
    SedonaQueryService queryService;

    @InjectMock
    VesselCacheService vesselCache;

    @BeforeEach
    void setUp() {
        doAnswer(inv -> inv.getArgument(0)).when(vesselCache).enrich(any());
    }

    private static final PositionRecord SAMPLE = new PositionRecord(
        123456789L, "2024-01-15T08:00:00", "POINT (2.3 51.5)",
        12.5f, 180.0f, 180, 0, 0.0f, 1,
        null, null, null, null);

    @Test
    void getPositions_noParams_returns200WithList() {
        doReturn(List.of(SAMPLE)).when(queryService)
            .queryPositions(isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), eq(1000));

        given()
            .when().get("/api/positions")
            .then()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .body("$.size()", is(1))
            .body("[0].mmsi", is(123456789));
    }

    @Test
    void getPositions_emptyResult_returns200WithEmptyArray() {
        doReturn(List.of()).when(queryService)
            .queryPositions(any(), any(), any(), any(), any(), any(), any(), anyInt());

        given()
            .when().get("/api/positions")
            .then()
            .statusCode(200)
            .body("$.size()", is(0));
    }

    @Test
    void getPositions_withBbox_passesParamsToService() {
        doReturn(List.of()).when(queryService)
            .queryPositions(any(), any(), any(), any(), any(), any(), any(), anyInt());

        given()
            .queryParam("minLon", 8.0)
            .queryParam("minLat", 54.5)
            .queryParam("maxLon", 15.5)
            .queryParam("maxLat", 57.8)
            .when().get("/api/positions")
            .then()
            .statusCode(200);

        verify(queryService).queryPositions(
            eq(8.0), eq(54.5), eq(15.5), eq(57.8),
            isNull(), isNull(), isNull(), eq(1000));
    }

    @Test
    void getPositions_withMmsi_passesParamToService() {
        doReturn(List.of()).when(queryService)
            .queryPositions(any(), any(), any(), any(), any(), any(), any(), anyInt());

        given()
            .queryParam("mmsi", 219001234L)
            .when().get("/api/positions")
            .then()
            .statusCode(200);

        verify(queryService).queryPositions(
            isNull(), isNull(), isNull(), isNull(),
            eq(219001234L), isNull(), isNull(), eq(1000));
    }

    @Test
    void getPositions_withDateRange_passesParamsToService() {
        doReturn(List.of()).when(queryService)
            .queryPositions(any(), any(), any(), any(), any(), any(), any(), anyInt());

        given()
            .queryParam("startDate", "2024-01-01")
            .queryParam("endDate", "2024-01-31")
            .when().get("/api/positions")
            .then()
            .statusCode(200);

        verify(queryService).queryPositions(
            isNull(), isNull(), isNull(), isNull(),
            isNull(), eq("2024-01-01"), eq("2024-01-31"), eq(1000));
    }

    @Test
    void getPositions_customLimit_passesLimitToService() {
        doReturn(List.of()).when(queryService)
            .queryPositions(any(), any(), any(), any(), any(), any(), any(), anyInt());

        given()
            .queryParam("limit", 50)
            .when().get("/api/positions")
            .then()
            .statusCode(200);

        verify(queryService).queryPositions(
            isNull(), isNull(), isNull(), isNull(),
            isNull(), isNull(), isNull(), eq(50));
    }

    @Test
    void getPositions_multipleResults_returnsAll() {
        PositionRecord second = new PositionRecord(
            987654321L, "2024-01-15T09:00:00", "POINT (10.0 55.0)",
            5.0f, 90.0f, null, 1, null, 18,
            null, null, null, null);

        doReturn(List.of(SAMPLE, second)).when(queryService)
            .queryPositions(any(), any(), any(), any(), any(), any(), any(), anyInt());

        given()
            .when().get("/api/positions")
            .then()
            .statusCode(200)
            .body("$.size()", is(2));
    }

    @Test
    void getPositions_responseContainsGeomWkt() {
        doReturn(List.of(SAMPLE)).when(queryService)
            .queryPositions(any(), any(), any(), any(), any(), any(), any(), anyInt());

        given()
            .when().get("/api/positions")
            .then()
            .statusCode(200)
            .body("[0].geomWkt", is("POINT (2.3 51.5)"));
    }
}
