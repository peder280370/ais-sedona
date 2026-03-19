package dk.carolus.ais.backend.resource;

import dk.carolus.ais.backend.model.VesselRecord;
import dk.carolus.ais.backend.service.SedonaQueryService;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

@QuarkusTest
class VesselsResourceTest {

    @InjectMock
    SedonaQueryService queryService;

    private static final VesselRecord SAMPLE = new VesselRecord(
        219001234L, 9123456L, "MAERSK ELBA", "OXDT",
        70, "Cargo", 294.0f, 32.0f, 14.0f, "SINGAPORE", "2024-01-15T08:00:00");

    @Test
    void getVessels_noParams_returns200WithList() {
        doReturn(List.of(SAMPLE)).when(queryService).queryVessels(isNull(), isNull());

        given()
            .when().get("/api/vessels")
            .then()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .body("$.size()", is(1))
            .body("[0].mmsi", is(219001234));
    }

    @Test
    void getVessels_emptyResult_returns200WithEmptyArray() {
        doReturn(List.of()).when(queryService).queryVessels(any(), any());

        given()
            .when().get("/api/vessels")
            .then()
            .statusCode(200)
            .body("$.size()", is(0));
    }

    @Test
    void getVessels_withMmsiQueryParam_passesParamToService() {
        doReturn(List.of(SAMPLE)).when(queryService).queryVessels(any(), any());

        given()
            .queryParam("mmsi", 219001234L)
            .when().get("/api/vessels")
            .then()
            .statusCode(200);

        verify(queryService).queryVessels(eq(219001234L), isNull());
    }

    @Test
    void getVessels_withNameFilter_passesNameToService() {
        doReturn(List.of(SAMPLE)).when(queryService).queryVessels(any(), any());

        given()
            .queryParam("name", "MAERSK")
            .when().get("/api/vessels")
            .then()
            .statusCode(200);

        verify(queryService).queryVessels(isNull(), eq("MAERSK"));
    }

    @Test
    void getVesselByPath_callsServiceWithMmsiAndNullName() {
        doReturn(List.of(SAMPLE)).when(queryService).queryVessels(any(), any());

        given()
            .when().get("/api/vessels/219001234")
            .then()
            .statusCode(200)
            .body("$.size()", is(1));

        verify(queryService).queryVessels(eq(219001234L), isNull());
    }

    @Test
    void getVesselByPath_notFound_returnsEmptyList() {
        doReturn(List.of()).when(queryService).queryVessels(any(), any());

        given()
            .when().get("/api/vessels/999999999")
            .then()
            .statusCode(200)
            .body("$.size()", is(0));
    }

    @Test
    void getVessels_responseContainsVesselFields() {
        doReturn(List.of(SAMPLE)).when(queryService).queryVessels(any(), any());

        given()
            .when().get("/api/vessels")
            .then()
            .statusCode(200)
            .body("[0].vesselName", is("MAERSK ELBA"))
            .body("[0].shipTypeDesc", is("Cargo"))
            .body("[0].destination", is("SINGAPORE"));
    }

    @Test
    void getVessels_nullableFieldsSerializedAsNull() {
        VesselRecord sparse = new VesselRecord(
            123456789L, null, null, null, null, null, null, null, null, null, null);
        doReturn(List.of(sparse)).when(queryService).queryVessels(any(), any());

        given()
            .when().get("/api/vessels")
            .then()
            .statusCode(200)
            .body("[0].mmsi", is(123456789))
            .body("[0].vesselName", nullValue())
            .body("[0].imo", nullValue());
    }
}
