package dk.carolus.ais.backend.resource;

import dk.carolus.ais.backend.model.ImportResult;
import dk.carolus.ais.backend.service.AisImportService;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

@QuarkusTest
class ImportResourceTest {

    @InjectMock
    AisImportService importService;

    private static final ImportResult SAMPLE_RESULT = new ImportResult(50, 10, 5, 1234);

    // ---- POST /api/import/dma --------------------------------------------

    @Test
    void importDma_validDate_returns200WithResult() throws Exception {
        doReturn(SAMPLE_RESULT).when(importService).importDma(eq("2024-01-15"));

        given()
            .contentType(ContentType.JSON)
            .body("{\"date\":\"2024-01-15\"}")
            .when().post("/api/import/dma")
            .then()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .body("positionsWritten", is(50))
            .body("vesselRecords", is(10))
            .body("tracksBuilt", is(5))
            .body("durationMs", is(1234));
    }

    @Test
    void importDma_delegatesToService() throws Exception {
        doReturn(new ImportResult(0, 0, 0, 0)).when(importService).importDma(any());

        given()
            .contentType(ContentType.JSON)
            .body("{\"date\":\"2024-03-17\"}")
            .when().post("/api/import/dma")
            .then()
            .statusCode(200);

        verify(importService).importDma("2024-03-17");
    }

    // ---- POST /api/import/file -------------------------------------------

    @Test
    void importFile_nmeaMultipart_returns200WithResult() throws Exception {
        doReturn(SAMPLE_RESULT).when(importService).importFile(any(), any(), eq("nmea"), isNull());

        given()
            .multiPart("file", "sample.aivdm",
                "!AIVDM,1,1,,A,15RTgt0PAso;90TKcjM8h6g208CQ,0*4A\n".getBytes(),
                "text/plain")
            .multiPart("format", "nmea")
            .when().post("/api/import/file")
            .then()
            .statusCode(200)
            .body("positionsWritten", is(50));
    }

    @Test
    void importFile_csvMultipart_returns200() throws Exception {
        doReturn(new ImportResult(1, 0, 0, 10)).when(importService).importFile(any(), any(), eq("csv"), isNull());

        given()
            .multiPart("file", "data.csv",
                "mmsi,timestamp,lat,lon\n123456789,2024-01-15T08:00:00Z,51.5,2.3\n".getBytes(),
                "text/plain")
            .multiPart("format", "csv")
            .when().post("/api/import/file")
            .then()
            .statusCode(200)
            .body("positionsWritten", is(1));
    }

    @Test
    void importFile_withDate_parsesDateAndPassesToService() throws Exception {
        doReturn(SAMPLE_RESULT).when(importService).importFile(any(), any(), any(), any());

        given()
            .multiPart("file", "data.aivdm",
                "!AIVDM,1,1,,A,15RTgt0PAso;90TKcjM8h6g208CQ,0*4A\n".getBytes(),
                "text/plain")
            .multiPart("format", "nmea")
            .multiPart("date", "2024-01-15")
            .when().post("/api/import/file")
            .then()
            .statusCode(200);

        verify(importService).importFile(
            any(),
            any(),
            eq("nmea"),
            eq(java.time.LocalDate.of(2024, 1, 15)));
    }

    @Test
    void importFile_defaultFormat_usesNmea() throws Exception {
        doReturn(SAMPLE_RESULT).when(importService).importFile(any(), any(), eq("nmea"), isNull());

        given()
            .multiPart("file", "data.aivdm",
                "!AIVDM,1,1,,A,15RTgt0PAso;90TKcjM8h6g208CQ,0*4A\n".getBytes(),
                "text/plain")
            // no format field — should default to "nmea"
            .when().post("/api/import/file")
            .then()
            .statusCode(200);
    }

    @Test
    void importDma_resultFieldsAllPresent() throws Exception {
        doReturn(new ImportResult(100, 20, 8, 5000)).when(importService).importDma(any());

        given()
            .contentType(ContentType.JSON)
            .body("{\"date\":\"2024-01-15\"}")
            .when().post("/api/import/dma")
            .then()
            .statusCode(200)
            .body("positionsWritten", is(100))
            .body("vesselRecords", is(20))
            .body("tracksBuilt", is(8))
            .body("durationMs", is(5000));
    }
}
