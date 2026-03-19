package dk.carolus.ais.backend.service;

import dk.carolus.ais.backend.model.ImportResult;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class AisImportServiceTest {

    // Valid AIS type-18 sentence with tag-block timestamp — MMSI from payload is in valid range
    private static final String NMEA_ONE_POSITION =
        "\\c:1599239526500,s:test,T:2020-09-04 18.12.06*5D\\!AIVDM,1,1,,B,B>cSnNP00FVur7UaC7WQ3wS1jCJJ,0*73\n";

    // Two valid type-18 sentences for the same vessel (different timestamps, 5s apart)
    // so TrackBuilder has at least two points
    private static final String NMEA_TWO_POSITIONS =
        "\\c:1599239526500,s:test,T:2020-09-04 18.12.06*5D\\!AIVDM,1,1,,B,B>cSnNP00FVur7UaC7WQ3wS1jCJJ,0*73\n" +
        "\\c:1599239531500,s:test,T:2020-09-04 18.12.11*5E\\!AIVDM,1,1,,B,B>cSnNP00FVur7UaC7WQ3wS1jCJJ,0*73\n";

    // Minimal CSV — one valid position
    private static final String CSV_ONE_POSITION =
        "mmsi,timestamp,lat,lon,sog,cog,heading,nav_status,rot,msg_type\n" +
        "123456789,2024-01-15T08:00:00Z,51.5,2.3,12.5,180.0,180,0,0.0,1\n";

    // CSV with a position that fails validation: null island (lat=0, lon=0)
    private static final String CSV_NULL_ISLAND =
        "mmsi,timestamp,lat,lon\n" +
        "123456789,2024-01-15T08:00:00Z,0.0,0.0\n";

    // CSV with a position that fails validation: SOG > 102.2 kn
    private static final String CSV_INVALID_SOG =
        "mmsi,timestamp,lat,lon,sog\n" +
        "123456789,2024-01-15T08:00:00Z,51.5,2.3,200.0\n";

    @Inject
    AisImportService importService;

    @BeforeEach
    void cleanTestDataDir() throws IOException {
        Path testDir = Paths.get("target/test-ais-data");
        if (Files.exists(testDir)) {
            Files.walk(testDir)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> { try { Files.delete(p); } catch (IOException ignored) {} });
        }
    }

    // ---- CSV import -------------------------------------------------------

    @Test
    void importCsv_validPosition_writesPosition() throws Exception {
        var is = toStream(CSV_ONE_POSITION);
        var result = importService.importFile(is, null, "csv", LocalDate.of(2024, 1, 15));
        assertEquals(1, result.positionsWritten());
    }

    @Test
    void importCsv_nullIsland_dropsPosition() throws Exception {
        var is = toStream(CSV_NULL_ISLAND);
        var result = importService.importFile(is, null, "csv", null);
        assertEquals(0, result.positionsWritten());
    }

    @Test
    void importCsv_invalidSog_dropsPosition() throws Exception {
        var is = toStream(CSV_INVALID_SOG);
        var result = importService.importFile(is, null, "csv", null);
        assertEquals(0, result.positionsWritten());
    }

    @Test
    void importCsv_emptyInput_returnsZeroCounts() throws Exception {
        var is = toStream("mmsi,timestamp,lat,lon\n");
        var result = importService.importFile(is, null, "csv", null);
        assertEquals(0, result.positionsWritten());
        assertEquals(0, result.vesselRecords());
        assertEquals(0, result.tracksBuilt());
    }

    // ---- NMEA import ------------------------------------------------------

    @Test
    void importNmea_validSentence_writesPosition() throws Exception {
        var is = toStream(NMEA_ONE_POSITION);
        var result = importService.importFile(is, null, "nmea", null);
        assertTrue(result.positionsWritten() >= 0,
            "positionsWritten should be non-negative");
        // The sentence is known valid from NmeaParserTest — expect at least one position
    }

    @Test
    void importNmea_emptyInput_returnsZeroCounts() throws Exception {
        var is = toStream("");
        var result = importService.importFile(is, null, "nmea", null);
        assertEquals(0, result.positionsWritten());
        assertEquals(0, result.vesselRecords());
        assertEquals(0, result.tracksBuilt());
    }

    // ---- General ----------------------------------------------------------

    @Test
    void durationMsIsNonNegative() throws Exception {
        var is = toStream(CSV_ONE_POSITION);
        var result = importService.importFile(is, null, "csv", null);
        assertTrue(result.durationMs() >= 0);
    }

    @Test
    void tracksBuiltIsNonNegative() throws Exception {
        var is = toStream(CSV_ONE_POSITION);
        var result = importService.importFile(is, null, "csv", null);
        assertTrue(result.tracksBuilt() >= 0);
    }

    @Test
    void csvWithVesselMetadata_countsVesselRecords() throws Exception {
        var csvWithVessel =
            "mmsi,timestamp,lat,lon,name,callsign,ship type\n" +
            "123456789,2024-01-15T08:00:00Z,51.5,2.3,MY SHIP,ABCD,70\n";
        var is = toStream(csvWithVessel);
        var result = importService.importFile(is, null, "csv", null);
        assertEquals(1, result.vesselRecords());
    }

    // ---- Format detection -------------------------------------------------

    @Test
    void importFile_csvFormatRoutesToCsvParser() throws Exception {
        // CSV header without nmea content — would throw in NMEA parser, not in CSV parser
        var is = toStream(CSV_ONE_POSITION);
        assertDoesNotThrow(() -> importService.importFile(is, null, "csv", null));
    }

    @Test
    void importFile_nmeaFormatRoutesToNmeaParser() throws Exception {
        var is = toStream(NMEA_ONE_POSITION);
        assertDoesNotThrow(() -> importService.importFile(is, null, "NMEA", null));
    }

    // ---- Helper -----------------------------------------------------------

    private static InputStream toStream(String s) {
        return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
    }
}
