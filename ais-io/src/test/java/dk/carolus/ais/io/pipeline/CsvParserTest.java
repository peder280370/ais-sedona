package dk.carolus.ais.io.pipeline;

import dk.carolus.ais.io.model.AisPosition;
import dk.carolus.ais.io.model.VesselMetadata;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CsvParserTest {

    private List<AisPosition> parse(String csv) throws Exception {
        CsvParser parser = new CsvParser();
        ByteArrayInputStream is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
        return parser.parse(is, "test");
    }

    private CsvParser.ParseResult parseFull(String csv) throws Exception {
        CsvParser parser = new CsvParser();
        ByteArrayInputStream is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
        return parser.parseFull(is, "test");
    }

    @Test
    void parsesBasicRow() throws Exception {
        String csv = "mmsi,timestamp,lat,lon,sog,cog,heading,nav_status,rot,msg_type\n"
                   + "123456789,2024-01-15T08:00:00Z,51.5,2.3,12.5,180.0,180,0,0.0,1\n";
        List<AisPosition> result = parse(csv);
        assertEquals(1, result.size());
        AisPosition p = result.get(0);
        assertEquals(123456789L, p.getMmsi());
        assertEquals(51.5, p.getLat(), 1e-9);
        assertEquals(2.3, p.getLon(), 1e-9);
        assertEquals(12.5f, p.getSog(), 1e-3f);
        assertEquals(1, p.getMsgType());
        assertEquals("2024-01-15", p.getDatePartition());
    }

    @Test
    void parsesAlternativeTimestampFormat() throws Exception {
        String csv = "mmsi,timestamp,lat,lon\n"
                   + "234567890,2024-01-15 10:30:00,55.2,10.5\n";
        List<AisPosition> result = parse(csv);
        assertEquals(1, result.size());
        assertEquals("2024-01-15", result.get(0).getDatePartition());
    }

    @Test
    void parsesColumnNameSynonyms() throws Exception {
        String csv = "MMSI,BaseDateTime,LAT,LON,SOG,COG\n"
                   + "111222333,2024-01-15T09:00:00Z,60.0,5.0,8.0,270.0\n";
        List<AisPosition> result = parse(csv);
        assertEquals(1, result.size());
        assertEquals(111222333L, result.get(0).getMmsi());
    }

    @Test
    void parsesOptionalNullableColumns() throws Exception {
        String csv = "mmsi,timestamp,lat,lon\n"
                   + "123456789,2024-01-15T08:00:00Z,51.5,2.3\n";
        List<AisPosition> result = parse(csv);
        AisPosition p = result.get(0);
        assertNull(p.getSog());
        assertNull(p.getCog());
        assertNull(p.getHeading());
    }

    @Test
    void skipsUnparsableRows() throws Exception {
        String csv = "mmsi,timestamp,lat,lon\n"
                   + "not-a-number,2024-01-15T08:00:00Z,51.5,2.3\n"
                   + "123456789,2024-01-15T08:05:00Z,51.4,2.4\n";
        List<AisPosition> result = parse(csv);
        assertEquals(1, result.size());
    }

    @Test
    void parsesDmaFormat() throws Exception {
        String csv = "# Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,Heading,IMO,Callsign,Name,Ship type,Cargo type,Width,Length,Type of position fixing device,Draught,Destination,ETA,Data source type,A,B,C,D\n"
                   + "14/03/2026 00:00:00,Class A,123456789,55.538870,5.033200,Under way using engine,0,8.5,270.0,269,9876543,ABCDE,MY VESSEL,Cargo,,20,150,GPS,5.0,ROTTERDAM,15/03/2026 06:00:00,AIS,,,,\n";
        List<AisPosition> result = parse(csv);
        assertEquals(1, result.size());
        AisPosition p = result.get(0);
        assertEquals(123456789L, p.getMmsi());
        assertEquals(55.538870, p.getLat(), 1e-6);
        assertEquals(5.033200, p.getLon(), 1e-6);
        assertEquals(8.5f, p.getSog(), 1e-3f);
        assertEquals("2026-03-14", p.getDatePartition());
        // "Under way using engine" is non-numeric → navStatus should be null
        assertNull(p.getNavStatus());
    }

    @Test
    void parsesDmaFormatBaseStation() throws Exception {
        // Base stations have empty SOG/COG/Heading — should not crash
        String csv = "# Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,Heading,IMO,Callsign,Name,Ship type,Cargo type,Width,Length,Type of position fixing device,Draught,Destination,ETA,Data source type,A,B,C,D\n"
                   + "14/03/2026 00:00:00,Base Station,2194006,55.538870,5.033200,Unknown value,,,,,Unknown,Unknown,,Undefined,,,,Surveyed,,Unknown,,AIS,,,,\n";
        List<AisPosition> result = parse(csv);
        assertEquals(1, result.size());
        AisPosition p = result.get(0);
        assertEquals(2194006L, p.getMmsi());
        assertNull(p.getSog());
        assertNull(p.getCog());
        assertNull(p.getNavStatus());
    }

    @Test
    void handlesCsvQuotedFields() {
        String line = "\"hello, world\",123,\"quoted\"";
        String[] tokens = CsvParser.splitCsvLine(line);
        assertEquals(3, tokens.length);
        assertEquals("hello, world", tokens[0]);
        assertEquals("123", tokens[1]);
        assertEquals("quoted", tokens[2]);
    }

    // ---- Vessel metadata tests ---------------------------------------------

    @Test
    void extractsVesselMetadataFromDmaRow() throws Exception {
        String csv = "# Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,Heading,IMO,Callsign,Name,Ship type,Cargo type,Width,Length,Type of position fixing device,Draught,Destination,ETA,Data source type,A,B,C,D\n"
                   + "14/03/2026 00:00:00,Class A,123456789,55.5,5.0,Under way using engine,0,8.5,270.0,269,9876543,ABCDE,MY VESSEL,Cargo,,20,150,GPS,5.0,ROTTERDAM,15/03/2026 06:00:00,AIS,,,,\n";
        CsvParser.ParseResult result = parseFull(csv);

        assertEquals(1, result.getVessels().size());
        VesselMetadata v = result.getVessels().get(0);
        assertEquals(123456789L, v.getMmsi());
        assertEquals(9876543L, v.getImo());
        assertEquals("MY VESSEL", v.getVesselName());
        assertEquals("ABCDE", v.getCallsign());
        assertEquals("Cargo", v.getShipTypeDesc());
        assertEquals(150f, v.getLengthM(), 0.01f);
        assertEquals(20f, v.getBeamM(), 0.01f);
        assertEquals(5.0f, v.getDraughtM(), 0.01f);
        assertEquals("ROTTERDAM", v.getDestination());
        assertNotNull(v.getLastSeen());
    }

    @Test
    void nullsUnknownMetadataValues() throws Exception {
        String csv = "# Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,Heading,IMO,Callsign,Name,Ship type,Cargo type,Width,Length,Type of position fixing device,Draught,Destination,ETA,Data source type,A,B,C,D\n"
                   + "14/03/2026 00:00:00,Class A,123456789,55.5,5.0,Under way using engine,0,8.5,270.0,269,Unknown,Unknown,MY VESSEL,Undefined,,,,GPS,,Unknown,,AIS,,,,\n";
        CsvParser.ParseResult result = parseFull(csv);

        assertEquals(1, result.getVessels().size());
        VesselMetadata v = result.getVessels().get(0);
        assertNull(v.getImo());
        assertNull(v.getCallsign());
        assertNull(v.getShipTypeDesc());
        assertNull(v.getDestination());
        assertEquals("MY VESSEL", v.getVesselName());
    }

    @Test
    void skipsRowsWithNoUsefulMetadata() throws Exception {
        // Base station: all metadata fields are Unknown/empty
        String csv = "# Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,Heading,IMO,Callsign,Name,Ship type,Cargo type,Width,Length,Type of position fixing device,Draught,Destination,ETA,Data source type,A,B,C,D\n"
                   + "14/03/2026 00:00:00,Base Station,2194006,55.5,5.0,Unknown value,,,,,Unknown,Unknown,,Undefined,,,,Surveyed,,Unknown,,AIS,,,,\n";
        CsvParser.ParseResult result = parseFull(csv);

        assertEquals(1, result.getPositions().size());
        assertEquals(0, result.getVessels().size());
    }

    @Test
    void deduplicatesMetadataByMmsi() throws Exception {
        // Two rows for same MMSI: first has name, second has destination
        String csv = "# Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,Heading,IMO,Callsign,Name,Ship type,Cargo type,Width,Length,Type of position fixing device,Draught,Destination,ETA,Data source type,A,B,C,D\n"
                   + "14/03/2026 00:00:00,Class A,123456789,55.5,5.0,Under way using engine,0,8.5,270.0,269,Unknown,Unknown,MY VESSEL,Unknown,,,,GPS,,Unknown,,AIS,,,,\n"
                   + "14/03/2026 01:00:00,Class A,123456789,55.6,5.1,Under way using engine,0,9.0,271.0,270,Unknown,Unknown,Unknown,Unknown,,,,GPS,,ROTTERDAM,,AIS,,,,\n";
        CsvParser.ParseResult result = parseFull(csv);

        assertEquals(2, result.getPositions().size());
        assertEquals(1, result.getVessels().size());
        VesselMetadata v = result.getVessels().get(0);
        assertEquals("MY VESSEL", v.getVesselName());    // from first row
        assertEquals("ROTTERDAM", v.getDestination());   // from second row
    }

    @Test
    void derivesLengthFromSizeColumnsWhenLengthMissing() throws Exception {
        // Length/Width columns empty, but A=100 B=20 C=10 D=8 present
        String csv = "# Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,Heading,IMO,Callsign,Name,Ship type,Cargo type,Width,Length,Type of position fixing device,Draught,Destination,ETA,Data source type,A,B,C,D\n"
                   + "14/03/2026 00:00:00,Class A,123456789,55.5,5.0,Under way using engine,0,8.5,270.0,269,Unknown,ABCDE,MY VESSEL,Cargo,,,,GPS,,Unknown,,AIS,100,20,10,8\n";
        CsvParser.ParseResult result = parseFull(csv);

        assertEquals(1, result.getVessels().size());
        VesselMetadata v = result.getVessels().get(0);
        assertEquals(120f, v.getLengthM(), 0.01f);  // A+B = 100+20
        assertEquals(18f,  v.getBeamM(),   0.01f);  // C+D = 10+8
    }

    @Test
    void noVesselsForMinimalCsv() throws Exception {
        // CSV with no metadata columns at all
        String csv = "mmsi,timestamp,lat,lon\n"
                   + "123456789,2024-01-15T08:00:00Z,51.5,2.3\n";
        CsvParser.ParseResult result = parseFull(csv);

        assertEquals(1, result.getPositions().size());
        assertEquals(0, result.getVessels().size());
    }
}
