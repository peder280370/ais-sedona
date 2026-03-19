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
        var parser = new CsvParser();
        var is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
        return parser.parse(is, "test");
    }

    private CsvParser.ParseResult parseFull(String csv) throws Exception {
        var parser = new CsvParser();
        var is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
        return parser.parseFull(is, "test");
    }

    @Test
    void parsesBasicRow() throws Exception {
        String csv = "mmsi,timestamp,lat,lon,sog,cog,heading,nav_status,rot,msg_type\n"
                   + "123456789,2024-01-15T08:00:00Z,51.5,2.3,12.5,180.0,180,0,0.0,1\n";
        var result = parse(csv);
        assertEquals(1, result.size());
        var p = result.getFirst();
        assertEquals(123456789L, p.mmsi());
        assertEquals(51.5, p.lat(), 1e-9);
        assertEquals(2.3, p.lon(), 1e-9);
        assertEquals(12.5f, p.sog(), 1e-3f);
        assertEquals(1, p.msgType());
        assertEquals("2024-01-15", p.getDatePartition());
    }

    @Test
    void parsesAlternativeTimestampFormat() throws Exception {
        String csv = "mmsi,timestamp,lat,lon\n"
                   + "234567890,2024-01-15 10:30:00,55.2,10.5\n";
        var result = parse(csv);
        assertEquals(1, result.size());
        assertEquals("2024-01-15", result.getFirst().getDatePartition());
    }

    @Test
    void parsesColumnNameSynonyms() throws Exception {
        String csv = "MMSI,BaseDateTime,LAT,LON,SOG,COG\n"
                   + "111222333,2024-01-15T09:00:00Z,60.0,5.0,8.0,270.0\n";
        var result = parse(csv);
        assertEquals(1, result.size());
        assertEquals(111222333L, result.getFirst().mmsi());
    }

    @Test
    void parsesOptionalNullableColumns() throws Exception {
        String csv = "mmsi,timestamp,lat,lon\n"
                   + "123456789,2024-01-15T08:00:00Z,51.5,2.3\n";
        var result = parse(csv);
        var p = result.getFirst();
        assertNull(p.sog());
        assertNull(p.cog());
        assertNull(p.heading());
    }

    @Test
    void skipsUnparsableRows() throws Exception {
        String csv = "mmsi,timestamp,lat,lon\n"
                   + "not-a-number,2024-01-15T08:00:00Z,51.5,2.3\n"
                   + "123456789,2024-01-15T08:05:00Z,51.4,2.4\n";
        var result = parse(csv);
        assertEquals(1, result.size());
    }

    @Test
    void parsesDmaFormat() throws Exception {
        String csv = "# Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,Heading,IMO,Callsign,Name,Ship type,Cargo type,Width,Length,Type of position fixing device,Draught,Destination,ETA,Data source type,A,B,C,D\n"
                   + "14/03/2026 00:00:00,Class A,123456789,55.538870,5.033200,Under way using engine,0,8.5,270.0,269,9876543,ABCDE,MY VESSEL,Cargo,,20,150,GPS,5.0,ROTTERDAM,15/03/2026 06:00:00,AIS,,,,\n";
        var result = parse(csv);
        assertEquals(1, result.size());
        var p = result.getFirst();
        assertEquals(123456789L, p.mmsi());
        assertEquals(55.538870, p.lat(), 1e-6);
        assertEquals(5.033200, p.lon(), 1e-6);
        assertEquals(8.5f, p.sog(), 1e-3f);
        assertEquals("2026-03-14", p.getDatePartition());
        // "Under way using engine" is non-numeric → navStatus should be null
        assertNull(p.navStatus());
    }

    @Test
    void parsesDmaFormatBaseStation() throws Exception {
        // Base stations have empty SOG/COG/Heading — should not crash
        String csv = "# Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,Heading,IMO,Callsign,Name,Ship type,Cargo type,Width,Length,Type of position fixing device,Draught,Destination,ETA,Data source type,A,B,C,D\n"
                   + "14/03/2026 00:00:00,Base Station,2194006,55.538870,5.033200,Unknown value,,,,,Unknown,Unknown,,Undefined,,,,Surveyed,,Unknown,,AIS,,,,\n";
        var result = parse(csv);
        assertEquals(1, result.size());
        var p = result.getFirst();
        assertEquals(2194006L, p.mmsi());
        assertNull(p.sog());
        assertNull(p.cog());
        assertNull(p.navStatus());
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
        var result = parseFull(csv);

        assertEquals(1, result.vessels().size());
        var v = result.vessels().getFirst();
        assertEquals(123456789L, v.mmsi());
        assertEquals(9876543L, v.imo());
        assertEquals("MY VESSEL", v.vesselName());
        assertEquals("ABCDE", v.callsign());
        assertEquals("Cargo", v.shipTypeDesc());
        assertEquals(150f, v.lengthM(), 0.01f);
        assertEquals(20f, v.beamM(), 0.01f);
        assertEquals(5.0f, v.draughtM(), 0.01f);
        assertEquals("ROTTERDAM", v.destination());
        assertNotNull(v.lastSeen());
    }

    @Test
    void nullsUnknownMetadataValues() throws Exception {
        String csv = "# Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,Heading,IMO,Callsign,Name,Ship type,Cargo type,Width,Length,Type of position fixing device,Draught,Destination,ETA,Data source type,A,B,C,D\n"
                   + "14/03/2026 00:00:00,Class A,123456789,55.5,5.0,Under way using engine,0,8.5,270.0,269,Unknown,Unknown,MY VESSEL,Undefined,,,,GPS,,Unknown,,AIS,,,,\n";
        var result = parseFull(csv);

        assertEquals(1, result.vessels().size());
        var v = result.vessels().getFirst();
        assertNull(v.imo());
        assertNull(v.callsign());
        assertNull(v.shipTypeDesc());
        assertNull(v.destination());
        assertEquals("MY VESSEL", v.vesselName());
    }

    @Test
    void skipsRowsWithNoUsefulMetadata() throws Exception {
        // Base station: all metadata fields are Unknown/empty
        String csv = "# Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,Heading,IMO,Callsign,Name,Ship type,Cargo type,Width,Length,Type of position fixing device,Draught,Destination,ETA,Data source type,A,B,C,D\n"
                   + "14/03/2026 00:00:00,Base Station,2194006,55.5,5.0,Unknown value,,,,,Unknown,Unknown,,Undefined,,,,Surveyed,,Unknown,,AIS,,,,\n";
        var result = parseFull(csv);

        assertEquals(1, result.positions().size());
        assertEquals(0, result.vessels().size());
    }

    @Test
    void deduplicatesMetadataByMmsi() throws Exception {
        // Two rows for same MMSI: first has name, second has destination
        String csv = "# Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,Heading,IMO,Callsign,Name,Ship type,Cargo type,Width,Length,Type of position fixing device,Draught,Destination,ETA,Data source type,A,B,C,D\n"
                   + "14/03/2026 00:00:00,Class A,123456789,55.5,5.0,Under way using engine,0,8.5,270.0,269,Unknown,Unknown,MY VESSEL,Unknown,,,,GPS,,Unknown,,AIS,,,,\n"
                   + "14/03/2026 01:00:00,Class A,123456789,55.6,5.1,Under way using engine,0,9.0,271.0,270,Unknown,Unknown,Unknown,Unknown,,,,GPS,,ROTTERDAM,,AIS,,,,\n";
        var result = parseFull(csv);

        assertEquals(2, result.positions().size());
        assertEquals(1, result.vessels().size());
        var v = result.vessels().getFirst();
        assertEquals("MY VESSEL", v.vesselName());    // from first row
        assertEquals("ROTTERDAM", v.destination());   // from second row
    }

    @Test
    void derivesLengthFromSizeColumnsWhenLengthMissing() throws Exception {
        // Length/Width columns empty, but A=100 B=20 C=10 D=8 present
        String csv = "# Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,Heading,IMO,Callsign,Name,Ship type,Cargo type,Width,Length,Type of position fixing device,Draught,Destination,ETA,Data source type,A,B,C,D\n"
                   + "14/03/2026 00:00:00,Class A,123456789,55.5,5.0,Under way using engine,0,8.5,270.0,269,Unknown,ABCDE,MY VESSEL,Cargo,,,,GPS,,Unknown,,AIS,100,20,10,8\n";
        var result = parseFull(csv);

        assertEquals(1, result.vessels().size());
        var v = result.vessels().getFirst();
        assertEquals(120f, v.lengthM(), 0.01f);  // A+B = 100+20
        assertEquals(18f,  v.beamM(),   0.01f);  // C+D = 10+8
    }

    @Test
    void noVesselsForMinimalCsv() throws Exception {
        // CSV with no metadata columns at all
        String csv = "mmsi,timestamp,lat,lon\n"
                   + "123456789,2024-01-15T08:00:00Z,51.5,2.3\n";
        var result = parseFull(csv);

        assertEquals(1, result.positions().size());
        assertEquals(0, result.vessels().size());
    }
}
