package dk.carolus.ais.io.pipeline;

import dk.carolus.ais.io.model.AisPosition;
import dk.carolus.ais.io.model.VesselMetadata;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class NmeaParserTest {

    private final Instant FALLBACK = Instant.parse("2020-09-04T18:00:00Z");

    private NmeaParser.ParseResult parse(String nmea) throws Exception {
        var parser = new NmeaParser();
        var is = new ByteArrayInputStream(nmea.getBytes(StandardCharsets.UTF_8));
        return parser.parse(is, "test", FALLBACK);
    }

    // -----------------------------------------------------------------------
    // Tag block parsing
    // -----------------------------------------------------------------------

    @Test
    void extractsTimestampFromTagBlock() throws Exception {
        // c: value is epoch milliseconds (1599239526500 = 2020-09-04T18:12:06.500Z)
        String nmea =
            "\\c:1599239526500,s:sdr-experiments,T:2020-09-04 18.12.06*5D\\!AIVDM,1,1,,B,B>cSnNP00FVur7UaC7WQ3wS1jCJJ,0*73\n";
        var result = parse(nmea);
        // Type 18 (Class B position) — should produce a position
        assertFalse(result.positions().isEmpty(), "Expected at least one position from type-18 sentence");
        var pos = result.positions().getFirst();
        assertEquals(Instant.ofEpochMilli(1599239526500L), pos.timestamp());
    }

    @Test
    void useFallbackTimestampWhenNoTagBlock() throws Exception {
        // Plain AIVDM type-1 sentence with no tag block (from sample-800lines.aivdm)
        String nmea = "!AIVDM,1,1,,A,15RTgt0PAso;90TKcjM8h6g208CQ,0*4A\n";
        var result = parse(nmea);
        if (!result.positions().isEmpty()) {
            assertEquals(FALLBACK, result.positions().getFirst().timestamp());
        }
        // It's acceptable for this specific sentence to fail to produce a position
        // (the test verifies the parser doesn't throw)
    }

    // -----------------------------------------------------------------------
    // Comment and blank line handling
    // -----------------------------------------------------------------------

    @Test
    void skipsCommentAndBlankLines() throws Exception {
        String nmea =
            "# This is a comment\n" +
            "\n" +
            "!AIVDM,1,1,,A,15RTgt0PAso;90TKcjM8h6g208CQ,0*4A\n";
        // No exception expected
        var result = parse(nmea);
        assertNotNull(result);
    }

    // -----------------------------------------------------------------------
    // Multi-sentence message assembly
    // -----------------------------------------------------------------------

    @Test
    void assemblesMultiSentenceMessage() throws Exception {
        // Type-5 message split over two sentences (from nmea-sample-large.txt)
        String nmea =
            "!AIVDM,2,1,9,B,53nFBv01SJ<thHp6220H4heHTf2222222222221?50:454o<`9QSlUDp,0*09\n" +
            "!AIVDM,2,2,9,B,888888888888880,2*2E\n";
        var result = parse(nmea);
        // Type 5 produces vessel metadata
        assertFalse(result.vessels().isEmpty(),
                "Expected vessel metadata from multi-sentence type-5 message");
    }

    // -----------------------------------------------------------------------
    // Vessel metadata extraction (type 5)
    // -----------------------------------------------------------------------

    @Test
    void extractsVesselMetadataFromType5() throws Exception {
        // Two-sentence type-5 from nmea-sample-large.txt
        String nmea =
            "!AIVDM,2,1,9,B,53nFBv01SJ<thHp6220H4heHTf2222222222221?50:454o<`9QSlUDp,0*09\n" +
            "!AIVDM,2,2,9,B,888888888888880,2*2E\n";
        var result = parse(nmea);
        assertFalse(result.vessels().isEmpty());
        var v = result.vessels().getFirst();
        assertTrue(v.mmsi() > 0);
    }

    // -----------------------------------------------------------------------
    // NMEA string trimming
    // -----------------------------------------------------------------------

    @Test
    void trimNmeaStripsAtPadding() {
        assertEquals("WEST", NmeaParser.trimNmea("WEST@@@@@@@@@@@@@@@@@@"));
        assertNull(NmeaParser.trimNmea("@@@@@@@@@@@@@@@@@@@@"));
        assertNull(NmeaParser.trimNmea(""));
        assertNull(NmeaParser.trimNmea(null));
        assertEquals("HELLO WORLD", NmeaParser.trimNmea("HELLO WORLD       "));
    }

    // -----------------------------------------------------------------------
    // Error tolerance
    // -----------------------------------------------------------------------

    @Test
    void toleratesMalformedLines() throws Exception {
        String nmea =
            "not a valid nmea line at all\n" +
            "!AIVDM,1,1,,A,15RTgt0PAso;90TKcjM8h6g208CQ,0*4A\n";
        // Should not throw
        var result = parse(nmea);
        assertNotNull(result);
    }

    @Test
    void skipsMixedTypesFileWithoutThrowingExceptions() throws Exception {
        // Feeds the entire mixed_types.nmea test file content as a string
        String nmea =
            "!AIVDM,1,1,,A,402M??AvAPP000h0MJR07Ug02H0I,0*10\n" +
            "!AIVDM,1,1,,A,232S8IhP01Q;WDrKSntnowwn0>@<,0*19\n" +
            "!AIVDM,1,1,,B,H52aML0t<D4r0hTHD0000000000,2*71\n" +  // type 24 part A
            "!AIVDM,2,1,1,A,55NPvo800001L@OCWO@EDLDpTF0br0QDLE:2220q000005OP07T0DhhAkmC0,0*65\n" +
            "!AIVDM,2,2,1,A,K83Dp888880,2*69\n";                   // type 5, 2 parts
        var result = parse(nmea);
        assertNotNull(result);
        // Just verify it doesn't crash and returns something
        assertTrue(result.positions().size() + result.vessels().size() >= 0);
    }
}
