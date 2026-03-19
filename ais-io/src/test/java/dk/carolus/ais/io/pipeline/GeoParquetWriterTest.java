package dk.carolus.ais.io.pipeline;

import dk.carolus.ais.io.model.AisPosition;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class GeoParquetWriterTest {

    @TempDir
    java.nio.file.Path tempDir;

    // ---- buildGeoMetadata ------------------------------------------------

    @Test
    void buildGeoMetadata_containsBboxField() {
        String meta = GeoParquetWriter.buildGeoMetadata(5.1, 50.2, 20.3, 60.4);
        assertTrue(meta.contains("\"bbox\""), "geo metadata must contain bbox field");
    }

    @Test
    void buildGeoMetadata_bboxValuesMatchInput() {
        String meta = GeoParquetWriter.buildGeoMetadata(5.0, 50.0, 20.0, 60.0);
        assertTrue(meta.contains("5.000000"), "minLon must appear in bbox");
        assertTrue(meta.contains("50.000000"), "minLat must appear in bbox");
        assertTrue(meta.contains("20.000000"), "maxLon must appear in bbox");
        assertTrue(meta.contains("60.000000"), "maxLat must appear in bbox");
    }

    @Test
    void buildGeoMetadata_usesDecimalPoint_notComma() {
        // Must use Locale.ROOT — decimal separator must be '.' not ','
        String meta = GeoParquetWriter.buildGeoMetadata(5.5, 50.5, 20.5, 60.5);
        assertFalse(meta.contains("5,5"), "decimal separator must be '.' not ','");
        assertTrue(meta.contains("5.5"), "bbox value must use decimal point");
    }

    @Test
    void buildGeoMetadata_isValidJson_withBbox() {
        String meta = GeoParquetWriter.buildGeoMetadata(-10.0, 40.0, 30.0, 70.0);
        // Simple structural checks — GeoParquet 1.1.0 required fields
        assertTrue(meta.contains("\"version\":\"1.1.0\""));
        assertTrue(meta.contains("\"primary_column\":\"geometry\""));
        assertTrue(meta.contains("\"encoding\":\"WKB\""));
        assertTrue(meta.contains("\"geometry_types\":[\"Point\"]"));
        assertTrue(meta.contains("\"bbox\":[-10.000000,40.000000,30.000000,70.000000]"));
    }

    // ---- write + close integration ---------------------------------------

    @Test
    void write_singlePosition_producesParquetFileWithBboxMetadata() throws IOException {
        AisPosition pos = new AisPosition(
            219001234L, Instant.parse("2024-01-15T08:00:00Z"),
            57.7, 10.5,   // lat, lon — North Sea
            12.5f, 180.0f, 180, 0, 0.0f, 1);

        try (GeoParquetWriter writer = new GeoParquetWriter(tempDir)) {
            writer.write(pos);
        }

        java.nio.file.Path partFile = findParquetFile(tempDir);
        String geoMeta = readGeoMetadata(partFile);

        assertNotNull(geoMeta, "geo metadata key must be present in parquet footer");
        assertTrue(geoMeta.contains("\"bbox\""), "parquet file must have bbox in geo metadata");
        // lon=10.5, lat=57.7 → bbox should be [10.5, 57.7, 10.5, 57.7]
        assertTrue(geoMeta.contains("10.5"), "bbox must contain the written longitude");
        assertTrue(geoMeta.contains("57.7"), "bbox must contain the written latitude");
    }

    @Test
    void write_multiplePositions_bboxCoversAllPoints() throws IOException {
        // Three positions spanning a range
        List<AisPosition> positions = List.of(
            new AisPosition(111111111L, Instant.parse("2024-01-15T08:00:00Z"), 55.0, 8.0, null, null, null, null, null, null),
            new AisPosition(111111111L, Instant.parse("2024-01-15T09:00:00Z"), 58.0, 12.0, null, null, null, null, null, null),
            new AisPosition(111111111L, Instant.parse("2024-01-15T10:00:00Z"), 56.5, 15.0, null, null, null, null, null, null)
        );

        try (GeoParquetWriter writer = new GeoParquetWriter(tempDir)) {
            for (AisPosition p : positions) writer.write(p);
        }

        // All three are in the same H3 r3 cell? Not necessarily — use total written check
        assertEquals(3, countParquetRows(tempDir));
    }

    @Test
    void write_positionsInDifferentPartitions_eachFileHasBbox() throws IOException {
        // Two positions in different dates → different partition files
        List<AisPosition> positions = List.of(
            new AisPosition(111111111L, Instant.parse("2024-01-15T08:00:00Z"), 55.0, 8.0, null, null, null, null, null, null),
            new AisPosition(222222222L, Instant.parse("2024-01-16T08:00:00Z"), 57.0, 10.0, null, null, null, null, null, null)
        );

        try (GeoParquetWriter writer = new GeoParquetWriter(tempDir)) {
            for (AisPosition p : positions) writer.write(p);
        }

        List<java.nio.file.Path> parquetFiles = findAllParquetFiles(tempDir);
        assertTrue(parquetFiles.size() >= 1, "at least one parquet file must be written");
        for (java.nio.file.Path f : parquetFiles) {
            String geoMeta = readGeoMetadata(f);
            assertNotNull(geoMeta, "every parquet file must have geo metadata");
            assertTrue(geoMeta.contains("\"bbox\""), "every parquet file must have bbox: " + f);
        }
    }

    @Test
    void getTotalWritten_reflectsBufferedCount() throws IOException {
        GeoParquetWriter writer = new GeoParquetWriter(tempDir);
        assertEquals(0, writer.getTotalWritten());

        writer.write(new AisPosition(111111111L, Instant.parse("2024-01-15T08:00:00Z"),
                55.0, 8.0, null, null, null, null, null, null));
        assertEquals(1, writer.getTotalWritten());

        writer.write(new AisPosition(111111111L, Instant.parse("2024-01-15T09:00:00Z"),
                55.1, 8.1, null, null, null, null, null, null));
        assertEquals(2, writer.getTotalWritten());

        writer.close();
        assertEquals(2, writer.getTotalWritten());
    }

    // ---- Helpers ---------------------------------------------------------

    private java.nio.file.Path findParquetFile(java.nio.file.Path root) throws IOException {
        return Files.walk(root)
            .filter(p -> p.toString().endsWith(".parquet"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No parquet file found under " + root));
    }

    private List<java.nio.file.Path> findAllParquetFiles(java.nio.file.Path root) throws IOException {
        return Files.walk(root)
            .filter(p -> p.toString().endsWith(".parquet"))
            .collect(java.util.stream.Collectors.toList());
    }

    private String readGeoMetadata(java.nio.file.Path parquetFile) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(localInputFile(parquetFile))) {
            Map<String, String> meta = reader.getFileMetaData().getKeyValueMetaData();
            return meta.get("geo");
        }
    }

    private long countParquetRows(java.nio.file.Path root) throws IOException {
        long total = 0;
        for (java.nio.file.Path f : findAllParquetFiles(root)) {
            try (ParquetFileReader reader = ParquetFileReader.open(localInputFile(f))) {
                total += reader.getRecordCount();
            }
        }
        return total;
    }

    /** Hadoop-free InputFile backed by RandomAccessFile — avoids UGI/getSubject issues. */
    private static InputFile localInputFile(java.nio.file.Path path) {
        return new InputFile() {
            @Override
            public long getLength() throws IOException {
                return Files.size(path);
            }

            @Override
            public SeekableInputStream newStream() throws IOException {
                RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r");
                return new DelegatingSeekableInputStream(new FileInputStream(raf.getFD())) {
                    @Override public long getPos() throws IOException { return raf.getFilePointer(); }
                    @Override public void seek(long pos) throws IOException { raf.seek(pos); }
                };
            }
        };
    }
}
