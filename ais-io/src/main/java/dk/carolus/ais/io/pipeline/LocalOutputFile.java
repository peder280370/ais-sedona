package dk.carolus.ais.io.pipeline;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A Parquet {@link OutputFile} backed by a local {@link java.io.File}.
 *
 * <p>This implementation avoids the Hadoop {@code FileSystem} API entirely,
 * making it compatible with Java 21+ (where {@code Subject.getSubject()} is
 * no longer supported).
 */
final class LocalOutputFile implements OutputFile {

    private final File file;

    LocalOutputFile(File file) {
        this.file = file;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        return wrap(new FileOutputStream(file, false));
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        return wrap(new FileOutputStream(file, false));
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }

    @Override
    public String getPath() {
        return file.getAbsolutePath();
    }

    private static PositionOutputStream wrap(OutputStream raw) {
        BufferedOutputStream buf = new BufferedOutputStream(raw, 64 * 1024);
        return new PositionOutputStream() {
            private long pos = 0;

            @Override
            public long getPos() {
                return pos;
            }

            @Override
            public void write(int b) throws IOException {
                buf.write(b);
                pos++;
            }

            @Override
            public void write(byte[] bytes, int off, int len) throws IOException {
                buf.write(bytes, off, len);
                pos += len;
            }

            @Override
            public void flush() throws IOException {
                buf.flush();
            }

            @Override
            public void close() throws IOException {
                buf.close();
            }
        };
    }
}
