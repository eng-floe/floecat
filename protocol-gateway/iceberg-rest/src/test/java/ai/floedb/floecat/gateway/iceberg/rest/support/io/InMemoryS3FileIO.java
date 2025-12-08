package ai.floedb.floecat.gateway.iceberg.rest.support.io;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * Simple {@link FileIO} used in tests to avoid hitting a real S3 bucket. It keeps a mirrored tree
 * under a local directory while preserving {@code s3://bucket/key} locations so the rest of the
 * gateway continues to behave as if S3 were present.
 */
public final class InMemoryS3FileIO implements FileIO {

  private static final String DEFAULT_ROOT =
      Paths.get(System.getProperty("java.io.tmpdir"), "floecat-test-s3").toString();

  private Path root;
  private Map<String, String> props = Collections.emptyMap();

  @Override
  public void initialize(Map<String, String> properties) {
    this.props = properties == null ? Map.of() : Map.copyOf(properties);
    String configuredRoot = props.getOrDefault("fs.floecat.test-root", DEFAULT_ROOT);
    root = Paths.get(configuredRoot).toAbsolutePath();
  }

  @Override
  public InputFile newInputFile(String location) {
    Path local = toLocalPath(location, false);
    return new LocalInputFile(location, local);
  }

  @Override
  public InputFile newInputFile(String location, long length) {
    return newInputFile(location);
  }

  @Override
  public OutputFile newOutputFile(String location) {
    Path local = toLocalPath(location, true);
    return new LocalOutputFile(location, local);
  }

  @Override
  public void deleteFile(String location) {
    Path local = toLocalPath(location, false);
    try {
      Files.deleteIfExists(local);
    } catch (IOException e) {
      throw new RuntimeException("Failed to delete " + location, e);
    }
  }

  @Override
  public Map<String, String> properties() {
    return props;
  }

  @Override
  public void close() {}

  private Path toLocalPath(String location, boolean ensureParent) {
    URI uri = URI.create(location);
    if (!"s3".equalsIgnoreCase(uri.getScheme())) {
      throw new IllegalArgumentException("Only s3:// URIs are supported: " + location);
    }
    String bucket = uri.getHost();
    if (bucket == null || bucket.isBlank()) {
      throw new IllegalArgumentException("Missing bucket in s3 URI: " + location);
    }
    String key = uri.getPath();
    String suffix = key == null ? "" : key.replaceFirst("^/", "");
    Path resolved =
        suffix == null || suffix.isEmpty()
            ? root.resolve(bucket)
            : root.resolve(bucket).resolve(suffix);
    if (ensureParent) {
      try {
        Path parent = resolved.getParent();
        if (parent != null) {
          Files.createDirectories(parent);
        } else {
          Files.createDirectories(resolved);
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to prepare path for " + location, e);
      }
    }
    return resolved;
  }

  private static final class LocalInputFile implements InputFile {
    private final String location;
    private final Path path;

    private LocalInputFile(String location, Path path) {
      this.location = location;
      this.path = path;
    }

    @Override
    public long getLength() {
      try {
        return Files.size(path);
      } catch (IOException e) {
        throw new RuntimeException("Failed to read size of " + location, e);
      }
    }

    @Override
    public SeekableInputStream newStream() {
      try {
        SeekableByteChannel channel = Files.newByteChannel(path);
        return new SeekableInputStream() {
          @Override
          public void seek(long newPos) throws IOException {
            channel.position(newPos);
          }

          @Override
          public long getPos() throws IOException {
            return channel.position();
          }

          @Override
          public int read() throws IOException {
            byte[] buf = new byte[1];
            int read = read(buf, 0, 1);
            return read == -1 ? -1 : buf[0] & 0xFF;
          }

          @Override
          public int read(byte[] b, int off, int len) throws IOException {
            ByteBuffer buffer = ByteBuffer.wrap(b, off, len);
            int read = channel.read(buffer);
            return read < 0 ? -1 : read;
          }

          @Override
          public void close() throws IOException {
            channel.close();
          }
        };
      } catch (IOException e) {
        throw new NotFoundException("Unable to open " + location, e);
      }
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public boolean exists() {
      return Files.exists(path);
    }
  }

  private static final class LocalOutputFile implements OutputFile {
    private final String location;
    private final Path path;

    private LocalOutputFile(String location, Path path) {
      this.location = location;
      this.path = path;
    }

    @Override
    public PositionOutputStream create() {
      return createInternal(false);
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return createInternal(true);
    }

    private PositionOutputStream createInternal(boolean overwrite) {
      try {
        if (!overwrite && Files.exists(path)) {
          throw new IOException("File already exists: " + path);
        }
        Files.createDirectories(path.getParent());
        OutputStream out = Files.newOutputStream(path);
        return new PositionOutputStream() {
          private long pos = 0;

          @Override
          public void write(int b) throws IOException {
            out.write(b);
            pos++;
          }

          @Override
          public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            pos += len;
          }

          @Override
          public long getPos() {
            return pos;
          }

          @Override
          public void close() throws IOException {
            out.close();
          }
        };
      } catch (IOException e) {
        throw new RuntimeException("Failed to create " + location, e);
      }
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public InputFile toInputFile() {
      return new LocalInputFile(location, path);
    }
  }
}
