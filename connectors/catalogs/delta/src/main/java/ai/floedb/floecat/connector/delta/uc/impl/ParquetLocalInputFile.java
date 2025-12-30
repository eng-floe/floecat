package ai.floedb.floecat.connector.delta.uc.impl;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

final class ParquetLocalInputFile implements InputFile {
  private final String path;
  private final Path local;

  ParquetLocalInputFile(Path root, String s3Uri) {
    this.path = normalizePath(s3Uri);
    this.local = toLocalPath(root, this.path);
  }

  @Override
  public long getLength() {
    try {
      return Files.size(local);
    } catch (IOException e) {
      throw new RuntimeException("Failed to read size of " + path, e);
    }
  }

  @Override
  public SeekableInputStream newStream() {
    try {
      SeekableByteChannel channel = Files.newByteChannel(local);
      return new LocalSeekableInputStream(channel);
    } catch (IOException e) {
      throw new RuntimeException("Failed to open " + path, e);
    }
  }

  private static String normalizePath(String s3Uri) {
    if (s3Uri.startsWith("s3a://")) {
      return "s3://" + s3Uri.substring(6);
    }
    return s3Uri;
  }

  private static Path toLocalPath(Path root, String s3Uri) {
    URI uri = URI.create(s3Uri);
    if (!"s3".equalsIgnoreCase(uri.getScheme())) {
      throw new IllegalArgumentException("Only s3:// URIs are supported: " + s3Uri);
    }
    String bucket = uri.getHost();
    if (bucket == null || bucket.isBlank()) {
      throw new IllegalArgumentException("Missing bucket in s3 URI: " + s3Uri);
    }
    String key = uri.getPath() == null ? "" : uri.getPath().replaceFirst("^/", "");
    if (key.isEmpty()) {
      return root.resolve(bucket);
    }
    return root.resolve(bucket).resolve(key);
  }

  private static final class LocalSeekableInputStream extends SeekableInputStream {
    private final SeekableByteChannel channel;

    private LocalSeekableInputStream(SeekableByteChannel channel) {
      this.channel = channel;
    }

    @Override
    public long getPos() throws IOException {
      return channel.position();
    }

    @Override
    public void seek(long newPos) throws IOException {
      if (newPos < 0) {
        throw new IOException("negative seek");
      }
      channel.position(newPos);
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
    public int read(ByteBuffer dst) throws IOException {
      if (!dst.hasRemaining()) {
        return 0;
      }
      if (dst.hasArray()) {
        int pos = dst.position();
        int n =
            channel.read(ByteBuffer.wrap(dst.array(), dst.arrayOffset() + pos, dst.remaining()));
        if (n > 0) {
          dst.position(pos + n);
        }
        return n;
      }
      byte[] tmp = new byte[Math.min(dst.remaining(), 8192)];
      int total = 0;
      while (dst.hasRemaining()) {
        int toRead = Math.min(tmp.length, dst.remaining());
        int n = read(tmp, 0, toRead);
        if (n <= 0) {
          return total == 0 ? -1 : total;
        }
        dst.put(tmp, 0, n);
        total += n;
        if (n < toRead) {
          break;
        }
      }
      return total;
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
      readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int off, int len) throws IOException {
      int done = 0;
      while (done < len) {
        int n = read(bytes, off + done, len - done);
        if (n < 0) {
          throw new IOException("EOF while reading fully");
        }
        done += n;
      }
    }

    @Override
    public void readFully(ByteBuffer dst) throws IOException {
      if (!dst.hasRemaining()) {
        return;
      }
      byte[] tmp = new byte[Math.min(dst.remaining(), 8192)];
      while (dst.hasRemaining()) {
        int toRead = Math.min(tmp.length, dst.remaining());
        readFully(tmp, 0, toRead);
        dst.put(tmp, 0, toRead);
      }
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }
  }
}
