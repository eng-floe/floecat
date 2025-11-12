package ai.floedb.metacat.connector.delta.uc.impl;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class ParquetS3V2InputFile implements InputFile {
  private final S3Client s3;
  private final String bucket, key;
  private final long len;

  ParquetS3V2InputFile(S3Client s3, String s3Uri) {
    this.s3 = s3;
    var u = URI.create(s3Uri.startsWith("s3a://") ? "s3://" + s3Uri.substring(6) : s3Uri);
    this.bucket = u.getHost();
    this.key = u.getPath().startsWith("/") ? u.getPath().substring(1) : u.getPath();
    this.len = s3.headObject(b -> b.bucket(bucket).key(key)).contentLength();
  }

  @Override
  public long getLength() {
    return len;
  }

  @Override
  public SeekableInputStream newStream() {
    return new SeekableInputStream() {
      long pos = 0;

      @Override
      public long getPos() {
        return pos;
      }

      @Override
      public void seek(long newPos) throws IOException {
        if (newPos < 0) {
          throw new IOException("negative seek");
        }
        this.pos = newPos;
      }

      @Override
      public int read() throws IOException {
        byte[] b = new byte[1];
        int read = read(b, 0, 1);
        if (read == -1) {
          return -1;
        }

        return b[0] & 0xFF;
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
          return 0;
        }

        long start = pos;
        long end = start + len - 1;
        try (var obj =
            s3.getObject(
                builder -> builder.bucket(bucket).key(key).range("bytes=" + start + "-" + end))) {
          int r = obj.read(b, off, len);
          if (r > 0) {
            pos += r;
          }
          return r;
        } catch (S3Exception e) {
          if (e.statusCode() == 416) return -1;
          throw new IOException("S3 read error at range bytes=" + start + "-" + end, e);
        }
      }

      @Override
      public int read(ByteBuffer buf) throws IOException {
        int len = buf.remaining();
        if (len == 0) {
          return 0;
        }

        long start = pos;
        long end = start + len - 1;

        try (var obj =
            s3.getObject(
                builder -> builder.bucket(bucket).key(key).range("bytes=" + start + "-" + end))) {
          byte[] b = new byte[len];
          int bytesRead = obj.read(b, 0, len);

          if (bytesRead > 0) {
            buf.put(b, 0, bytesRead);
            pos += bytesRead;
            return bytesRead;
          } else {
            return bytesRead;
          }
        } catch (S3Exception e) {
          if (e.statusCode() == 416) return -1;
          throw new IOException("S3 read error with ByteBuffer", e);
        }
      }

      @Override
      public void readFully(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
          int bytesRead = read(buf);
          if (bytesRead == -1) {
            throw new IOException(
                "Reached end of stream before reading fully required bytes into ByteBuffer.");
          }
        }
      }

      @Override
      public void readFully(byte[] bytes) throws IOException {
        readFully(bytes, 0, bytes.length);
      }

      @Override
      public void readFully(byte[] bytes, int off, int len) throws IOException {
        int bytesRead = 0;
        while (bytesRead < len) {
          int currentRead = read(bytes, off + bytesRead, len - bytesRead);
          if (currentRead == -1) {
            throw new IOException("Reached end of stream before reading fully required bytes.");
          }
          bytesRead += currentRead;
        }
      }
    };
  }
}
