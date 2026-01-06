/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.connector.delta.uc.impl;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import software.amazon.awssdk.services.s3.S3Client;

public class ParquetS3V2InputFile implements InputFile {
  private final S3RangeReader reader;
  private final String bucket;
  private final String key;

  ParquetS3V2InputFile(S3Client s3, String s3Uri) {
    var u = URI.create(s3Uri.startsWith("s3a://") ? "s3://" + s3Uri.substring(6) : s3Uri);
    this.bucket = u.getHost();
    this.key = u.getPath().startsWith("/") ? u.getPath().substring(1) : u.getPath();
    try {
      this.reader = new S3RangeReader(s3, bucket, key);
    } catch (IOException e) {
      throw new RuntimeException("Failed to initialize S3RangeReader for " + s3Uri, e);
    }
  }

  @Override
  public long getLength() {
    return reader.length();
  }

  @Override
  public SeekableInputStream newStream() {
    return new ParquetS3SeekableInputStream(reader);
  }

  private static final class ParquetS3SeekableInputStream extends SeekableInputStream {
    private final S3RangeReader reader;
    private long pos = 0;
    private boolean closed = false;

    ParquetS3SeekableInputStream(S3RangeReader reader) {
      this.reader = reader;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long newPos) throws IOException {
      if (newPos < 0) throw new IOException("negative seek");
      this.pos = newPos;
    }

    @Override
    public int read() throws IOException {
      byte[] b = new byte[1];
      int n = read(b, 0, 1);
      if (n == -1) {
        return -1;
      }

      return b[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (closed) throw new IOException("stream closed");
      if (len == 0) {
        return 0;
      }

      int n = reader.readAt(pos, b, off, len);
      if (n > 0) {
        pos += n;
      }
      return n;
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
      if (closed) {
        throw new IOException("stream closed");
      }

      int len = buf.remaining();
      if (len == 0) {
        return 0;
      }

      byte[] tmp = new byte[Math.min(len, 64 * 1024)];
      int total = 0;
      while (buf.hasRemaining()) {
        int toRead = Math.min(tmp.length, buf.remaining());
        int n = read(tmp, 0, toRead);
        if (n == -1) {
          return (total == 0) ? -1 : total;
        }
        buf.put(tmp, 0, n);
        total += n;
      }
      return total;
    }

    @Override
    public void readFully(ByteBuffer buf) throws IOException {
      while (buf.hasRemaining()) {
        int n = read(buf);
        if (n == -1) {
          throw new IOException(
              "Reached EOF before reading fully into ByteBuffer; remaining=" + buf.remaining());
        }
      }
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
        if (n == -1) {
          throw new IOException("Reached EOF before reading fully; remaining=" + (len - done));
        }
        done += n;
      }
    }

    @Override
    public void close() {
      closed = true;
      reader.close();
    }
  }
}
