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

package ai.floedb.floecat.connector.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

public final class IcebergParquetInputFileAdapter implements InputFile {
  private final org.apache.iceberg.io.InputFile delegate;

  public IcebergParquetInputFileAdapter(org.apache.iceberg.io.InputFile delegate) {
    this.delegate = delegate;
  }

  @Override
  public long getLength() throws IOException {
    return delegate.getLength();
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    return new IcebergSeekableInputStreamAdapter(delegate.newStream());
  }

  private static final class IcebergSeekableInputStreamAdapter extends SeekableInputStream {
    private final org.apache.iceberg.io.SeekableInputStream delegate;

    private IcebergSeekableInputStreamAdapter(org.apache.iceberg.io.SeekableInputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      delegate.seek(newPos);
    }

    @Override
    public int read() throws IOException {
      return delegate.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return delegate.read(b, off, len);
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
      readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
      int offset = start;
      int remaining = len;
      while (remaining > 0) {
        int read = delegate.read(bytes, offset, remaining);
        if (read < 0) {
          throw new java.io.EOFException("Unexpected EOF while reading Iceberg parquet input");
        }
        offset += read;
        remaining -= read;
      }
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
      if (!buf.hasRemaining()) {
        return 0;
      }
      byte[] scratch = new byte[Math.min(buf.remaining(), 8192)];
      int read = delegate.read(scratch, 0, Math.min(scratch.length, buf.remaining()));
      if (read > 0) {
        buf.put(scratch, 0, read);
      }
      return read;
    }

    @Override
    public void readFully(ByteBuffer buf) throws IOException {
      while (buf.hasRemaining()) {
        int read = read(buf);
        if (read < 0) {
          throw new java.io.EOFException("Unexpected EOF while reading Iceberg parquet input");
        }
      }
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}
