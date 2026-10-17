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

import io.delta.kernel.defaults.engine.fileio.OutputFile;
import io.delta.kernel.defaults.engine.fileio.PositionOutputStream;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/** Writable local S3 facade used only to construct and mutate Delta fixtures. */
final class WritableLocalFileSystemClient extends LocalFileSystemClient {
  WritableLocalFileSystemClient(Path root) {
    super(root);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new LocalOutputFile(resolvePath(path), toLocalPath(path));
  }

  @Override
  public boolean delete(String path) {
    try {
      return Files.deleteIfExists(toLocalPath(path));
    } catch (IOException e) {
      throw new RuntimeException("Failed to delete " + path, e);
    }
  }

  @Override
  public void copyFileAtomically(String sourcePath, String destinationPath, boolean overwrite) {
    Path destination = toLocalPath(destinationPath);
    Path temporary = null;
    try {
      Files.createDirectories(destination.getParent());
      temporary =
          Files.createTempFile(
              destination.getParent(), destination.getFileName().toString(), ".tmp");
      Files.copy(toLocalPath(sourcePath), temporary, StandardCopyOption.REPLACE_EXISTING);
      Files.move(
          temporary,
          destination,
          overwrite
              ? new StandardCopyOption[] {
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING
              }
              : new StandardCopyOption[] {StandardCopyOption.ATOMIC_MOVE});
    } catch (IOException e) {
      throw new RuntimeException("Failed to copy " + sourcePath + " to " + destinationPath, e);
    } finally {
      if (temporary != null) {
        try {
          Files.deleteIfExists(temporary);
        } catch (IOException ignored) {
          // The original copy failure is more useful than cleanup failure.
        }
      }
    }
  }

  private record LocalOutputFile(String path, Path local) implements OutputFile {
    @Override
    public PositionOutputStream create(boolean overwrite) throws IOException {
      Files.createDirectories(local.getParent());
      OutputStream stream =
          overwrite
              ? Files.newOutputStream(
                  local,
                  StandardOpenOption.CREATE,
                  StandardOpenOption.TRUNCATE_EXISTING,
                  StandardOpenOption.WRITE)
              : Files.newOutputStream(
                  local, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
      return new LocalPositionOutputStream(stream);
    }

    @Override
    public void writeAtomically(CloseableIterator<String> lines, boolean overwrite)
        throws IOException {
      Files.createDirectories(local.getParent());
      Path temporary =
          Files.createTempFile(local.getParent(), local.getFileName().toString(), ".tmp");
      try {
        try (var writer = Files.newBufferedWriter(temporary, StandardCharsets.UTF_8)) {
          while (lines.hasNext()) {
            writer.write(lines.next());
            writer.newLine();
          }
        } finally {
          lines.close();
        }
        Files.move(
            temporary,
            local,
            overwrite
                ? new StandardCopyOption[] {
                  StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING
                }
                : new StandardCopyOption[] {StandardCopyOption.ATOMIC_MOVE});
      } finally {
        Files.deleteIfExists(temporary);
      }
    }
  }

  private static final class LocalPositionOutputStream extends PositionOutputStream {
    private final OutputStream delegate;
    private long position;

    private LocalPositionOutputStream(OutputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public void write(int value) throws IOException {
      delegate.write(value);
      position++;
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
      delegate.write(bytes, offset, length);
      position += length;
    }

    @Override
    public void flush() throws IOException {
      delegate.flush();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    @Override
    public long getPos() {
      return position;
    }
  }
}
