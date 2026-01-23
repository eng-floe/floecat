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

import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.defaults.engine.fileio.InputFile;
import io.delta.kernel.defaults.engine.fileio.OutputFile;
import io.delta.kernel.defaults.engine.fileio.SeekableInputStream;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

final class LocalFileSystemClient implements FileIO {
  private final Path root;

  LocalFileSystemClient(Path root) {
    this.root = root.toAbsolutePath();
  }

  @Override
  public InputFile newInputFile(String path, long fileSize) {
    String resolved = resolvePath(path);
    Path local = toLocalPath(path);
    if (isMissingCheckpoint(local, resolved)) {
      return new MissingInputFile(resolved);
    }
    return new LocalInputFile(resolved, local);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    throw new UnsupportedOperationException(
        "Writing files not implemented for read-only LocalFileIO");
  }

  @Override
  public boolean delete(String path) {
    throw new UnsupportedOperationException(
        "Deleting files not implemented for read-only LocalFileIO");
  }

  @Override
  public String resolvePath(String path) {
    if (path.startsWith("s3a://")) {
      return "s3://" + path.substring(6);
    }
    if (path.startsWith("s3://")) {
      return path;
    }
    throw new IllegalArgumentException("Unsupported file system path: " + path);
  }

  @Override
  public FileStatus getFileStatus(String path) throws IOException {
    Path local = toLocalPath(path);
    if (!Files.exists(local)) {
      throw new IOException("File not found: " + path);
    }
    long size = Files.size(local);
    long mod = Files.getLastModifiedTime(local).toMillis();
    return FileStatus.of(path, size, mod);
  }

  @Override
  public CloseableIterator<FileStatus> listFrom(String filePath) throws IOException {
    String resolved = resolvePath(filePath);
    S3Location location = parseS3Location(resolved);
    Path bucketRoot = root.resolve(location.bucket());
    Path local = toLocalPath(resolved);
    Path dir = local.getParent();
    if (dir == null || !Files.isDirectory(dir)) {
      throw new IOException("Invalid local path for listFrom: " + filePath);
    }

    String startName = local.getFileName() == null ? "" : local.getFileName().toString();
    boolean includeStart = Files.isRegularFile(local);

    List<Path> files;
    try (var stream = Files.list(dir)) {
      files =
          stream
              .filter(Files::isRegularFile)
              .sorted(Comparator.comparing(p -> p.getFileName().toString()))
              .toList();
    }

    List<FileStatus> statuses = new ArrayList<>();
    if (includeStart) {
      statuses.add(toStatus(bucketRoot, location.bucket(), local));
    }
    for (Path file : files) {
      String name = file.getFileName().toString();
      if (includeStart && name.equals(startName)) {
        continue;
      }
      if (name.compareTo(startName) <= 0) {
        continue;
      }
      statuses.add(toStatus(bucketRoot, location.bucket(), file));
    }

    Iterator<FileStatus> iter = statuses.iterator();
    return new CloseableIterator<>() {
      private boolean closed = false;

      @Override
      public boolean hasNext() {
        return !closed && iter.hasNext();
      }

      @Override
      public FileStatus next() {
        if (closed) {
          throw new NoSuchElementException("Iterator closed");
        }
        return iter.next();
      }

      @Override
      public void close() {
        closed = true;
      }
    };
  }

  @Override
  public boolean mkdirs(String path) throws IOException {
    Path local = toLocalPath(path);
    Files.createDirectories(local);
    return true;
  }

  @Override
  public Optional<String> getConf(String confKey) {
    return Optional.empty();
  }

  private FileStatus toStatus(Path bucketRoot, String bucket, Path local) throws IOException {
    String key = bucketRoot.relativize(local).toString().replace(File.separatorChar, '/');
    String s3Path = "s3://" + bucket + "/" + key;
    long size = Files.size(local);
    long mod = Files.getLastModifiedTime(local).toInstant().toEpochMilli();
    if (mod <= 0) {
      mod = Instant.now().toEpochMilli();
    }
    return FileStatus.of(s3Path, size, mod);
  }

  private Path toLocalPath(String path) {
    String resolved = resolvePath(path);
    S3Location loc = parseS3Location(resolved);
    Path bucketRoot = root.resolve(loc.bucket());
    if (loc.key().isEmpty()) {
      return bucketRoot;
    }
    return bucketRoot.resolve(loc.key());
  }

  private static S3Location parseS3Location(String uri) {
    URI u = URI.create(uri);
    if (!"s3".equalsIgnoreCase(u.getScheme())) {
      throw new IllegalArgumentException("Unsupported path scheme: " + uri);
    }
    String bucket = u.getHost();
    if (bucket == null || bucket.isBlank()) {
      throw new IllegalArgumentException("Missing bucket in " + uri);
    }
    String key = u.getPath() == null ? "" : u.getPath().replaceFirst("^/", "");
    return new S3Location(bucket, key);
  }

  private static boolean isMissingCheckpoint(Path local, String resolvedPath) {
    return resolvedPath.endsWith("/_last_checkpoint") && !Files.exists(local);
  }

  private record S3Location(String bucket, String key) {}

  private static final class LocalInputFile implements InputFile {
    private final String path;
    private final Path local;

    private LocalInputFile(String path, Path local) {
      this.path = path;
      this.local = local;
    }

    @Override
    public SeekableInputStream newStream() {
      try {
        var channel = Files.newByteChannel(local);
        return new LocalSeekableInputStream(channel);
      } catch (IOException e) {
        throw new RuntimeException("Failed to open " + path, e);
      }
    }

    @Override
    public long length() {
      try {
        return Files.size(local);
      } catch (IOException e) {
        throw new RuntimeException("Failed to get length for " + path, e);
      }
    }

    @Override
    public String path() {
      return path;
    }
  }

  private static final class MissingInputFile implements InputFile {
    private final String path;

    private MissingInputFile(String path) {
      this.path = path;
    }

    @Override
    public SeekableInputStream newStream() {
      return new MissingSeekableInputStream(path);
    }

    @Override
    public long length() {
      return 0L;
    }

    @Override
    public String path() {
      return path;
    }
  }

  private static final class MissingSeekableInputStream extends SeekableInputStream {
    private final String path;

    private MissingSeekableInputStream(String path) {
      this.path = path;
    }

    @Override
    public int read() throws IOException {
      throw new FileNotFoundException("File not found: " + path);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      throw new FileNotFoundException("File not found: " + path);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
      throw new FileNotFoundException("File not found: " + path);
    }

    @Override
    public void seek(long newPos) throws IOException {
      throw new FileNotFoundException("File not found: " + path);
    }

    @Override
    public long getPos() throws IOException {
      return 0L;
    }

    @Override
    public void close() {}
  }

  private static final class LocalSeekableInputStream extends SeekableInputStream {
    private final SeekableByteChannel channel;

    private LocalSeekableInputStream(SeekableByteChannel channel) {
      this.channel = channel;
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
      var buffer = ByteBuffer.wrap(b, off, len);
      int read = channel.read(buffer);
      return read < 0 ? -1 : read;
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
      int done = 0;
      while (done < len) {
        int n = read(b, off + done, len - done);
        if (n == -1) {
          throw new IOException("Reached EOF while reading fully");
        }
        done += n;
      }
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
    public void close() throws IOException {
      channel.close();
    }
  }
}
