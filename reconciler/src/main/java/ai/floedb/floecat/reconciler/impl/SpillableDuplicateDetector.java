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

package ai.floedb.floecat.reconciler.impl;

import ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexEntry;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Consumer;

/** Exact duplicate detection using bounded heap and sequential operation-scoped local spill. */
final class SpillableDuplicateDetector implements AutoCloseable {
  private static final long MAX_BUFFER_BYTES = 8L * 1024L * 1024L;
  private static final int MERGE_FAN_IN = 64;
  private static final long DEFAULT_MAX_DISK_BYTES = 8L * 1024L * 1024L * 1024L;

  private final long maxBufferBytes;
  private final int mergeFanIn;
  private final long maxDiskBytes;
  private final List<String> buffered = new ArrayList<>();
  private final Set<Path> files = new HashSet<>();
  private List<Path> runs = new ArrayList<>();
  private Path directory;
  private Path replayPath;
  private DataOutputStream replayStream;
  private long bufferedBytes;
  private long liveDiskBytes;
  private boolean verified;

  SpillableDuplicateDetector() {
    this(MAX_BUFFER_BYTES, MERGE_FAN_IN, configuredMaxDiskBytes());
  }

  SpillableDuplicateDetector(long maxBufferBytes, int mergeFanIn, long maxDiskBytes) {
    if (maxBufferBytes <= 0L || mergeFanIn < 2 || maxDiskBytes <= 0L) {
      throw new IllegalArgumentException("duplicate detector limits must be positive");
    }
    this.maxBufferBytes = maxBufferBytes;
    this.mergeFanIn = mergeFanIn;
    this.maxDiskBytes = maxDiskBytes;
  }

  void add(String value) {
    if (verified) {
      throw new IllegalStateException("duplicate detector is already verified");
    }
    if (value == null) {
      throw new IllegalArgumentException("duplicate detector value is required");
    }
    buffered.add(value);
    bufferedBytes += 64L + 2L * value.length();
    if (bufferedBytes >= maxBufferBytes) {
      flush();
    }
  }

  void add(String value, ReusableArtifactIndexEntry entry) {
    if (entry == null) {
      throw new IllegalArgumentException("reusable artifact index entry is required");
    }
    byte[] bytes = entry.toByteArray();
    add(value);
    long recordBytes = Math.addExact(Integer.BYTES, bytes.length);
    DataOutputStream stream;
    try {
      stream = ensureReplayStream();
    } catch (IOException error) {
      throw new IllegalStateException("cannot create reusable artifact index spill", error);
    }
    reserveDisk(recordBytes);
    try {
      stream.writeInt(bytes.length);
      stream.write(bytes);
      liveDiskBytes = Math.addExact(liveDiskBytes, recordBytes);
    } catch (IOException error) {
      throw new IllegalStateException("cannot spill reusable artifact index entries", error);
    }
  }

  void verifyNoDuplicates() {
    if (verified) {
      return;
    }
    closeReplayStream();
    flush();
    while (runs.size() > mergeFanIn) {
      List<Path> merged = new ArrayList<>();
      for (int from = 0; from < runs.size(); from += mergeFanIn) {
        List<Path> inputs =
            List.copyOf(runs.subList(from, Math.min(from + mergeFanIn, runs.size())));
        Path output = newRunPath();
        merge(inputs, output);
        delete(inputs);
        merged.add(output);
      }
      runs = merged;
    }
    if (runs.size() > 1) {
      merge(runs, null);
    }
    verified = true;
  }

  void forEachEntry(Consumer<ReusableArtifactIndexEntry> consumer) {
    if (!verified) {
      throw new IllegalStateException("duplicate detector has not been verified");
    }
    if (consumer == null) {
      throw new IllegalArgumentException("reusable artifact index consumer is required");
    }
    if (replayPath == null) {
      return;
    }
    try (DataInputStream stream =
        new DataInputStream(new BufferedInputStream(Files.newInputStream(replayPath)))) {
      while (true) {
        int length;
        try {
          length = stream.readInt();
        } catch (EOFException ignored) {
          return;
        }
        if (length < 0) {
          throw new IOException("negative reusable artifact index entry length");
        }
        byte[] bytes = stream.readNBytes(length);
        if (bytes.length != length) {
          throw new IOException("truncated reusable artifact index entry");
        }
        consumer.accept(ReusableArtifactIndexEntry.parseFrom(bytes));
      }
    } catch (IOException error) {
      throw new IllegalStateException("cannot replay reusable artifact index entries", error);
    }
  }

  @Override
  public void close() {
    closeReplayStreamQuietly();
    delete(List.copyOf(files));
    if (directory != null) {
      try {
        Files.deleteIfExists(directory);
      } catch (IOException ignored) {
        directory.toFile().deleteOnExit();
      }
    }
    buffered.clear();
    runs.clear();
  }

  private void flush() {
    if (buffered.isEmpty()) {
      return;
    }
    buffered.sort(String::compareTo);
    requireNoAdjacentDuplicate(buffered);
    long outputBytes =
        buffered.stream()
            .mapToLong(value -> Integer.BYTES + value.getBytes(StandardCharsets.UTF_8).length)
            .reduce(0L, Math::addExact);
    Path output = newRunPath();
    reserveDisk(outputBytes);
    try (DataOutputStream stream =
        new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(output)))) {
      for (String value : buffered) {
        write(stream, value);
      }
    } catch (IOException error) {
      throw new IllegalStateException("cannot spill reusable artifact duplicate keys", error);
    }
    liveDiskBytes += fileSize(output);
    runs.add(output);
    buffered.clear();
    bufferedBytes = 0L;
  }

  private void merge(List<Path> inputs, Path output) {
    List<RunReader> readers = new ArrayList<>();
    DataOutputStream stream = null;
    try {
      if (output != null) {
        long outputBytes = inputs.stream().mapToLong(SpillableDuplicateDetector::fileSize).sum();
        reserveDisk(outputBytes);
        stream = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(output)));
      }
      PriorityQueue<RunReader> pending =
          new PriorityQueue<>(Comparator.comparing(RunReader::current));
      for (Path input : inputs) {
        RunReader reader = new RunReader(input);
        readers.add(reader);
        if (reader.current() != null) {
          pending.add(reader);
        }
      }
      String previous = null;
      while (!pending.isEmpty()) {
        RunReader reader = pending.remove();
        String current = reader.current();
        if (current.equals(previous)) {
          throw new IllegalArgumentException("reusable artifact index contains a duplicate target");
        }
        previous = current;
        if (stream != null) {
          write(stream, current);
        }
        reader.advance();
        if (reader.current() != null) {
          pending.add(reader);
        }
      }
    } catch (IOException error) {
      throw new IllegalStateException("cannot merge reusable artifact duplicate keys", error);
    } finally {
      for (RunReader reader : readers) {
        reader.close();
      }
      if (stream != null) {
        try {
          stream.close();
        } catch (IOException error) {
          throw new IllegalStateException("cannot close reusable artifact duplicate spill", error);
        }
      }
    }
    if (output != null) {
      liveDiskBytes += fileSize(output);
    }
  }

  private Path newRunPath() {
    try {
      if (directory == null) {
        directory = Files.createTempDirectory("floecat-reusable-artifact-duplicates-");
      }
      Path path = Files.createTempFile(directory, "keys-", ".bin");
      files.add(path);
      return path;
    } catch (IOException error) {
      throw new IllegalStateException("cannot create reusable artifact duplicate spill", error);
    }
  }

  private DataOutputStream ensureReplayStream() throws IOException {
    if (replayStream == null) {
      if (directory == null) {
        directory = Files.createTempDirectory("floecat-reusable-artifact-duplicates-");
      }
      replayPath = Files.createTempFile(directory, "entries-", ".bin");
      files.add(replayPath);
      replayStream =
          new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(replayPath)));
    }
    return replayStream;
  }

  private void closeReplayStream() {
    if (replayStream == null) {
      return;
    }
    try {
      replayStream.close();
      replayStream = null;
    } catch (IOException error) {
      throw new IllegalStateException("cannot close reusable artifact index spill", error);
    }
  }

  private void closeReplayStreamQuietly() {
    if (replayStream == null) {
      return;
    }
    try {
      replayStream.close();
    } catch (IOException ignored) {
      // The operation-scoped file is deleted below.
    } finally {
      replayStream = null;
    }
  }

  private void delete(List<Path> paths) {
    for (Path path : paths) {
      try {
        liveDiskBytes = Math.max(0L, liveDiskBytes - Files.size(path));
        Files.deleteIfExists(path);
        files.remove(path);
      } catch (IOException ignored) {
        path.toFile().deleteOnExit();
      }
    }
  }

  private void reserveDisk(long additionalBytes) {
    if (Math.addExact(liveDiskBytes, additionalBytes) > maxDiskBytes) {
      throw new IllegalStateException(
          "reusable artifact duplicate spill exceeded its configured local-disk budget");
    }
    try {
      if (directory != null && additionalBytes > Files.getFileStore(directory).getUsableSpace()) {
        throw new IllegalStateException(
            "insufficient local disk for reusable artifact duplicate detection");
      }
    } catch (IOException error) {
      throw new IllegalStateException(
          "cannot inspect reusable artifact duplicate spill disk", error);
    }
  }

  private static long fileSize(Path path) {
    try {
      return Files.size(path);
    } catch (IOException error) {
      throw new IllegalStateException("cannot size reusable artifact duplicate spill", error);
    }
  }

  private static long configuredMaxDiskBytes() {
    String configured = System.getProperty("floecat.reusable-artifact-duplicates.max-disk-bytes");
    if (configured == null || configured.isBlank()) {
      configured = System.getenv("FLOECAT_REUSABLE_ARTIFACT_DUPLICATES_MAX_DISK_BYTES");
    }
    if (configured == null || configured.isBlank()) {
      return DEFAULT_MAX_DISK_BYTES;
    }
    try {
      long parsed = Long.parseLong(configured);
      return parsed > 0L ? parsed : DEFAULT_MAX_DISK_BYTES;
    } catch (NumberFormatException ignored) {
      return DEFAULT_MAX_DISK_BYTES;
    }
  }

  private static void requireNoAdjacentDuplicate(List<String> values) {
    for (int index = 1; index < values.size(); index++) {
      if (values.get(index - 1).equals(values.get(index))) {
        throw new IllegalArgumentException("reusable artifact index contains a duplicate target");
      }
    }
  }

  private static void write(DataOutputStream stream, String value) throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    stream.writeInt(bytes.length);
    stream.write(bytes);
  }

  private static final class RunReader implements AutoCloseable {
    private final DataInputStream stream;
    private String current;

    private RunReader(Path path) throws IOException {
      stream = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
      advance();
    }

    private String current() {
      return current;
    }

    private void advance() throws IOException {
      try {
        int length = stream.readInt();
        if (length < 0) {
          throw new IOException("negative reusable artifact duplicate key length");
        }
        byte[] bytes = stream.readNBytes(length);
        if (bytes.length != length) {
          throw new IOException("truncated reusable artifact duplicate key");
        }
        current = new String(bytes, StandardCharsets.UTF_8);
      } catch (EOFException ignored) {
        current = null;
      }
    }

    @Override
    public void close() {
      try {
        stream.close();
      } catch (IOException ignored) {
        // The operation-scoped file is deleted by the owner.
      }
    }
  }
}
