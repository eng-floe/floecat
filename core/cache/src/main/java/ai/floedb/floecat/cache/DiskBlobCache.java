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

package ai.floedb.floecat.cache;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Content-addressed {@link BlobCache} backed only by local disk.
 *
 * <p>The cache has no resident entry index: a key hashes directly to its path. Files carry a small
 * checksummed envelope so a partial write or local corruption is discarded before the source is
 * consulted. Fills are staged beside their destination and atomically renamed. Runtime disk faults
 * fail open; the source loader remains the authority.
 *
 * <p>Large hits use Java's scoped file mapping API. A live mapping is ref-counted by path, and both
 * sweeping and partition eviction skip it. Closing {@link Content} deterministically unmaps it.
 */
public final class DiskBlobCache implements BlobCache, AutoCloseable {

  private static final int MAGIC = 0xF10ECB10;
  private static final int VERSION = 1;
  private static final int CHECKSUM_BYTES = 32;
  private static final int HEADER_BYTES =
      Integer.BYTES + Integer.BYTES + Long.BYTES + CHECKSUM_BYTES;
  private static final int SWEEP_CANDIDATES = 4096;
  private static final String ENTRY_SUFFIX = ".blob";
  private static final String STAGING_MARKER = ".staging-";
  private static final long ABANDONED_STAGING_MILLIS = Duration.ofMinutes(10).toMillis();

  private final Path root;
  private final long maxBytes;
  private final int mmapThresholdBytes;
  private final long accessUpdateIntervalMillis;
  private final BlobCacheEvents events;
  private final ConcurrentMap<String, PartitionState> partitions = new ConcurrentHashMap<>();
  private final ConcurrentMap<Path, AtomicLong> mappings = new ConcurrentHashMap<>();
  private final Set<Path> pendingDeletes = ConcurrentHashMap.newKeySet();
  private final AtomicLong knownBytes = new AtomicLong();
  private final AtomicLong knownEntries = new AtomicLong();
  private final AtomicLong liveMappings = new AtomicLong();
  private final AtomicBoolean closed = new AtomicBoolean();

  public DiskBlobCache(
      Path root,
      long maxBytes,
      int mmapThresholdBytes,
      Duration accessUpdateInterval,
      BlobCacheEvents events) {
    if (maxBytes <= 0L) {
      throw new IllegalArgumentException("blob-cache maxBytes must be positive");
    }
    if (mmapThresholdBytes < 1) {
      throw new IllegalArgumentException("blob-cache mmap threshold must be positive");
    }
    if (accessUpdateInterval == null || accessUpdateInterval.isNegative()) {
      throw new IllegalArgumentException("blob-cache access update interval must not be negative");
    }
    this.root = root.toAbsolutePath().normalize();
    this.maxBytes = maxBytes;
    this.mmapThresholdBytes = mmapThresholdBytes;
    this.accessUpdateIntervalMillis = accessUpdateInterval.toMillis();
    this.events = events == null ? BlobCacheEvents.none() : events;
    try {
      Files.createDirectories(this.root);
    } catch (IOException e) {
      throw new IllegalArgumentException("cannot create blob-cache root " + this.root, e);
    }
  }

  @Override
  public Optional<Content> get(Key key, Fill fill, Loader loader) {
    requireOpen();
    if (key == null || fill == null || loader == null) {
      throw new NullPointerException("blob-cache get arguments must not be null");
    }
    long started = System.nanoTime();
    Path path = entryPath(key);
    PartitionState partition = partition(key.partition());
    boolean partitionRetired;
    Optional<Content> cached;
    partition.lock.readLock().lock();
    try {
      partitionRetired = partition.retired;
      cached = partitionRetired ? Optional.empty() : readCached(path);
    } finally {
      partition.lock.readLock().unlock();
    }
    if (cached.isPresent()) {
      events.hit(Duration.ofNanos(System.nanoTime() - started));
      return cached;
    }
    if (partitionRetired) {
      return loadDirect(loader, started);
    }
    if (fill == Fill.BYPASS_FILL) {
      return loadDirect(loader, started);
    }

    byte[] bytes;
    try {
      bytes = loader.load();
      events.miss();
      events.loadTime(Duration.ofNanos(System.nanoTime() - started));
      if (bytes != null) {
        partition.lock.writeLock().lock();
        try {
          if (!partition.retired) {
            publish(path, bytes, false);
          }
        } finally {
          partition.lock.writeLock().unlock();
        }
      }
      return Optional.ofNullable(bytes).map(HeapContent::new);
    } catch (RuntimeException e) {
      events.miss();
      events.loadFailed(Duration.ofNanos(System.nanoTime() - started), e);
      throw e;
    }
  }

  @Override
  public Optional<Content> getRange(Key key, long offset, int length, Fill fill, Loader loader) {
    requireOpen();
    if (key == null || fill == null || loader == null) {
      throw new NullPointerException("blob-cache range arguments must not be null");
    }
    if (offset < 0L || length < 0) {
      throw new IllegalArgumentException("blob-cache range is invalid");
    }

    long started = System.nanoTime();
    PartitionState partition = partition(key.partition());
    Optional<Content> whole;
    partition.lock.readLock().lock();
    try {
      whole = partition.retired ? Optional.empty() : readCached(entryPath(key));
    } finally {
      partition.lock.readLock().unlock();
    }
    if (whole.isPresent()) {
      try {
        Content slice = new SliceContent(whole.orElseThrow(), offset, length);
        events.hit(Duration.ofNanos(System.nanoTime() - started));
        return Optional.of(slice);
      } catch (RuntimeException | Error failure) {
        whole.orElseThrow().close();
        throw failure;
      }
    }

    return get(
        rangeKey(key, offset, length),
        fill,
        () -> {
          byte[] loaded = loader.load();
          if (loaded != null && loaded.length != length) {
            throw new IllegalArgumentException("blob-cache range loader returned the wrong length");
          }
          return loaded;
        });
  }

  @Override
  public Map<Key, Content> getAll(List<Key> keys, Fill fill, BatchLoader loader) {
    requireOpen();
    if (keys == null || fill == null || loader == null) {
      throw new NullPointerException("blob-cache getAll arguments must not be null");
    }
    Map<String, List<Key>> byPartition = new LinkedHashMap<>();
    for (Key key : new LinkedHashSet<>(keys)) {
      if (key == null) {
        throw new NullPointerException("blob-cache batch key must not be null");
      }
      byPartition.computeIfAbsent(key.partition(), ignored -> new ArrayList<>()).add(key);
    }
    Map<Key, Content> result = new LinkedHashMap<>();
    try {
      for (List<Key> partitionKeys : byPartition.values()) {
        result.putAll(getAllFromPartition(partitionKeys, fill, loader));
      }
      return result;
    } catch (RuntimeException | Error failure) {
      result.values().forEach(Content::close);
      throw failure;
    }
  }

  private Map<Key, Content> getAllFromPartition(List<Key> keys, Fill fill, BatchLoader loader) {
    long started = System.nanoTime();
    PartitionState partition = partition(keys.getFirst().partition());
    Map<Key, Content> result = new LinkedHashMap<>();
    List<Key> misses = new ArrayList<>();
    int hits = 0;
    partition.lock.readLock().lock();
    try {
      for (Key key : keys) {
        Optional<Content> cached =
            partition.retired ? Optional.empty() : readCached(entryPath(key));
        if (cached.isPresent()) {
          result.put(key, cached.orElseThrow());
          hits++;
          continue;
        }
        misses.add(key);
      }
    } finally {
      partition.lock.readLock().unlock();
    }
    try {
      for (int i = 0; i < hits; i++) {
        events.hit(Duration.ofNanos(System.nanoTime() - started));
      }

      if (misses.isEmpty()) {
        return result;
      }
      Map<Key, byte[]> fetched = loadBatch(loader, misses, started);
      for (Key key : misses) {
        byte[] bytes = fetched.get(key);
        if (bytes != null) {
          if (fill == Fill.FILL) {
            partition.lock.writeLock().lock();
            try {
              if (!partition.retired) {
                publish(entryPath(key), bytes, false);
              }
            } finally {
              partition.lock.writeLock().unlock();
            }
          }
          result.put(key, new HeapContent(bytes));
        }
      }
      return result;
    } catch (RuntimeException | Error failure) {
      result.values().forEach(Content::close);
      throw failure;
    }
  }

  private Map<Key, byte[]> loadBatch(BatchLoader loader, List<Key> keys, long started) {
    try {
      Map<Key, byte[]> fetched = loader.load(keys);
      keys.forEach(ignored -> events.miss());
      events.loadTime(Duration.ofNanos(System.nanoTime() - started));
      return fetched == null ? Map.of() : fetched;
    } catch (RuntimeException failure) {
      keys.forEach(ignored -> events.miss());
      events.loadFailed(Duration.ofNanos(System.nanoTime() - started), failure);
      throw failure;
    } catch (Error failure) {
      throw failure;
    }
  }

  private Optional<Content> loadDirect(Loader loader, long started) {
    try {
      byte[] bytes = loader.load();
      events.miss();
      events.loadTime(Duration.ofNanos(System.nanoTime() - started));
      return Optional.ofNullable(bytes).map(HeapContent::new);
    } catch (RuntimeException e) {
      events.miss();
      events.loadFailed(Duration.ofNanos(System.nanoTime() - started), e);
      throw e;
    }
  }

  @Override
  public void put(Key key, byte[] bytes) {
    requireOpen();
    if (key == null || bytes == null) {
      throw new NullPointerException("blob-cache put arguments must not be null");
    }
    PartitionState partition = partition(key.partition());
    partition.lock.writeLock().lock();
    try {
      if (partition.retired) {
        events.admissionRejected();
        return;
      }
      publish(entryPath(key), bytes, true);
    } finally {
      partition.lock.writeLock().unlock();
    }
  }

  @Override
  public void evict(Key key) {
    if (key == null || closed.get()) {
      return;
    }
    PartitionState partition = partition(key.partition());
    partition.lock.writeLock().lock();
    try {
      retire(entryPath(key));
    } finally {
      partition.lock.writeLock().unlock();
    }
  }

  @Override
  public void evictPartition(String partition) {
    if (partition == null || partition.isBlank() || closed.get()) {
      return;
    }
    Path directory = partitionPath(partition);
    if (!directory.startsWith(root)) {
      throw new IllegalArgumentException("blob-cache partition escaped its root");
    }
    PartitionState state = partition(partition);
    state.lock.writeLock().lock();
    try {
      // Account ids are immutable identities. Once their durable account is deleted, keep this
      // local partition non-admitting for the rest of the process so a late writer cannot
      // repopulate it after account deletion and disk eviction completed.
      state.retired = true;
      try {
        if (!Files.exists(directory)) {
          return;
        }
        try (var paths = Files.walk(directory)) {
          paths.filter(Files::isRegularFile).forEach(this::retire);
        }
        deleteEmptyTree(directory);
      } catch (IOException ignored) {
        // Best effort: account deletion remains authoritative in the durable stores.
      }
    } finally {
      state.lock.writeLock().unlock();
    }
  }

  @Override
  public synchronized SweepResult sweep() {
    if (closed.get()) {
      return new SweepResult(knownBytes.get(), 0L, 0L);
    }
    long reclaimedBytes = 0L;
    long reclaimedEntries = 0L;
    Scan scan;
    do {
      scan = scanOldest();
      long over = Math.max(0L, scan.bytes() - maxBytes);
      if (over == 0L) {
        break;
      }
      long before = reclaimedBytes;
      List<Entry> oldest = new ArrayList<>(scan.oldest());
      oldest.sort(Comparator.comparingLong(Entry::modifiedMillis));
      for (Entry entry : oldest) {
        if (reclaimedBytes - before >= over) {
          break;
        }
        if (retire(entry.path())) {
          reclaimedBytes += entry.bytes();
          reclaimedEntries++;
        }
      }
      if (reclaimedBytes == before) {
        break;
      }
    } while (scan.oldest().size() == SWEEP_CANDIDATES);

    Scan finalScan = scanOldest();
    knownBytes.set(finalScan.bytes());
    knownEntries.set(finalScan.entries());
    SweepResult result =
        new SweepResult(finalScan.bytes() + reclaimedBytes, reclaimedBytes, reclaimedEntries);
    events.swept(result);
    return result;
  }

  private Scan scanOldest() {
    PriorityQueue<Entry> oldest =
        new PriorityQueue<>(Comparator.comparingLong(Entry::modifiedMillis).reversed());
    long bytes = 0L;
    long entries = 0L;
    try {
      if (!Files.exists(root)) {
        return new Scan(0L, 0L, List.of());
      }
      try (var paths = Files.walk(root)) {
        var iterator = paths.filter(Files::isRegularFile).iterator();
        while (iterator.hasNext()) {
          Path path = iterator.next();
          String name = path.getFileName().toString();
          if (name.contains(STAGING_MARKER)) {
            try {
              if (System.currentTimeMillis() - Files.getLastModifiedTime(path).toMillis()
                  >= ABANDONED_STAGING_MILLIS) {
                deleteIfIdle(path);
              }
            } catch (IOException ignored) {
              // The next sweep retries an entry racing a fill or removal.
            }
            continue;
          }
          if (!name.endsWith(ENTRY_SUFFIX)) {
            continue;
          }
          try {
            long size = Files.size(path);
            long modified = Files.getLastModifiedTime(path).toMillis();
            bytes = saturatingAdd(bytes, size);
            entries++;
            Entry candidate = new Entry(path, size, modified);
            if (oldest.size() < SWEEP_CANDIDATES) {
              oldest.add(candidate);
            } else if (modified < oldest.element().modifiedMillis()) {
              oldest.remove();
              oldest.add(candidate);
            }
          } catch (IOException ignored) {
            // A concurrent eviction or local I/O fault is retried by the next sweep.
          }
        }
      }
    } catch (IOException ignored) {
      // Metrics retain the last complete scan; the cache remains fail-open.
      return new Scan(knownBytes.get(), knownEntries.get(), List.of());
    }
    return new Scan(bytes, entries, List.copyOf(oldest));
  }

  private Optional<Content> readCached(Path path) {
    retainPath(path);
    boolean mapped = false;
    try {
      if (pendingDeletes.contains(path)) {
        return Optional.empty();
      }
      FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
      try {
        long fileBytes = channel.size();
        Header header = readHeader(channel, fileBytes);
        Content content;
        if (header.payloadBytes() >= mmapThresholdBytes) {
          content = mapped(path, channel, header);
          channel = null;
          liveMappings.incrementAndGet();
          mapped = true;
        } else {
          content = heap(channel, header);
        }
        touch(path);
        return Optional.of(content);
      } finally {
        if (channel != null) {
          channel.close();
        }
      }
    } catch (NoSuchFileException e) {
      return Optional.empty();
    } catch (IOException | RuntimeException e) {
      discardCorrupt(path);
      return Optional.empty();
    } finally {
      if (!mapped) {
        releasePath(path, false);
      }
    }
  }

  private Header readHeader(FileChannel channel, long fileBytes) throws IOException {
    if (fileBytes < HEADER_BYTES) {
      throw new IOException("truncated blob-cache entry");
    }
    ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
    readFully(channel, header, 0L);
    header.flip();
    if (header.getInt() != MAGIC || header.getInt() != VERSION) {
      throw new IOException("unknown blob-cache envelope");
    }
    long payloadBytes = header.getLong();
    if (payloadBytes < 0L
        || payloadBytes > Integer.MAX_VALUE
        || payloadBytes != fileBytes - HEADER_BYTES) {
      throw new IOException("invalid blob-cache payload length");
    }
    byte[] checksum = new byte[CHECKSUM_BYTES];
    header.get(checksum);
    return new Header((int) payloadBytes, checksum);
  }

  private Content heap(FileChannel channel, Header header) throws IOException {
    ByteBuffer bytes = ByteBuffer.allocate(header.payloadBytes());
    readFully(channel, bytes, HEADER_BYTES);
    bytes.flip();
    verify(bytes.duplicate(), header.checksum());
    return new HeapContent(bytes.asReadOnlyBuffer());
  }

  private Content mapped(Path path, FileChannel channel, Header header) throws IOException {
    Arena arena = Arena.ofShared();
    try {
      MemorySegment segment =
          channel.map(FileChannel.MapMode.READ_ONLY, HEADER_BYTES, header.payloadBytes(), arena);
      ByteBuffer bytes = segment.asByteBuffer().asReadOnlyBuffer();
      verify(bytes.duplicate(), header.checksum());
      channel.close();
      return new MappedContent(path, bytes, arena);
    } catch (Throwable t) {
      arena.close();
      throw t;
    }
  }

  private synchronized void publish(Path path, byte[] bytes, boolean replace) {
    if ((long) HEADER_BYTES + bytes.length > maxBytes) {
      events.admissionRejected();
      return;
    }
    Path staging = null;
    try {
      Files.createDirectories(path.getParent());
      if (pendingDeletes.contains(path)) {
        events.admissionRejected();
        return;
      }
      boolean existed = Files.isRegularFile(path);
      if (existed && !replace) {
        return;
      }
      AtomicLong refs = mappings.get(path);
      if (existed && refs != null && refs.get() > 0L) {
        pendingDeletes.add(path);
        events.admissionRejected();
        return;
      }
      long oldBytes = existed ? Files.size(path) : 0L;
      staging =
          path.resolveSibling(
              path.getFileName() + STAGING_MARKER + Long.toUnsignedString(System.nanoTime()));
      byte[] checksum = digest(ByteBuffer.wrap(bytes));
      ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
      header.putInt(MAGIC).putInt(VERSION).putLong(bytes.length).put(checksum).flip();
      try (FileChannel channel =
          FileChannel.open(staging, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
        writeFully(channel, header);
        writeFully(channel, ByteBuffer.wrap(bytes));
        channel.force(false);
      }
      try {
        if (replace) {
          Files.move(
              staging, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } else {
          Files.move(staging, path, StandardCopyOption.ATOMIC_MOVE);
        }
        long newBytes = HEADER_BYTES + bytes.length;
        knownBytes.updateAndGet(current -> Math.max(0L, current - oldBytes + newBytes));
        if (!existed) {
          knownEntries.incrementAndGet();
        }
      } catch (java.nio.file.FileAlreadyExistsException ignored) {
        Files.deleteIfExists(staging);
      }
    } catch (IOException ignored) {
      if (staging != null) {
        try {
          Files.deleteIfExists(staging);
        } catch (IOException ignoredAgain) {
          // A later sweep removes abandoned staging files.
        }
      }
    }
  }

  private synchronized boolean deleteIfIdle(Path path) {
    AtomicLong refs = mappings.get(path);
    if (refs != null && refs.get() > 0L) {
      return false;
    }
    try {
      if (!Files.isRegularFile(path)) {
        pendingDeletes.remove(path);
        return false;
      }
      long size = Files.size(path);
      if (Files.deleteIfExists(path)) {
        pendingDeletes.remove(path);
        knownBytes.updateAndGet(current -> Math.max(0L, current - size));
        knownEntries.updateAndGet(current -> Math.max(0L, current - 1L));
        events.evicted(size);
        return true;
      }
    } catch (IOException ignored) {
      // Fail open; the next sweep retries it.
    }
    return false;
  }

  private void discardCorrupt(Path path) {
    long bytes = 0L;
    try {
      bytes = Files.size(path);
    } catch (IOException ignored) {
      // The entry may already have disappeared.
    }
    events.corrupted(bytes);
    pendingDeletes.add(path);
    deleteIfIdle(path);
  }

  private void touch(Path path) {
    try {
      long now = System.currentTimeMillis();
      if (accessUpdateIntervalMillis == 0L
          || now - Files.getLastModifiedTime(path).toMillis() >= accessUpdateIntervalMillis) {
        Files.setLastModifiedTime(path, FileTime.fromMillis(now));
      }
    } catch (IOException ignored) {
      // Recency is eviction quality, never correctness.
    }
  }

  private void retainPath(Path path) {
    mappings.computeIfAbsent(path, ignored -> new AtomicLong()).incrementAndGet();
  }

  private void releasePath(Path path, boolean mapped) {
    mappings.computeIfPresent(
        path,
        (ignored, refs) -> {
          refs.decrementAndGet();
          return refs.get() == 0L ? null : refs;
        });
    if (mapped) {
      liveMappings.decrementAndGet();
    }
    if (pendingDeletes.contains(path)) {
      deleteIfIdle(path);
    }
  }

  private synchronized boolean retire(Path path) {
    // Mark first, then inspect the reference count. A concurrent reader either retained before
    // this mark (and therefore protects the file) or observes the mark and never opens it.
    pendingDeletes.add(path);
    return deleteIfIdle(path);
  }

  private PartitionState partition(String partition) {
    return partitions.computeIfAbsent(partition, ignored -> new PartitionState());
  }

  private Path entryPath(Key key) {
    String identity = hexDigest(key.identity());
    return partitionPath(key.partition())
        .resolve(identity.substring(0, 2))
        .resolve(identity + ENTRY_SUFFIX);
  }

  private static Key rangeKey(Key key, long offset, int length) {
    return new Key(
        key.partition(),
        "\u0000range\u0000" + offset + "\u0000" + length + "\u0000" + key.identity());
  }

  private Path partitionPath(String partition) {
    String hash = hexDigest(partition);
    return root.resolve(hash.substring(0, 2)).resolve(hash);
  }

  private static String hexDigest(String value) {
    return HexFormat.of()
        .formatHex(
            digest(ByteBuffer.wrap(value.getBytes(java.nio.charset.StandardCharsets.UTF_8))));
  }

  private static byte[] digest(ByteBuffer bytes) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(bytes);
      return digest.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }

  private static void verify(ByteBuffer bytes, byte[] expected) throws IOException {
    if (!MessageDigest.isEqual(digest(bytes), expected)) {
      throw new IOException("blob-cache checksum mismatch");
    }
  }

  private static void readFully(FileChannel channel, ByteBuffer target, long position)
      throws IOException {
    while (target.hasRemaining()) {
      int read = channel.read(target, position);
      if (read < 0) {
        throw new IOException("truncated blob-cache entry");
      }
      position += read;
    }
  }

  private static void writeFully(FileChannel channel, ByteBuffer source) throws IOException {
    while (source.hasRemaining()) {
      channel.write(source);
    }
  }

  private static byte[] copy(Content content) {
    byte[] bytes = new byte[content.size()];
    content.buffer().get(bytes);
    return bytes;
  }

  private static long saturatingAdd(long left, long right) {
    return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
  }

  private static void deleteEmptyTree(Path directory) throws IOException {
    if (!Files.exists(directory)) {
      return;
    }
    try (DirectoryStream<Path> children = Files.newDirectoryStream(directory)) {
      for (Path child : children) {
        if (Files.isDirectory(child)) {
          deleteEmptyTree(child);
        }
      }
    }
    try (DirectoryStream<Path> remaining = Files.newDirectoryStream(directory)) {
      if (!remaining.iterator().hasNext()) {
        Files.deleteIfExists(directory);
      }
    }
  }

  private void requireOpen() {
    if (closed.get()) {
      throw new IllegalStateException("blob cache is closed");
    }
  }

  @Override
  public long bytes() {
    return knownBytes.get();
  }

  @Override
  public long entryCount() {
    return knownEntries.get();
  }

  @Override
  public long liveMappings() {
    return liveMappings.get();
  }

  @Override
  public long maxBytes() {
    return maxBytes;
  }

  @Override
  public boolean enabled() {
    return !closed.get();
  }

  @Override
  public CacheFamily family() {
    return CacheFamily.BLOB;
  }

  @Override
  public void close() {
    closed.set(true);
  }

  private record Header(int payloadBytes, byte[] checksum) {}

  private record Entry(Path path, long bytes, long modifiedMillis) {}

  private record Scan(long bytes, long entries, List<Entry> oldest) {}

  private static final class PartitionState {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private boolean retired;
  }

  private static final class HeapContent implements Content {
    private final ByteBuffer bytes;

    private HeapContent(byte[] bytes) {
      this(ByteBuffer.wrap(bytes).asReadOnlyBuffer());
    }

    private HeapContent(ByteBuffer bytes) {
      this.bytes = bytes;
    }

    @Override
    public ByteBuffer buffer() {
      return bytes.asReadOnlyBuffer();
    }

    @Override
    public int size() {
      return bytes.remaining();
    }

    @Override
    public void close() {}
  }

  private static final class SliceContent implements Content {
    private final Content whole;
    private final int offset;
    private final int length;
    private final AtomicBoolean released = new AtomicBoolean();

    private SliceContent(Content whole, long offset, int length) {
      if (offset > whole.size() || (long) length > whole.size() - offset) {
        throw new IllegalArgumentException("blob-cache range exceeds the cached body");
      }
      this.whole = whole;
      this.offset = Math.toIntExact(offset);
      this.length = length;
    }

    @Override
    public ByteBuffer buffer() {
      if (released.get()) {
        throw new IllegalStateException("blob-cache range content is closed");
      }
      ByteBuffer bytes = whole.buffer();
      bytes.position(offset);
      bytes.limit(offset + length);
      return bytes.slice().asReadOnlyBuffer();
    }

    @Override
    public int size() {
      return length;
    }

    @Override
    public void close() {
      if (released.compareAndSet(false, true)) {
        whole.close();
      }
    }
  }

  private final class MappedContent implements Content {
    private final Path path;
    private final ByteBuffer bytes;
    private final Arena arena;
    private final AtomicBoolean released = new AtomicBoolean();

    private MappedContent(Path path, ByteBuffer bytes, Arena arena) {
      this.path = path;
      this.bytes = bytes;
      this.arena = arena;
    }

    @Override
    public ByteBuffer buffer() {
      if (released.get()) {
        throw new IllegalStateException("mapped blob content is closed");
      }
      return bytes.asReadOnlyBuffer();
    }

    @Override
    public int size() {
      return bytes.remaining();
    }

    @Override
    public void close() {
      if (released.compareAndSet(false, true)) {
        arena.close();
        releasePath(path, true);
      }
    }
  }
}
