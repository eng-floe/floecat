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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Local-disk cache for immutable serialized bodies.
 *
 * <p>Unlike {@link MemoryCache}, a read is scoped: a large entry may be backed by a live mapped
 * file and must be closed before it can be reclaimed. The caller chooses whether a source miss may
 * fill the tier; that policy belongs here because bulk reads only need it for disk blobs.
 */
public interface BlobCache {

  enum Fill {
    FILL,
    /** Consume an existing entry, but do not admit a source miss. */
    BYPASS_FILL
  }

  /** Stable cache identity. The partition is used for account-scoped removal and telemetry. */
  record Key(String partition, String identity) {
    public Key {
      if (partition == null || partition.isBlank()) {
        throw new IllegalArgumentException("blob-cache partition must not be blank");
      }
      if (identity == null || identity.isBlank()) {
        throw new IllegalArgumentException("blob-cache identity must not be blank");
      }
    }
  }

  /** One scoped body. Its buffer is read-only and independently positioned. */
  interface Content extends AutoCloseable {
    ByteBuffer buffer();

    int size();

    @Override
    void close();
  }

  @FunctionalInterface
  interface Loader {
    /** Returns the source body, or {@code null} when it is absent. */
    byte[] load();
  }

  @FunctionalInterface
  interface BatchLoader {
    /** Returns the source bodies present for the requested keys. */
    Map<Key, byte[]> load(List<Key> keys);
  }

  Optional<Content> get(Key key, Fill fill, Loader loader);

  /**
   * Reads one immutable byte range. A resident whole body may satisfy the request; otherwise the
   * exact range is cached independently so a small lookup never admits the complete large object.
   * The loader must return exactly {@code length} bytes when the source object is present.
   */
  Optional<Content> getRange(Key key, long offset, int length, Fill fill, Loader loader);

  /** Read a batch under the same fill-fencing rules as {@link #get}. */
  Map<Key, Content> getAll(List<Key> keys, Fill fill, BatchLoader loader);

  /** Publish immutable bytes already held by a successful writer. */
  void put(Key key, byte[] bytes);

  /** Drop one identity if present. */
  void evict(Key key);

  /** Drop every identity belonging to one partition. */
  void evictPartition(String partition);

  /** Enforce the byte budget and remove abandoned staging files. */
  SweepResult sweep();

  long bytes();

  long entryCount();

  long liveMappings();

  long maxBytes();

  boolean enabled();

  CacheFamily family();

  record SweepResult(long bytesScanned, long bytesReclaimed, long entriesReclaimed) {}

  /** A disabled tier that preserves source semantics without touching disk. */
  static BlobCache disabled() {
    return new BlobCache() {
      @Override
      public Optional<Content> get(Key key, Fill fill, Loader loader) {
        byte[] loaded = loader.load();
        return Optional.ofNullable(loaded).map(HeapContent::new);
      }

      @Override
      public Optional<Content> getRange(
          Key key, long offset, int length, Fill fill, Loader loader) {
        if (key == null || fill == null || loader == null) {
          throw new NullPointerException("blob-cache range arguments must not be null");
        }
        if (offset < 0L || length < 0) {
          throw new IllegalArgumentException("blob-cache range is invalid");
        }
        byte[] loaded = loader.load();
        if (loaded != null && loaded.length != length) {
          throw new IllegalArgumentException("blob-cache range loader returned the wrong length");
        }
        return Optional.ofNullable(loaded).map(HeapContent::new);
      }

      @Override
      public Map<Key, Content> getAll(List<Key> keys, Fill fill, BatchLoader loader) {
        Map<Key, byte[]> loaded = loader.load(List.copyOf(keys));
        var result = new java.util.LinkedHashMap<Key, Content>();
        for (Key key : keys) {
          byte[] bytes = loaded.get(key);
          if (bytes != null) {
            result.put(key, new HeapContent(bytes));
          }
        }
        return result;
      }

      @Override
      public void put(Key key, byte[] bytes) {}

      @Override
      public void evict(Key key) {}

      @Override
      public void evictPartition(String partition) {}

      @Override
      public SweepResult sweep() {
        return new SweepResult(0L, 0L, 0L);
      }

      @Override
      public long bytes() {
        return 0L;
      }

      @Override
      public long entryCount() {
        return 0L;
      }

      @Override
      public long liveMappings() {
        return 0L;
      }

      @Override
      public long maxBytes() {
        return 0L;
      }

      @Override
      public boolean enabled() {
        return false;
      }

      @Override
      public CacheFamily family() {
        return CacheFamily.BLOB;
      }
    };
  }

  /** Heap body used only by the disabled pass-through implementation. */
  final class HeapContent implements Content {
    private final byte[] bytes;

    private HeapContent(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public ByteBuffer buffer() {
      return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
    }

    @Override
    public int size() {
      return bytes.length;
    }

    @Override
    public void close() {}
  }
}
