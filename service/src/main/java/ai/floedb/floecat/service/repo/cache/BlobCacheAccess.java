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

package ai.floedb.floecat.service.repo.cache;

import ai.floedb.floecat.cache.BlobCache;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Floecat key policy in front of the generic disk cache.
 *
 * <p>Callers choose content identity, not paths. Immutable bodies use their URI. A mutable body
 * reached through a pointer uses the process incarnation, pointer key and version as well, so a
 * re-write at one URI cannot make an old disk body reachable after a version change or a process
 * restart. The process component deliberately makes mutable entries cold after restart: pointer
 * versions may return to one after delete/recreate, while immutable URI entries remain reusable.
 */
public final class BlobCacheAccess {
  private static final String GLOBAL_PARTITION = "_global";

  private final BlobCache cache;
  private final String versionedNamespace;

  public BlobCacheAccess(BlobCache cache) {
    this(cache, UUID.randomUUID().toString());
  }

  BlobCacheAccess(BlobCache cache, String versionedNamespace) {
    this.cache = Objects.requireNonNull(cache, "cache");
    this.versionedNamespace = Objects.requireNonNull(versionedNamespace, "versionedNamespace");
  }

  public static BlobCacheAccess disabled() {
    return new BlobCacheAccess(BlobCache.disabled());
  }

  public Optional<BlobCache.Content> immutable(
      String uri, BlobCache.Fill fill, BlobCache.Loader loader) {
    return cache.get(immutableKey(uri), fill, loader);
  }

  public Optional<BlobCache.Content> versioned(
      Pointer pointer, BlobCache.Fill fill, BlobCache.Loader loader) {
    Objects.requireNonNull(pointer, "pointer");
    return cache.get(versionedKey(pointer), fill, loader);
  }

  public Optional<byte[]> immutableBytes(String uri, BlobCache.Fill fill, BlobCache.Loader loader) {
    Optional<BlobCache.Content> content = immutable(uri, fill, loader);
    if (content.isEmpty()) {
      return Optional.empty();
    }
    try (BlobCache.Content body = content.orElseThrow()) {
      ByteBuffer buffer = body.buffer();
      byte[] copy = new byte[buffer.remaining()];
      buffer.get(copy);
      return Optional.of(copy);
    }
  }

  public Map<String, byte[]> immutableBytes(
      List<String> uris, BlobCache.Fill fill, Function<List<String>, Map<String, byte[]>> loader) {
    Map<String, byte[]> result = new LinkedHashMap<>();
    try (Contents contents = immutableContents(uris, fill, loader)) {
      for (String uri : contents.keys()) {
        ByteBuffer buffer = contents.get(uri).orElseThrow();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        result.put(uri, bytes);
      }
      return result;
    }
  }

  public Contents immutableContents(
      List<String> uris, BlobCache.Fill fill, Function<List<String>, Map<String, byte[]>> loader) {
    Map<BlobCache.Key, String> uriByKey = new LinkedHashMap<>();
    for (String uri : uris) {
      uriByKey.put(immutableKey(uri), uri);
    }
    Map<BlobCache.Key, BlobCache.Content> content =
        cache.getAll(
            List.copyOf(uriByKey.keySet()),
            fill,
            keys -> {
              List<String> requested = keys.stream().map(uriByKey::get).toList();
              Map<String, byte[]> loaded = loader.apply(requested);
              Map<BlobCache.Key, byte[]> byKey = new LinkedHashMap<>();
              for (BlobCache.Key key : keys) {
                byte[] bytes = loaded.get(uriByKey.get(key));
                if (bytes != null) {
                  byKey.put(key, bytes);
                }
              }
              return byKey;
            });
    Map<String, BlobCache.Content> byUri = new LinkedHashMap<>();
    content.forEach((key, body) -> byUri.put(uriByKey.get(key), body));
    return new Contents(byUri);
  }

  public Contents referencedContents(
      List<Pointer> pointers,
      BlobCache.Fill fill,
      Predicate<Pointer> immutable,
      Function<List<String>, Map<String, byte[]>> loader) {
    Map<Pointer, BlobCache.Key> keyByPointer = new LinkedHashMap<>();
    Map<BlobCache.Key, Pointer> pointerByKey = new LinkedHashMap<>();
    for (Pointer pointer : pointers) {
      BlobCache.Key key =
          immutable.test(pointer) ? immutableKey(pointer.getBlobUri()) : versionedKey(pointer);
      keyByPointer.put(pointer, key);
      pointerByKey.putIfAbsent(key, pointer);
    }
    Map<BlobCache.Key, BlobCache.Content> content =
        cache.getAll(
            List.copyOf(pointerByKey.keySet()),
            fill,
            keys -> {
              List<String> requested =
                  keys.stream().map(key -> pointerByKey.get(key).getBlobUri()).distinct().toList();
              Map<String, byte[]> loaded = loader.apply(requested);
              Map<BlobCache.Key, byte[]> byKey = new LinkedHashMap<>();
              for (BlobCache.Key key : keys) {
                byte[] bytes = loaded.get(pointerByKey.get(key).getBlobUri());
                if (bytes != null) {
                  byKey.put(key, bytes);
                }
              }
              return byKey;
            });
    Map<String, BlobCache.Content> result = new LinkedHashMap<>();
    for (Map.Entry<Pointer, BlobCache.Key> entry : keyByPointer.entrySet()) {
      BlobCache.Content body = content.get(entry.getValue());
      if (body != null) {
        result.put(entry.getKey().getKey(), body);
      }
    }
    return new Contents(result);
  }

  /** A batch whose buffers remain valid only until the batch is closed. */
  public static final class Contents implements AutoCloseable {
    private final Map<String, BlobCache.Content> content;

    private Contents(Map<String, BlobCache.Content> content) {
      this.content = Map.copyOf(content);
    }

    public Set<String> keys() {
      return content.keySet();
    }

    public Optional<ByteBuffer> get(String key) {
      BlobCache.Content body = content.get(key);
      return body == null ? Optional.empty() : Optional.of(body.buffer());
    }

    @Override
    public void close() {
      Set<BlobCache.Content> unique =
          java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
      unique.addAll(content.values());
      unique.forEach(BlobCache.Content::close);
    }
  }

  public void putImmutable(String uri, byte[] bytes) {
    cache.put(immutableKey(uri), bytes);
  }

  public void putVersioned(Pointer pointer, byte[] bytes) {
    cache.put(versionedKey(pointer), bytes);
  }

  /** Fence a version before publishing a newly-created pointer that may reuse an old version. */
  public void prepareVersionedCreate(Pointer pointer) {
    cache.evict(versionedKey(pointer));
  }

  public void evictVersioned(Pointer pointer) {
    cache.evict(versionedKey(pointer));
  }

  public void evictAccount(String accountId) {
    if (accountId != null && !accountId.isBlank()) {
      cache.evictPartition(accountId);
    }
  }

  public boolean enabled() {
    return cache.enabled();
  }

  private static BlobCache.Key immutableKey(String uri) {
    if (uri == null || uri.isBlank()) {
      throw new IllegalArgumentException("blob URI must not be blank");
    }
    return new BlobCache.Key(partition(uri), "immutable\0" + uri);
  }

  private BlobCache.Key versionedKey(Pointer pointer) {
    Objects.requireNonNull(pointer, "pointer");
    String uri = pointer.getBlobUri();
    String identity =
        "pointer\0"
            + versionedNamespace
            + '\0'
            + pointer.getKey()
            + '\0'
            + pointer.getVersion()
            + '\0'
            + uri;
    return new BlobCache.Key(partition(uri), identity);
  }

  static String partition(String uri) {
    if (uri == null || !uri.startsWith("/accounts/")) {
      return GLOBAL_PARTITION;
    }
    int start = "/accounts/".length();
    int end = uri.indexOf('/', start);
    if (end <= start) {
      return GLOBAL_PARTITION;
    }
    String encoded = uri.substring(start, end);
    if (Keys.isReservedAccountDirectorySegment(encoded)) {
      return GLOBAL_PARTITION;
    }
    try {
      return Keys.decodeSegment(encoded);
    } catch (IllegalArgumentException ignored) {
      return GLOBAL_PARTITION;
    }
  }
}
