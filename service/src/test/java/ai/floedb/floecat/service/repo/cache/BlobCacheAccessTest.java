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

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.cache.BlobCache;
import ai.floedb.floecat.cache.BlobCacheEvents;
import ai.floedb.floecat.cache.DiskBlobCache;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.reconciler.impl.ReusableArtifactIndexStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.testsupport.DiskBlobCacheTestSupport;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BlobCacheAccessTest {

  @TempDir Path tempDir;

  @Test
  void accountPartitionComesFromTheCanonicalUriSegment() {
    String uri = Keys.tableBlobUri("account/with spaces", "table", "abc");

    assertThat(BlobCacheAccess.partition(uri)).isEqualTo("account/with spaces");
  }

  @Test
  void pointerVersionSeparatesBodiesThatReuseAUri() {
    var cache = new RecordingBlobCache();
    var access = new BlobCacheAccess(cache);
    String uri = "/accounts/account-a/stats/current.pb";

    access.versioned(pointer("pointer", uri, 1L), BlobCache.Fill.FILL, () -> new byte[] {1});
    access.versioned(pointer("pointer", uri, 2L), BlobCache.Fill.FILL, () -> new byte[] {2});

    assertThat(cache.keys).hasSize(2);
    assertThat(cache.keys.get(0).identity()).isNotEqualTo(cache.keys.get(1).identity());
  }

  @Test
  void immutableAdapterBatchesColdMissesThenServesThemFromDisk() {
    var delegate = new CountingBlobStore();
    delegate.put("/accounts/a/one", new byte[] {1}, "application/octet-stream");
    delegate.put("/accounts/a/two", new byte[] {2}, "application/octet-stream");
    var cached =
        new CachedImmutableBlobStore(
            delegate, DiskBlobCacheTestSupport.create(tempDir.resolve("batch")));

    assertThat(cached.getBatch(List.of("/accounts/a/one", "/accounts/a/two"))).hasSize(2);
    assertThat(delegate.batchGets).hasValue(1);
    delegate.pointGets.set(0);

    assertThat(cached.get("/accounts/a/one")).containsExactly(1);
    assertThat(delegate.pointGets).hasValue(0);
  }

  @Test
  void immutableAdapterPreservesRangeReadsOnAColdMiss() {
    var delegate = new CountingBlobStore();
    delegate.put(
        "/accounts/a/pack", "abcdef".getBytes(StandardCharsets.UTF_8), "application/octet-stream");
    var cached =
        new CachedImmutableBlobStore(
            delegate, DiskBlobCacheTestSupport.create(tempDir.resolve("range")));

    assertThat(cached.getRange("/accounts/a/pack", 1L, 3))
        .isEqualTo("bcd".getBytes(StandardCharsets.UTF_8));
    assertThat(delegate.rangeGets).hasValue(1);
    assertThat(delegate.pointGets).hasValue(0);
  }

  @Test
  void immutableAdapterKeepsMappedBatchBodiesScoped() {
    var delegate = new CountingBlobStore();
    String uri = "/accounts/a/index-object";
    delegate.put(uri, new byte[] {1, 2, 3}, "application/octet-stream");
    var disk =
        new DiskBlobCache(
            tempDir.resolve("scoped-batch"), 1024 * 1024, 1, Duration.ZERO, BlobCacheEvents.none());
    var cached = new CachedImmutableBlobStore(delegate, new BlobCacheAccess(disk));

    try (ReusableArtifactIndexStore.ScopedObjects ignored = cached.getBatchScoped(List.of(uri))) {
      assertThat(delegate.batchGets).hasValue(1);
    }
    try (ReusableArtifactIndexStore.ScopedObjects bodies = cached.getBatchScoped(List.of(uri))) {
      assertThat(bodies.get(uri)).isNotNull();
      assertThat(disk.liveMappings()).isEqualTo(1);
      assertThat(delegate.batchGets).hasValue(1);
    }
    assertThat(disk.liveMappings()).isZero();
  }

  private static Pointer pointer(String key, String uri, long version) {
    return Pointer.newBuilder().setKey(key).setBlobUri(uri).setVersion(version).build();
  }

  private static final class RecordingBlobCache implements BlobCache {
    private final List<Key> keys = new ArrayList<>();

    @Override
    public java.util.Optional<Content> get(Key key, Fill fill, Loader loader) {
      keys.add(key);
      loader.load();
      return java.util.Optional.empty();
    }

    @Override
    public Map<Key, Content> getAll(List<Key> keys, Fill fill, BatchLoader loader) {
      this.keys.addAll(keys);
      loader.load(keys);
      return Map.of();
    }

    @Override
    public void put(Key key, byte[] bytes) {
      keys.add(key);
    }

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
      return 0;
    }

    @Override
    public long entryCount() {
      return 0;
    }

    @Override
    public long liveMappings() {
      return 0;
    }

    @Override
    public long maxBytes() {
      return 0;
    }

    @Override
    public boolean enabled() {
      return true;
    }

    @Override
    public ai.floedb.floecat.cache.CacheFamily family() {
      return ai.floedb.floecat.cache.CacheFamily.BLOB;
    }
  }

  private static final class CountingBlobStore extends InMemoryBlobStore {
    private final AtomicInteger pointGets = new AtomicInteger();
    private final AtomicInteger batchGets = new AtomicInteger();
    private final AtomicInteger rangeGets = new AtomicInteger();

    @Override
    public byte[] get(String uri) {
      pointGets.incrementAndGet();
      return super.get(uri);
    }

    @Override
    public Map<String, byte[]> getBatch(List<String> uris) {
      batchGets.incrementAndGet();
      return uris.stream()
          .filter(uri -> head(uri).isPresent())
          .collect(java.util.stream.Collectors.toMap(uri -> uri, super::get));
    }

    @Override
    public byte[] getRange(String uri, long offset, int length) {
      rangeGets.incrementAndGet();
      byte[] bytes = super.get(uri);
      if (bytes == null) {
        return null;
      }
      return Arrays.copyOfRange(bytes, Math.toIntExact(offset), Math.toIntExact(offset) + length);
    }
  }
}
