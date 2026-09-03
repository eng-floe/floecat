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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DiskBlobCacheTest {

  @TempDir Path root;

  @Test
  void aFilledBodySurvivesASecondCacheInstanceWithoutReloading() throws Exception {
    BlobCache.Key key = new BlobCache.Key("account-a", "sha-a");
    byte[] expected = "payload".getBytes(StandardCharsets.UTF_8);
    AtomicInteger loads = new AtomicInteger();

    try (var first = cache()) {
      assertThat(read(first, key, BlobCache.Fill.FILL, () -> load(loads, expected)))
          .containsExactly(expected);
    }
    try (var restarted = cache()) {
      assertThat(read(restarted, key, BlobCache.Fill.FILL, () -> load(loads, expected)))
          .containsExactly(expected);
    }

    assertThat(loads).hasValue(1);
  }

  @Test
  void bypassReturnsTheBodyWithoutFillingDisk() throws Exception {
    BlobCache.Key key = new BlobCache.Key("account-a", "sha-a");
    AtomicInteger loads = new AtomicInteger();
    byte[] expected = "payload".getBytes(StandardCharsets.UTF_8);

    try (var cache = cache()) {
      assertThat(read(cache, key, BlobCache.Fill.BYPASS_FILL, () -> load(loads, expected)))
          .containsExactly(expected);
      assertThat(read(cache, key, BlobCache.Fill.FILL, () -> load(loads, expected)))
          .containsExactly(expected);
      assertThat(read(cache, key, BlobCache.Fill.BYPASS_FILL, () -> load(loads, expected)))
          .containsExactly(expected);
    }

    assertThat(loads).hasValue(2);
  }

  @Test
  void aTruncatedEntryIsDiscardedAndReloaded() throws Exception {
    BlobCache.Key key = new BlobCache.Key("account-a", "sha-a");
    AtomicInteger loads = new AtomicInteger();
    byte[] expected = "payload".getBytes(StandardCharsets.UTF_8);

    try (var cache = cache()) {
      assertThat(read(cache, key, BlobCache.Fill.FILL, () -> load(loads, expected)))
          .containsExactly(expected);
      Path entry =
          Files.walk(root)
              .filter(path -> path.toString().endsWith(".blob"))
              .findFirst()
              .orElseThrow();
      Files.write(entry, new byte[] {1, 2, 3});
      assertThat(read(cache, key, BlobCache.Fill.FILL, () -> load(loads, expected)))
          .containsExactly(expected);
    }

    assertThat(loads).hasValue(2);
  }

  @Test
  void aSweepReclaimsAnotherEntryWithoutDeletingALiveMapping() throws Exception {
    BlobCache.Key key = new BlobCache.Key("account-a", "sha-a");
    byte[] expected = "mapped-payload".getBytes(StandardCharsets.UTF_8);

    try (var cache = new DiskBlobCache(root, 62, 1, Duration.ZERO, BlobCacheEvents.none())) {
      try (BlobCache.Content ignored =
          cache.get(key, BlobCache.Fill.FILL, () -> expected).orElseThrow()) {
        // The cold source response is already on heap. The next read proves the disk-hit path.
      }
      BlobCache.Content content =
          cache
              .get(
                  key,
                  BlobCache.Fill.FILL,
                  () -> {
                    throw new AssertionError("a disk hit must not reload");
                  })
              .orElseThrow();
      try (BlobCache.Content ignored =
          cache
              .get(
                  new BlobCache.Key("account-a", "sha-b"),
                  BlobCache.Fill.FILL,
                  () -> new byte[] {1})
              .orElseThrow()) {
        // A second entry puts the tier over budget while the first is mapped.
      }
      assertThat(cache.liveMappings()).isOne();
      assertThat(cache.sweep().entriesReclaimed()).isOne();
      assertThat(bytes(content)).containsExactly(expected);
      content.close();
      assertThat(cache.liveMappings()).isZero();
      cache.evict(key);
      AtomicInteger reloads = new AtomicInteger();
      read(cache, key, BlobCache.Fill.FILL, () -> load(reloads, expected));
      assertThat(reloads).hasValue(1);
    }
  }

  @Test
  void evictPartitionDropsOnlyThatAccountsFiles() throws Exception {
    BlobCache.Key first = new BlobCache.Key("account-a", "same-content");
    BlobCache.Key second = new BlobCache.Key("account-b", "same-content");

    try (var cache = cache()) {
      read(cache, first, BlobCache.Fill.FILL, () -> new byte[] {1});
      read(cache, second, BlobCache.Fill.FILL, () -> new byte[] {2});
      cache.evictPartition("account-a");

      AtomicInteger firstLoads = new AtomicInteger();
      AtomicInteger secondLoads = new AtomicInteger();
      read(cache, first, BlobCache.Fill.FILL, () -> load(firstLoads, new byte[] {1}));
      read(cache, second, BlobCache.Fill.FILL, () -> load(secondLoads, new byte[] {2}));
      assertThat(firstLoads).hasValue(1);
      assertThat(secondLoads).hasValue(0);
    }
  }

  @Test
  void retiredPartitionRejectsLateWriterPublication() throws Exception {
    BlobCache.Key key = new BlobCache.Key("account-a", "sha-a");

    try (var cache = cache()) {
      cache.evictPartition("account-a");
      cache.put(key, new byte[] {1});

      AtomicInteger reloads = new AtomicInteger();
      assertThat(read(cache, key, BlobCache.Fill.FILL, () -> load(reloads, new byte[] {2})))
          .containsExactly(2);
      assertThat(read(cache, key, BlobCache.Fill.FILL, () -> load(reloads, new byte[] {2})))
          .containsExactly(2);
      assertThat(reloads).hasValue(2);
    }
  }

  @Test
  void partitionEvictionFencesAnInFlightFill() throws Exception {
    BlobCache.Key key = new BlobCache.Key("account-a", "sha-a");
    byte[] old = "old-account".getBytes(StandardCharsets.UTF_8);
    byte[] current = "current-account".getBytes(StandardCharsets.UTF_8);
    CountDownLatch loading = new CountDownLatch(1);
    CountDownLatch finishLoad = new CountDownLatch(1);

    try (var cache = cache()) {
      CompletableFuture<byte[]> raced =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return read(
                      cache,
                      key,
                      BlobCache.Fill.FILL,
                      () -> {
                        loading.countDown();
                        try {
                          finishLoad.await();
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new IllegalStateException(e);
                        }
                        return old;
                      });
                } catch (Exception e) {
                  throw new IllegalStateException(e);
                }
              });
      loading.await();
      cache.evictPartition("account-a");
      finishLoad.countDown();
      assertThat(raced.join()).containsExactly(old);

      AtomicInteger reloads = new AtomicInteger();
      assertThat(read(cache, key, BlobCache.Fill.FILL, () -> load(reloads, current)))
          .containsExactly(current);
      assertThat(reloads).hasValue(1);
    }
  }

  @Test
  void exactEvictionFencesAnInFlightFill() throws Exception {
    BlobCache.Key key = new BlobCache.Key("account-a", "sha-a");
    byte[] stale = "stale".getBytes(StandardCharsets.UTF_8);
    byte[] current = "current".getBytes(StandardCharsets.UTF_8);
    CountDownLatch loading = new CountDownLatch(1);
    CountDownLatch finishLoad = new CountDownLatch(1);

    try (var cache = cache()) {
      CompletableFuture<byte[]> raced =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return read(
                      cache,
                      key,
                      BlobCache.Fill.FILL,
                      () -> {
                        loading.countDown();
                        try {
                          finishLoad.await();
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new IllegalStateException(e);
                        }
                        return stale;
                      });
                } catch (Exception e) {
                  throw new IllegalStateException(e);
                }
              });
      loading.await();
      cache.evict(key);
      finishLoad.countDown();
      assertThat(raced.join()).containsExactly(stale);

      AtomicInteger reloads = new AtomicInteger();
      assertThat(read(cache, key, BlobCache.Fill.FILL, () -> load(reloads, current)))
          .containsExactly(current);
      assertThat(reloads).hasValue(1);
    }
  }

  @Test
  void partitionEvictionFencesAnInFlightBatchFill() throws Exception {
    BlobCache.Key first = new BlobCache.Key("account-a", "first");
    BlobCache.Key second = new BlobCache.Key("account-a", "second");
    CountDownLatch loading = new CountDownLatch(1);
    CountDownLatch finishLoad = new CountDownLatch(1);

    try (var cache = cache()) {
      CompletableFuture<Map<BlobCache.Key, byte[]>> raced =
          CompletableFuture.supplyAsync(
              () ->
                  readAll(
                      cache,
                      List.of(first, second),
                      keys -> {
                        loading.countDown();
                        try {
                          finishLoad.await();
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new IllegalStateException(e);
                        }
                        return Map.of(first, new byte[] {1}, second, new byte[] {2});
                      }));
      loading.await();
      cache.evictPartition("account-a");
      finishLoad.countDown();
      assertThat(raced.join())
          .containsEntry(first, new byte[] {1})
          .containsEntry(second, new byte[] {2});

      AtomicInteger reloads = new AtomicInteger();
      assertThat(read(cache, first, BlobCache.Fill.FILL, () -> load(reloads, new byte[] {3})))
          .containsExactly(3);
      assertThat(read(cache, second, BlobCache.Fill.FILL, () -> load(reloads, new byte[] {4})))
          .containsExactly(4);
      assertThat(reloads).hasValue(2);
    }
  }

  @Test
  void partitionEvictionHidesAMappedEntryUntilItsLastReaderCloses() throws Exception {
    BlobCache.Key key = new BlobCache.Key("account-a", "sha-a");
    byte[] old = "old-account".getBytes(StandardCharsets.UTF_8);
    byte[] current = "current-account".getBytes(StandardCharsets.UTF_8);

    try (var cache =
        new DiskBlobCache(root, 1024 * 1024, 1, Duration.ZERO, BlobCacheEvents.none())) {
      read(cache, key, BlobCache.Fill.FILL, () -> old);
      BlobCache.Content oldReader =
          cache
              .get(
                  key,
                  BlobCache.Fill.FILL,
                  () -> {
                    throw new AssertionError("the entry should be mapped from disk");
                  })
              .orElseThrow();
      cache.evictPartition("account-a");

      AtomicInteger reloads = new AtomicInteger();
      assertThat(read(cache, key, BlobCache.Fill.FILL, () -> load(reloads, current)))
          .containsExactly(current);
      assertThat(bytes(oldReader)).containsExactly(old);
      oldReader.close();
      assertThat(read(cache, key, BlobCache.Fill.FILL, () -> load(reloads, current)))
          .containsExactly(current);
      assertThat(read(cache, key, BlobCache.Fill.FILL, () -> load(reloads, current)))
          .containsExactly(current);
      assertThat(reloads).hasValue(3);
    }
  }

  private DiskBlobCache cache() {
    return new DiskBlobCache(root, 1024 * 1024, 256 * 1024, Duration.ZERO, BlobCacheEvents.none());
  }

  private static Map<BlobCache.Key, byte[]> readAll(
      DiskBlobCache cache, List<BlobCache.Key> keys, BlobCache.BatchLoader loader) {
    Map<BlobCache.Key, BlobCache.Content> content = cache.getAll(keys, BlobCache.Fill.FILL, loader);
    Map<BlobCache.Key, byte[]> result = new LinkedHashMap<>();
    try {
      content.forEach((key, body) -> result.put(key, bytes(body)));
      return result;
    } finally {
      content.values().forEach(BlobCache.Content::close);
    }
  }

  private static byte[] read(
      BlobCache cache, BlobCache.Key key, BlobCache.Fill fill, BlobCache.Loader loader)
      throws Exception {
    try (BlobCache.Content content = cache.get(key, fill, loader).orElse(null)) {
      return content == null ? null : bytes(content);
    }
  }

  private static byte[] bytes(BlobCache.Content content) {
    byte[] bytes = new byte[content.size()];
    content.buffer().get(bytes);
    return bytes;
  }

  private static byte[] load(AtomicInteger loads, byte[] bytes) {
    loads.incrementAndGet();
    return bytes;
  }
}
