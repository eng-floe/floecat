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

import ai.floedb.floecat.cache.CacheEvents;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Authoritative reads repair only the exact facts their result proves. */
class ConsistentPointerCacheRepairTest {

  private static final String ACCOUNT = "acct-1";
  private static final String TABLE = "tbl-1";

  private final InMemoryPointerStore store = new InMemoryPointerStore();
  private final PointerCache cache =
      new PointerCache(AuthoritativePointerStore.of(store), 1024L * 1024L, CacheEvents.none());
  private final CachingPointerStore caching = new CachingPointerStore(store, cache);

  @Test
  void aConsistentReadRepairsWhatItDisproves() {
    String key = Keys.tablePointerById(ACCOUNT, TABLE);
    store.compareAndSet(key, 0L, pointer(key, "s3://v1", 1L));
    caching.get(key);

    store.compareAndSet(key, 1L, pointer(key, "s3://v2", 2L));
    assertThat(cache.peek(key).orElseThrow().getBlobUri()).isEqualTo("s3://v1");

    assertThat(caching.getConsistent(key).orElseThrow().getBlobUri()).isEqualTo("s3://v2");
    assertThat(cache.peek(key).map(Pointer::getBlobUri)).contains("s3://v2");
    assertThat(caching.get(key).orElseThrow().getBlobUri()).isEqualTo("s3://v2");
  }

  @Test
  void aConsistentReadRepairsAKeyWhoseVersionWentBackwards() {
    String key = Keys.relationPointerByName(ACCOUNT, "cat", "ns", "sales");
    store.compareAndSet(key, 0L, pointer(key, "s3://t1", 1L));
    store.compareAndSet(key, 1L, pointer(key, "s3://t1", 2L));
    store.compareAndSet(key, 2L, pointer(key, "s3://t1", 3L));
    caching.get(key);
    assertThat(cache.peek(key).orElseThrow().getVersion()).isEqualTo(3L);

    store.delete(key);
    store.compareAndSet(key, 0L, pointer(key, "s3://t2", 1L));

    assertThat(caching.getConsistent(key).orElseThrow().getBlobUri()).isEqualTo("s3://t2");
    assertThat(cache.peek(key).map(Pointer::getBlobUri)).contains("s3://t2");
    assertThat(caching.get(key).orElseThrow().getBlobUri()).isEqualTo("s3://t2");
  }

  @Test
  void aConsistentReadDropsAnEntryTheStoreNoLongerHas() {
    String key = Keys.tablePointerById(ACCOUNT, TABLE);
    store.compareAndSet(key, 0L, pointer(key, "s3://v1", 1L));
    caching.get(key);

    store.delete(key);
    assertThat(caching.getConsistent(key)).isEmpty();
    assertThat(cache.peek(key)).isEmpty();
  }

  @Test
  void aConsistentListRepairsCachedEntriesBelowItsPrefix() {
    String key = Keys.tablePointerById(ACCOUNT, TABLE);
    store.compareAndSet(key, 0L, pointer(key, "s3://a", 1L));
    caching.get(key);
    store.delete(key);

    assertThat(
            caching.listPointersByPrefixConsistent(
                Keys.tablePointerByIdPrefix(ACCOUNT), 10, "", new StringBuilder()))
        .isEmpty();
    assertThat(caching.get(key)).isEmpty();
  }

  @Test
  void aConsistentPageWithoutATokenSinkCannotReplaceACompletePrefix() {
    String first = Keys.tablePointerById(ACCOUNT, "a");
    String second = Keys.tablePointerById(ACCOUNT, "b");
    store.compareAndSet(first, 0L, pointer(first, "s3://a", 1L));
    store.compareAndSet(second, 0L, pointer(second, "s3://b", 1L));
    caching.get(second);

    assertThat(
            caching.listPointersByPrefixConsistent(
                Keys.tablePointerByIdPrefix(ACCOUNT), 1, "", null))
        .extracting(Pointer::getKey)
        .containsExactly(first);
    assertThat(cache.peek(second)).isPresent();
  }

  @Test
  void aConsistentPageRemovesStaleEntriesInsideTheRangeItProves() {
    String first = Keys.tablePointerById(ACCOUNT, "a");
    String stale = Keys.tablePointerById(ACCOUNT, "b");
    String third = Keys.tablePointerById(ACCOUNT, "c");
    String beyondPage = Keys.tablePointerById(ACCOUNT, "d");
    for (String key : List.of(first, stale, third, beyondPage)) {
      store.compareAndSet(key, 0L, pointer(key, "s3://" + key, 1L));
    }
    caching.get(first);
    store.delete(stale);

    assertThat(
            caching.listPointersByPrefixConsistent(
                Keys.tablePointerByIdPrefix(ACCOUNT), 2, "", new StringBuilder()))
        .extracting(Pointer::getKey)
        .containsExactly(first, third);
    assertThat(
            caching.listPointersByPrefix(
                Keys.tablePointerByIdPrefix(ACCOUNT), 10, "", new StringBuilder()))
        .extracting(Pointer::getKey)
        .containsExactly(first, third, beyondPage);
  }

  @Test
  void aConsistentContinuationRepairsFromTheCachedPageBoundary() {
    String prefix = Keys.tablePointerByIdPrefix(ACCOUNT);
    List<String> keys =
        List.of(
            Keys.tablePointerById(ACCOUNT, "a"),
            Keys.tablePointerById(ACCOUNT, "b"),
            Keys.tablePointerById(ACCOUNT, "c"),
            Keys.tablePointerById(ACCOUNT, "d"),
            Keys.tablePointerById(ACCOUNT, "e"));
    keys.forEach(key -> store.compareAndSet(key, 0L, pointer(key, "s3://" + key, 1L)));

    StringBuilder cachedNext = new StringBuilder();
    assertThat(caching.listPointersByPrefix(prefix, 2, "", cachedNext))
        .extracting(Pointer::getKey)
        .containsExactly(keys.get(0), keys.get(1));
    store.delete(keys.get(2));

    assertThat(
            caching.listPointersByPrefixConsistent(
                prefix, 10, cachedNext.toString(), new StringBuilder()))
        .extracting(Pointer::getKey)
        .containsExactly(keys.get(3), keys.get(4));
    assertThat(caching.listPointersByPrefix(prefix, 10, "", new StringBuilder()))
        .extracting(Pointer::getKey)
        .containsExactly(keys.get(0), keys.get(1), keys.get(3), keys.get(4));
  }

  @Test
  void consistentPaginationCarriesARepairBoundaryAcrossPages() {
    String prefix = Keys.tablePointerByIdPrefix(ACCOUNT);
    List<String> keys =
        List.of(
            Keys.tablePointerById(ACCOUNT, "a"),
            Keys.tablePointerById(ACCOUNT, "b"),
            Keys.tablePointerById(ACCOUNT, "c"),
            Keys.tablePointerById(ACCOUNT, "d"));
    keys.forEach(key -> store.compareAndSet(key, 0L, pointer(key, "s3://" + key, 1L)));
    caching.get(keys.getFirst());

    StringBuilder next = new StringBuilder();
    assertThat(caching.listPointersByPrefixConsistent(prefix, 2, "", next))
        .extracting(Pointer::getKey)
        .containsExactly(keys.get(0), keys.get(1));
    store.delete(keys.get(2));

    assertThat(caching.listPointersByPrefixConsistent(prefix, 2, next.toString(), next))
        .extracting(Pointer::getKey)
        .containsExactly(keys.get(3));
    assertThat(caching.listPointersByPrefix(prefix, 10, "", new StringBuilder()))
        .extracting(Pointer::getKey)
        .containsExactly(keys.get(0), keys.get(1), keys.get(3));
  }

  @Test
  void anOpaqueContinuationDegradesInsteadOfClaimingACompleteRepair() {
    String prefix = Keys.tablePointerByIdPrefix(ACCOUNT);
    List<String> keys =
        List.of(
            Keys.tablePointerById(ACCOUNT, "a"),
            Keys.tablePointerById(ACCOUNT, "b"),
            Keys.tablePointerById(ACCOUNT, "c"));
    keys.forEach(key -> store.compareAndSet(key, 0L, pointer(key, "s3://" + key, 1L)));
    caching.get(keys.getFirst());
    store.delete(keys.get(1));

    assertThat(
            caching.listPointersByPrefixConsistent(
                prefix, 10, store.pageTokenAfterKey(keys.getFirst()), new StringBuilder()))
        .extracting(Pointer::getKey)
        .containsExactly(keys.get(2));
    assertThat(caching.listPointersByPrefix(prefix, 10, "", new StringBuilder()))
        .extracting(Pointer::getKey)
        .containsExactly(keys.get(0), keys.get(2));
  }

  @Test
  void aConsistentCountRepairsCachedEntriesBelowItsPrefix() {
    String key = Keys.tablePointerById(ACCOUNT, TABLE);
    store.compareAndSet(key, 0L, pointer(key, "s3://a", 1L));
    caching.get(key);
    store.delete(key);

    assertThat(caching.countByPrefixConsistent(Keys.tablePointerByIdPrefix(ACCOUNT))).isZero();
    assertThat(caching.get(key)).isEmpty();
  }

  private static Pointer pointer(String key, String blobUri, long version) {
    return Pointer.newBuilder().setKey(key).setBlobUri(blobUri).setVersion(version).build();
  }
}
