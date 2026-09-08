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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class PointerWriteThroughObservabilityTest {

  private static final String ACCOUNT = "acct-1";
  private static final long CACHE_BYTES = 1024L * 1024L;

  private final InMemoryPointerStore store = new InMemoryPointerStore();
  private final RecordingEvents events = new RecordingEvents();
  private final PointerCache cache =
      new PointerCache(AuthoritativePointerStore.of(store), CACHE_BYTES, events);
  private final CachingPointerStore caching = new CachingPointerStore(store, cache);

  @Test
  void publishingIntoACompleteIndexRecordsAnAppliedWriteThrough() {
    String key = Keys.tablePointerByName(ACCOUNT, "catalog", "namespace", "orders");
    assertThat(caching.get(key)).isEmpty();

    assertThat(caching.compareAndSet(key, 0L, pointer(key, "s3://orders"))).isTrue();

    assertThat(events.applied).hasValue(1);
    assertThat(events.skipped).hasValue(0);
    assertThat(events.account).hasValue(ACCOUNT);
  }

  @Test
  void refreshingAResidentPointerRecordsAnAppliedWriteThrough() {
    String key = Keys.tableRootByTable(ACCOUNT, "table-1");
    store.compareAndSet(key, 0L, pointer(key, "s3://root-v1"));
    assertThat(caching.get(key)).isPresent();

    assertThat(caching.compareAndSet(key, 1L, pointer(key, "s3://root-v2"))).isTrue();

    assertThat(events.applied).hasValue(1);
    assertThat(events.skipped).hasValue(0);
    assertThat(events.account).hasValue(ACCOUNT);
  }

  @Test
  void aColdResidentPointerRecordsASkippedWriteThrough() {
    String key = Keys.tableRootByTable(ACCOUNT, "table-1");

    assertThat(caching.compareAndSet(key, 0L, pointer(key, "s3://root"))).isTrue();

    assertThat(cache.peek(key)).isEmpty();
    assertThat(events.applied).hasValue(0);
    assertThat(events.skipped).hasValue(1);
    assertThat(events.account).hasValue(ACCOUNT);
  }

  @Test
  void aPublicationRejectedByTheVersionGuardRecordsASkippedWriteThrough() {
    String key = Keys.tableRootByTable(ACCOUNT, "table-1");
    store.compareAndSet(key, 0L, pointer(key, "s3://root-v1"));
    store.compareAndSet(key, 1L, pointer(key, "s3://root-v2"));
    assertThat(caching.get(key).map(Pointer::getVersion)).contains(2L);

    store.delete(key);
    assertThat(caching.compareAndSet(key, 0L, pointer(key, "s3://recreated"))).isTrue();

    assertThat(cache.peek(key)).isEmpty();
    assertThat(events.applied).hasValue(0);
    assertThat(events.skipped).hasValue(1);
    assertThat(events.account).hasValue(ACCOUNT);
  }

  private static Pointer pointer(String key, String blobUri) {
    return Pointer.newBuilder().setKey(key).setBlobUri(blobUri).build();
  }

  private static final class RecordingEvents implements CacheEvents {
    private final AtomicInteger applied = new AtomicInteger();
    private final AtomicInteger skipped = new AtomicInteger();
    private final AtomicReference<String> account = new AtomicReference<>();

    @Override
    public CacheEvents forAccount(String accountId) {
      return new CacheEvents() {
        @Override
        public void writeThrough(WriteThroughResult result) {
          account.set(accountId);
          record(result);
        }
      };
    }

    @Override
    public void writeThrough(WriteThroughResult result) {
      account.set("<global>");
      record(result);
    }

    private void record(WriteThroughResult result) {
      switch (result) {
        case APPLIED -> applied.incrementAndGet();
        case SKIPPED -> skipped.incrementAndGet();
      }
    }
  }
}
