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

package ai.floedb.floecat.service.storage;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.service.storage.StoreReadObserver.Observation;
import ai.floedb.floecat.service.storage.StoreReadObserver.ReadCall;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import java.util.List;
import org.junit.jupiter.api.Test;

class ObservedBlobStoreTest {
  @Test
  void batchReadIsOneCallWithObjectAndByteUnits() {
    InMemoryBlobStore delegate = new InMemoryBlobStore();
    delegate.put("first", new byte[] {1, 2}, "application/octet-stream");
    delegate.put("second", new byte[] {3, 4, 5}, "application/octet-stream");
    RecordingObserver observer = new RecordingObserver();
    BlobStore store = new ObservedBlobStore(delegate, observer);

    store.getBatch(List.of("first", "second"));

    assertThat(observer.call.store()).isEqualTo(StoreReadObserver.Store.BLOB);
    assertThat(observer.call.operation()).isEqualTo(StoreReadObserver.Operation.GET_BATCH);
    assertThat(observer.call.itemCount()).isEqualTo(2);
    assertThat(observer.bytes).isEqualTo(5L);
  }

  private static final class RecordingObserver implements StoreReadObserver {
    private ReadCall call;
    private long bytes = Long.MIN_VALUE;

    @Override
    public boolean capturesTargets() {
      return true;
    }

    @Override
    public Observation begin(ReadCall call) {
      this.call = call;
      return new Observation() {
        @Override
        public void success(long observedBytes) {
          bytes = observedBytes;
        }
      };
    }
  }
}
