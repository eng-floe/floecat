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
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ObservedPointerStoreTest {
  @Test
  void batchReadIsOneCallAddressingEveryKey() {
    List<ReadCall> observed = new ArrayList<>();
    StoreReadObserver observer =
        new StoreReadObserver() {
          @Override
          public boolean capturesTargets() {
            return true;
          }

          @Override
          public Observation begin(ReadCall call) {
            observed.add(call);
            return Observation.NOOP;
          }
        };
    PointerStore store = new ObservedPointerStore(new InMemoryPointerStore(), observer);

    store.getBatch(List.of("first", "second", "third"));

    assertThat(observed)
        .singleElement()
        .satisfies(
            call -> {
              assertThat(call.store()).isEqualTo(StoreReadObserver.Store.POINTER);
              assertThat(call.operation()).isEqualTo(StoreReadObserver.Operation.GET_BATCH);
              assertThat(call.itemCount()).isEqualTo(3);
              assertThat(call.targets()).containsExactly("first", "second", "third");
            });
  }

  @Test
  void targetNamesAreNotCapturedUnlessTheObserverOptsIn() {
    List<ReadCall> observed = new ArrayList<>();
    PointerStore store =
        new ObservedPointerStore(
            new InMemoryPointerStore(),
            call -> {
              observed.add(call);
              return Observation.NOOP;
            });

    store.get("sensitive-key");

    assertThat(observed).singleElement().extracting(ReadCall::targets).isEqualTo(List.of());
  }
}
