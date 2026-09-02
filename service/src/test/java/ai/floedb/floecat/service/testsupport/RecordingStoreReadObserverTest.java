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

package ai.floedb.floecat.service.testsupport;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.service.storage.StoreReadObserver.Operation;
import ai.floedb.floecat.service.storage.StoreReadObserver.ReadCall;
import ai.floedb.floecat.service.storage.StoreReadObserver.Store;
import java.util.List;
import org.junit.jupiter.api.Test;

class RecordingStoreReadObserverTest {
  @Test
  void keepsBatchCallsAndAddressedItemsAsDifferentUnits() {
    RecordingStoreReadObserver observer = new RecordingStoreReadObserver();

    observer.begin(new ReadCall(Store.POINTER, Operation.GET_BATCH, 3, List.of("a", "b", "c")));
    observer.begin(new ReadCall(Store.BLOB, Operation.GET_BATCH, 2, List.of("one", "two")));

    assertThat(observer.pointerRoundTrips()).isEqualTo(1);
    assertThat(observer.pointerKeysRead()).isEqualTo(3);
    assertThat(observer.blobRoundTrips()).isEqualTo(1);
    assertThat(observer.blobObjectGets()).isEqualTo(2);
  }

  @Test
  void preservesLegacyUnitsForDefaultAndConsistentMethods() {
    RecordingStoreReadObserver observer = new RecordingStoreReadObserver();

    observer.begin(new ReadCall(Store.POINTER, Operation.SCAN_PREFIX_CONSISTENT, 0, List.of("/")));
    observer.begin(new ReadCall(Store.POINTER, Operation.COUNT_PREFIX_CONSISTENT, 0, List.of("/")));
    observer.begin(new ReadCall(Store.POINTER, Operation.IS_EMPTY, 0, List.of()));
    observer.begin(new ReadCall(Store.BLOB, Operation.GET_RANGE, 1, List.of("one")));
    observer.begin(new ReadCall(Store.BLOB, Operation.GET_RANGES, 2, List.of("two", "three")));

    assertThat(observer.pointerRoundTrips()).isEqualTo(2);
    assertThat(observer.pointerPrefixWalks()).isEqualTo(2);
    assertThat(observer.blobRoundTrips()).isEqualTo(3);
    assertThat(observer.blobObjectGets()).isEqualTo(3);
  }
}
