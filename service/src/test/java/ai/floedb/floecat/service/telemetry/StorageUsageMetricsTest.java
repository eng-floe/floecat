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

package ai.floedb.floecat.service.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;
import org.junit.jupiter.api.Test;

class StorageUsageMetricsTest {

  @Test
  void failedBlobHeadsConsumeTheAttemptBudget() {
    StorageUsageMetrics metrics = new StorageUsageMetrics();
    metrics.pointerStore = mock(PointerStore.class);
    metrics.blobStore = mock(BlobStore.class);
    metrics.pageSize = 20;
    metrics.sampleMax = 3;
    metrics.defaultAvgBytes = 17L;
    List<Pointer> pointers =
        List.of(
            blobPointer("one"),
            blobPointer("two"),
            blobPointer("three"),
            blobPointer("four"),
            blobPointer("five"));
    when(metrics.pointerStore.listPointersByPrefix(
            anyString(),
            org.mockito.ArgumentMatchers.anyInt(),
            anyString(),
            org.mockito.ArgumentMatchers.any()))
        .thenAnswer(
            invocation -> {
              StringBuilder next = invocation.getArgument(3);
              next.setLength(0);
              return pointers;
            });
    when(metrics.blobStore.head(anyString())).thenThrow(new IllegalStateException("unavailable"));

    long estimate = metrics.estimateBytesForAccount("acct", 10);

    assertEquals(170L, estimate);
    verify(metrics.blobStore, times(3)).head(anyString());
  }

  private static Pointer blobPointer(String id) {
    return PointerReferences.blobPointer("/accounts/acct/pointers/" + id, "s3://bucket/" + id, 1L);
  }
}
