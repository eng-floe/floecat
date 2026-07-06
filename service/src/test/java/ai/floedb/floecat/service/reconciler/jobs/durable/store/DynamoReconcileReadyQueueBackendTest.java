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

package ai.floedb.floecat.service.reconciler.jobs.durable.store;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueBackend.ReadyQueueSlice;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueStore.LeaseScanStats;
import ai.floedb.floecat.storage.aws.DynamoDbClientManager;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

class DynamoReconcileReadyQueueBackendTest {

  @Test
  void scanReadySliceNormalizesApiCallTimeoutAsLeaseScanAbort() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.query(any(QueryRequest.class))).thenThrow(ApiCallTimeoutException.create(25L));
    DynamoReconcileReadyQueueBackend backend = new DynamoReconcileReadyQueueBackend();
    backend.bind(() -> dynamoDb, "floecat_pointers", null);
    LeaseScanStats stats = new LeaseScanStats();
    stats.deadlineAtMs = System.currentTimeMillis() + 5_000L;

    LeaseScanAbortedException error =
        assertThrows(
            LeaseScanAbortedException.class,
            () ->
                backend.scanReadySlice(
                    new ReadyQueueSlice(ReconcileReadyQueueStore.ReadyIndexType.GLOBAL, ""),
                    16,
                    "",
                    stats));

    assertFalse(error.callerCancelled());
    assertTrue(stats.abortedByDeadline);
  }

  @Test
  void scanReadySliceRefreshesManagerClientAndRetriesAfterClosedPool() {
    DynamoDbClient staleClient = mock(DynamoDbClient.class);
    DynamoDbClient refreshedClient = mock(DynamoDbClient.class);
    RuntimeException closedPool = new RuntimeException("Connection pool shut down");
    when(staleClient.query(any(QueryRequest.class))).thenThrow(closedPool);
    when(refreshedClient.query(any(QueryRequest.class)))
        .thenReturn(QueryResponse.builder().build());

    DynamoDbClientManager manager = mock(DynamoDbClientManager.class);
    when(manager.current()).thenReturn(staleClient, refreshedClient);
    @SuppressWarnings("unchecked")
    Instance<DynamoDbClientManager> managerInstance = mock(Instance.class);
    when(managerInstance.isResolvable()).thenReturn(true);
    when(managerInstance.get()).thenReturn(manager);

    DynamoReconcileReadyQueueBackend backend = new DynamoReconcileReadyQueueBackend();
    backend.dynamoDbClientManager = managerInstance;
    LeaseScanStats stats = new LeaseScanStats();
    stats.deadlineAtMs = System.currentTimeMillis() + 5_000L;

    var page =
        backend.scanReadySlice(
            new ReadyQueueSlice(ReconcileReadyQueueStore.ReadyIndexType.GLOBAL, ""), 16, "", stats);

    assertTrue(page.entries().isEmpty());
    verify(manager).refreshAfterFailure(staleClient, closedPool);
    verify(refreshedClient).query(any(QueryRequest.class));
  }

  @Test
  void loadCanonicalSnapshotNormalizesAttemptTimeoutAsLeaseScanAbort() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenThrow(ApiCallAttemptTimeoutException.create(25L));
    DynamoReconcileReadyQueueBackend backend = new DynamoReconcileReadyQueueBackend();
    backend.bind(() -> dynamoDb, "floecat_pointers", null);
    LeaseScanStats stats = new LeaseScanStats();
    stats.deadlineAtMs = System.currentTimeMillis() + 5_000L;

    LeaseScanAbortedException error =
        assertThrows(
            LeaseScanAbortedException.class,
            () -> backend.loadCanonicalSnapshot("/accounts/acct-1/reconcile/jobs/job-1", stats));

    assertFalse(error.callerCancelled());
    assertTrue(stats.abortedByDeadline);
  }
}
