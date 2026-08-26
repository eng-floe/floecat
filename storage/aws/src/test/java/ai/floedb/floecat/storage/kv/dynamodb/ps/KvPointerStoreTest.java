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
package ai.floedb.floecat.storage.kv.dynamodb.ps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.storage.aws.AwsStorageMetricPublisher;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.kv.AttrValue;
import ai.floedb.floecat.storage.kv.KvStore;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import ai.floedb.floecat.telemetry.StorageTelemetry;
import ai.floedb.floecat.telemetry.StoreOperationSummary;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.TestObservability;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.smallrye.mutiny.Uni;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.metrics.MetricCollector;

class KvPointerStoreTest {

  @Test
  void requestFixtureReconcilesLogicalFakeStoreAndSdkCalls() {
    TestObservability observability = new TestObservability();
    StorageTelemetry telemetry = new StorageTelemetry(observability);
    FailingReadKvStore backend = new FailingReadKvStore(null);
    KvPointerStore store = new KvPointerStore(new PointerStoreEntity(backend), telemetry) {};
    AwsStorageMetricPublisher publisher = new AwsStorageMetricPublisher(telemetry);
    Context requestContext = StoreOperationSummary.start(Context.root(), true);

    try (Scope ignored = requestContext.makeCurrent()) {
      store.get("/accounts/acct-1/k");
      MetricCollector sdkCall = MetricCollector.create("ApiCall");
      sdkCall.reportMetric(CoreMetric.SERVICE_ID, "DynamoDb");
      sdkCall.reportMetric(CoreMetric.OPERATION_NAME, "GetItem");
      sdkCall.reportMetric(CoreMetric.API_CALL_SUCCESSFUL, true);
      publisher.publish(sdkCall.collect());
      PhaseDiagnostics diagnostics = observability.diagnostics("test", "request");
      StoreOperationSummary.addTo(diagnostics);
      diagnostics.emit("request-summary");
    }

    assertEquals(1, backend.getCalls());
    assertEquals(
        1L,
        observability.diagnosticEvents().stream()
            .filter(event -> event.eventName().equals("request-summary"))
            .findFirst()
            .orElseThrow()
            .fields()
            .get("backend_dynamodb_get_count"));
    assertEquals(
        List.of("get", "get_item"),
        observability.counterTagHistory(Telemetry.Metrics.STORE_REQUESTS).stream()
            .map(
                tags ->
                    tags.stream()
                        .filter(tag -> tag.key().equals("operation"))
                        .findFirst()
                        .orElseThrow()
                        .value())
            .toList());
  }

  @Test
  void everyBackendOperationCrossesTheSharedTelemetrySeamOnce() {
    TestObservability observability = new TestObservability();
    KvPointerStore store =
        new KvPointerStore(
            new PointerStoreEntity(new FailingReadKvStore(null)),
            new StorageTelemetry(observability)) {};
    Pointer pointer = Pointer.newBuilder().setKey("/accounts/acct-1/k").build();

    store.get("/accounts/acct-1/k");
    store.compareAndSet("/accounts/acct-1/k", 0L, pointer);
    store.delete("/accounts/acct-1/k");
    store.compareAndDelete("/accounts/acct-1/k", 1L);
    store.compareAndSetBatch(List.of());
    store.listPointersByPrefix("/accounts/acct-1/", 10, "", new StringBuilder());
    store.deleteByPrefix("/accounts/acct-1/");
    store.countByPrefix("/accounts/acct-1/");
    store.isEmpty();
    store.dump("test");
    // Token encoding is local, so the operation list below must not gain a false backend call.
    assertEquals("token", store.pageTokenAfterKey("/accounts/acct-1/k"));

    assertEquals(
        List.of(
            "get",
            "compare_and_set",
            "delete",
            "compare_and_delete",
            "compare_and_set_batch",
            "list_by_prefix",
            "delete_by_prefix",
            "count_by_prefix",
            "is_empty",
            "dump"),
        observability.scopes().get("STORE").stream().map(scope -> scope.operation()).toList());
  }

  @Test
  void getCrossesSharedStorageTelemetrySeamOnce() {
    TestObservability observability = new TestObservability();
    KvPointerStore store =
        new KvPointerStore(
            new PointerStoreEntity(new FailingReadKvStore(null)),
            new StorageTelemetry(observability)) {};

    store.get("/accounts/acct-1/catalog/cat-1");

    assertSame(1, observability.scopes().get("STORE").size());
    assertSame("dynamodb", observability.scopes().get("STORE").get(0).component());
    assertSame("get", observability.scopes().get("STORE").get(0).operation());
  }

  @Test
  void getRethrowsNonClosedPoolRuntimeException() {
    RuntimeException failure = new RuntimeException("validation failed");
    KvPointerStore store = pointerStoreFailingReadsWith(failure);

    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> store.get("/accounts/acct-1/catalog/cat-1"));

    assertSame(failure, thrown);
  }

  @Test
  void getWrapsClosedPoolRuntimeExceptionAsRetryableAbort() {
    RuntimeException failure = new RuntimeException("Connection pool shut down");
    KvPointerStore store = pointerStoreFailingReadsWith(failure);

    StorageAbortRetryableException thrown =
        assertThrows(
            StorageAbortRetryableException.class,
            () -> store.get("/accounts/acct-1/catalog/cat-1"));

    assertSame(failure, thrown.getCause());
  }

  private static KvPointerStore pointerStoreFailingReadsWith(RuntimeException failure) {
    return new KvPointerStore(
        new PointerStoreEntity(new FailingReadKvStore(failure)),
        new StorageTelemetry(new TestObservability())) {};
  }

  private static final class FailingReadKvStore implements KvStore {
    private final RuntimeException failure;
    private int getCalls;

    private FailingReadKvStore(RuntimeException failure) {
      this.failure = failure;
    }

    @Override
    public Uni<Optional<Record>> get(Key key) {
      getCalls++;
      return failure == null
          ? Uni.createFrom().item(Optional.empty())
          : Uni.createFrom().failure(failure);
    }

    private int getCalls() {
      return getCalls;
    }

    @Override
    public Uni<Boolean> putCas(Record record, long expectedVersion) {
      return Uni.createFrom().item(true);
    }

    @Override
    public Uni<Boolean> deleteCas(Key key, long expectedVersion) {
      return Uni.createFrom().item(true);
    }

    @Override
    public Uni<Optional<Long>> updateMetadataAttrsIfExists(
        Key key, Map<String, AttrValue> sets, Map<String, Long> increments) {
      return Uni.createFrom().item(Optional.empty());
    }

    @Override
    public Uni<Page> queryByPartitionKeyPrefix(
        String partitionKey, String sortKeyPrefix, int limit, Optional<String> pageToken) {
      return Uni.createFrom().item(new Page(List.of(), Optional.empty()));
    }

    @Override
    public String pageTokenAfterKey(Key key) {
      return "token";
    }

    @Override
    public Uni<Integer> deleteByPrefix(String partitionKey, String sortKeyPrefix) {
      return Uni.createFrom().item(0);
    }

    @Override
    public Uni<Void> reset() {
      return Uni.createFrom().voidItem();
    }

    @Override
    public Uni<Boolean> isEmpty() {
      return Uni.createFrom().item(true);
    }

    @Override
    public Uni<Void> dump(String header) {
      return Uni.createFrom().voidItem();
    }

    @Override
    public Uni<Boolean> txnWriteCas(List<TxnOp> ops) {
      return Uni.createFrom().item(true);
    }
  }
}
