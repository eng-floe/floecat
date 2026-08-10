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
package ai.floedb.floecat.service.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.nullable;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.query.rpc.PinKind;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.query.resolver.QueryInputResolver;
import ai.floedb.floecat.service.testsupport.ConcurrentTestSupport;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** Verifies query-input pin chains share the process metadata-I/O admission gate. */
class QueryInputResolverAdmissionTest {

  @Test
  void pinResolutionWaitsForProcessAdmissionBeforeStartingTheBackend() throws Exception {
    MetadataIoRunner admission = new MetadataIoRunner(1);
    MetadataResourceReader admittedReads = new MetadataResourceReader(admission);
    CatalogOverlay graph = mock(CatalogOverlay.class);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("table")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    TablePin expected =
        TablePin.newBuilder()
            .setTableId(tableId)
            .setPinKind(PinKind.PIN_KIND_CURRENT)
            .setSnapshotId(1)
            .build();
    AtomicBoolean backendStarted = new AtomicBoolean();
    when(graph.tablePinFor(anyString(), eq(tableId), nullable(SnapshotRef.class), any()))
        .thenAnswer(
            ignored -> {
              backendStarted.set(true);
              return expected;
            });
    QueryInputResolver resolver = new QueryInputResolver(graph, null, admittedReads);

    CountDownLatch holderEntered = new CountDownLatch(1);
    CountDownLatch releaseHolder = new CountDownLatch(1);
    try (ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor()) {
      CompletableFuture<Void> holder =
          CompletableFuture.runAsync(
              () ->
                  admittedReads.read(
                      () -> {
                        holderEntered.countDown();
                        ConcurrentTestSupport.awaitUninterruptibly(releaseHolder);
                        return null;
                      }),
              workers);
      assertThat(holderEntered.await(2, TimeUnit.SECONDS)).isTrue();

      CompletableFuture<QueryInputResolver.ResolutionResult> resolution =
          CompletableFuture.supplyAsync(
              () ->
                  resolver.resolveInputs(
                      "query",
                      "correlation",
                      List.of(QueryInput.newBuilder().setTableId(tableId).build()),
                      Optional.empty(),
                      Optional.empty(),
                      new ConcurrentHashMap<>(),
                      null,
                      () -> false),
              workers);
      try {
        ConcurrentTestSupport.await(() -> admission.admissionWaiters() == 1, Duration.ofSeconds(2));
        assertThat(backendStarted).isFalse();

        releaseHolder.countDown();
        assertThat(resolution.get(2, TimeUnit.SECONDS).relationPinSet().getPinsCount()).isOne();
        assertThat(backendStarted).isTrue();
      } finally {
        releaseHolder.countDown();
        holder.get(2, TimeUnit.SECONDS);
      }
      ConcurrentTestSupport.await(() -> admission.permitsInUse() == 0, Duration.ofSeconds(2));
    }
  }
}
