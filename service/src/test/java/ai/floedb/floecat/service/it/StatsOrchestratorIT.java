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
package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.*;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.rpc.*;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.statistics.StatsOrchestrator;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsResolutionResult;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsSyncOutcome;
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for sync-first resolution policy in {@link StatsOrchestrator}.
 *
 * <p>Tests run against the full CDI context (no mocks) and verify outcome quality — HIT, SKIPPED,
 * FAILED, TIMEOUT — and that async follow-up jobs are enqueued when required.
 */
@QuarkusTest
class StatsOrchestratorIT {

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  ConnectorsGrpc.ConnectorsBlockingStub connectors;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;
  @Inject StatsOrchestrator orchestrator;
  @Inject StatsStore statsStore;
  @Inject ReconcileJobStore jobStore;

  private static final String PREFIX = "StatsOrchestratorIT_";
  private static final long SNAP = 42L;
  private static final Set<String> ALL_JOB_STATES =
      Set.of(
          "JS_QUEUED", "JS_RUNNING", "JS_FAILED", "JS_SUCCEEDED", "JS_CANCELLED", "JS_CANCELLING");
  private static final StatsTarget TABLE_TARGET =
      StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build();

  @BeforeEach
  void reset() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void resolveReturnsHitWhenStatsAreSeeded() {
    var cat = TestSupport.createCatalog(catalog, PREFIX + "hit", "");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "ns", List.of("s"), "");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "t",
            "s3://bucket/t",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "");
    ResourceId tableId = tbl.getResourceId();

    statsStore.putTargetStats(
        TargetStatsRecords.tableRecord(
            tableId, SNAP, TableValueStats.newBuilder().setRowCount(5).build(), null));

    // Guard: confirm seed is visible in the store before calling resolve.
    assertTrue(
        statsStore.getTargetStats(tableId, SNAP, TABLE_TARGET).isPresent(),
        "seeded stats record must be visible in the store before resolve");

    StatsCaptureRequest req =
        StatsCaptureRequest.builder(tableId, SNAP, TABLE_TARGET)
            .executionMode(StatsExecutionMode.ASYNC)
            .correlationId("it-hit")
            .build();

    StatsResolutionResult result = orchestrator.resolve(req);

    assertEquals(StatsSyncOutcome.HIT, result.outcome());
    assertEquals("", result.outcomeDetail());
    assertTrue(result.stats().isPresent(), "stats must be present on HIT");
    assertTrue(
        jobStore.list(tableId.getAccountId(), 10, "", "", Set.of("JS_QUEUED")).jobs.isEmpty(),
        "no async job should be enqueued on a store HIT");
  }

  @Test
  void resolveReturnsSkippedForAsyncModeOnMiss() {
    var cat = TestSupport.createCatalog(catalog, PREFIX + "skip", "");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "ns", List.of("s"), "");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "t",
            "s3://bucket/t",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "");
    ResourceId tableId = tbl.getResourceId();

    StatsCaptureRequest req =
        StatsCaptureRequest.builder(tableId, 1L, TABLE_TARGET)
            .executionMode(StatsExecutionMode.ASYNC)
            .correlationId("it-skip")
            .build();

    StatsResolutionResult result = orchestrator.resolve(req);

    assertEquals(StatsSyncOutcome.SKIPPED, result.outcome());
    // reason is "async_mode" because executionMode != SYNC
    assertEquals("async_mode", result.outcomeDetail());
    assertFalse(result.stats().isPresent());
    // table has no upstream connector: orchestrator skips enqueue (missing_connector_id),
    // so no job lands in the job store
    assertTrue(
        jobStore.list(tableId.getAccountId(), 10, "", "", Set.of("JS_QUEUED")).jobs.isEmpty(),
        "no job should be enqueued when table has no upstream connector");
  }

  @Test
  void resolveReturnsFailedForTableWithNoUpstreamConnector() {
    var cat = TestSupport.createCatalog(catalog, PREFIX + "noconn", "");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "ns", List.of("s"), "");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "t",
            "s3://bucket/t",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "");
    ResourceId tableId = tbl.getResourceId();

    StatsCaptureRequest req =
        StatsCaptureRequest.builder(tableId, 1L, TABLE_TARGET)
            .executionMode(StatsExecutionMode.SYNC)
            .latencyBudget(Optional.of(Duration.ofSeconds(1)))
            .correlationId("it-noconn")
            .build();

    StatsResolutionResult result = orchestrator.resolve(req);

    // sync attempt aborts immediately when table has no upstream connector
    assertEquals(StatsSyncOutcome.FAILED, result.outcome());
    assertEquals("sync capture failed; async follow-up enqueued", result.outcomeDetail());
    assertFalse(result.stats().isPresent());
    // the follow-up enqueue is also skipped (missing_connector_id), so no job in the store
    assertTrue(
        jobStore.list(tableId.getAccountId(), 10, "", "", Set.of("JS_QUEUED")).jobs.isEmpty(),
        "no job should be enqueued when table has no upstream connector");
  }

  @Test
  void resolveReturnsWeakOutcomeAndFollowUpWhenSyncCannotCompleteWithinBudget() {
    var cat = TestSupport.createCatalog(catalog, PREFIX + "timeout", "");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "ns", List.of("s"), "");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "t",
            "s3://bucket/t",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "");
    ResourceId tableId = tbl.getResourceId();

    Connector conn = createDummyConnector(cat.getResourceId(), ns.getResourceId(), "to");
    attachConnectorToTable(tableId, conn);

    // 150ms budget: sync capture enqueues job, polls at 100ms, times out before any worker runs
    StatsCaptureRequest req =
        StatsCaptureRequest.builder(tableId, 1L, TABLE_TARGET)
            .executionMode(StatsExecutionMode.SYNC)
            .latencyBudget(Optional.of(Duration.ofMillis(150)))
            .correlationId("it-timeout")
            .build();

    StatsResolutionResult result = orchestrator.resolve(req);

    // Depending on background worker timing, the sync attempt may either:
    // - hit the budget and return TIMEOUT, or
    // - transition the job to failed quickly and return FAILED.
    assertTrue(
        result.outcome() == StatsSyncOutcome.TIMEOUT || result.outcome() == StatsSyncOutcome.FAILED,
        "expected weak sync outcome (TIMEOUT or FAILED), got " + result.outcome());
    assertTrue(
        result.outcomeDetail().contains("async follow-up enqueued"),
        "expected follow-up enqueue detail, got: " + result.outcomeDetail());
    assertFalse(result.stats().isPresent());
    // At least one connector-backed job should be observable; states may transition quickly and
    // equivalent follow-up may be deduplicated.
    assertTrue(
        await(
            Duration.ofSeconds(2),
            () -> !listJobIds(tableId.getAccountId()).isEmpty(),
            Duration.ofMillis(20)),
        "job should be observable after sync TIMEOUT when connector is present");
  }

  @Test
  void resolveSkippedEnqueuesJobWhenTableHasConnector() {
    var cat = TestSupport.createCatalog(catalog, PREFIX + "skip2", "");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "ns", List.of("s"), "");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "t",
            "s3://bucket/t",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "");
    ResourceId tableId = tbl.getResourceId();

    Connector conn = createDummyConnector(cat.getResourceId(), ns.getResourceId(), "sk2");
    attachConnectorToTable(tableId, conn);

    StatsCaptureRequest req =
        StatsCaptureRequest.builder(tableId, 1L, TABLE_TARGET)
            .executionMode(StatsExecutionMode.ASYNC)
            .correlationId("it-skip-conn")
            .build();

    StatsResolutionResult result = orchestrator.resolve(req);

    assertEquals(StatsSyncOutcome.SKIPPED, result.outcome());
    assertEquals("async_mode", result.outcomeDetail());
    assertFalse(result.stats().isPresent());
    assertTrue(
        await(
            Duration.ofSeconds(2),
            () -> !listJobIds(tableId.getAccountId()).isEmpty(),
            Duration.ofMillis(20)),
        "async job should be enqueued when table has an upstream connector");
  }

  @Test
  void resolveReturnsFailedAndEnqueuesFollowUpWhenSyncJobIsCancelled() throws Exception {
    var cat = TestSupport.createCatalog(catalog, PREFIX + "cancel", "");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "ns", List.of("s"), "");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "t",
            "s3://bucket/t",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "");
    ResourceId tableId = tbl.getResourceId();

    Connector conn = createDummyConnector(cat.getResourceId(), ns.getResourceId(), "cancel");
    attachConnectorToTable(tableId, conn);

    // 2s budget: long enough for us to find and cancel the sync job before it times out
    StatsCaptureRequest req =
        StatsCaptureRequest.builder(tableId, 1L, TABLE_TARGET)
            .executionMode(StatsExecutionMode.SYNC)
            .latencyBudget(Optional.of(Duration.ofSeconds(2)))
            .correlationId("it-cancel")
            .build();

    CompletableFuture<StatsResolutionResult> future =
        CompletableFuture.supplyAsync(() -> orchestrator.resolve(req));

    // Poll until the sync capture job appears, then cancel it to force FAILED outcome
    String jobId = null;
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (jobId == null && System.nanoTime() < deadline) {
      var page = jobStore.list(tableId.getAccountId(), 10, "", "", Set.of("JS_QUEUED"));
      if (!page.jobs.isEmpty()) {
        jobId = page.jobs.get(0).jobId;
      } else {
        Thread.sleep(20);
      }
    }
    assertNotNull(jobId, "sync capture job must appear in job store within 3s");

    jobStore.cancel(tableId.getAccountId(), jobId, "test_cancel");
    final String cancelledJobId = jobId;

    StatsResolutionResult result = future.get(5, TimeUnit.SECONDS);

    assertEquals(StatsSyncOutcome.FAILED, result.outcome());
    assertEquals("sync capture failed; async follow-up enqueued", result.outcomeDetail());
    assertFalse(result.stats().isPresent());
    // Cancelled job quickly transitions state; require an additional job id to appear eventually.
    assertTrue(
        await(
            Duration.ofSeconds(2),
            () ->
                listJobIds(tableId.getAccountId()).stream()
                    .anyMatch(otherJobId -> !otherJobId.equals(cancelledJobId)),
            Duration.ofMillis(20)),
        "async follow-up job should be enqueued after sync FAILED");
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private Connector createDummyConnector(
      ResourceId catalogId, ResourceId namespaceId, String suffix) {
    var source =
        SourceSelector.newBuilder()
            .setNamespace(NamespacePath.newBuilder().addSegments("examples").addSegments("iceberg"))
            .build();
    var destination =
        DestinationTarget.newBuilder().setCatalogId(catalogId).setNamespaceId(namespaceId).build();
    var spec =
        ConnectorSpec.newBuilder()
            .setDisplayName(PREFIX + suffix)
            .setKind(ConnectorKind.CK_UNITY)
            .setUri("dummy://ignored")
            .setSource(source)
            .setDestination(destination)
            .setAuth(AuthConfig.newBuilder().setScheme("none"))
            .build();
    return TestSupport.createConnector(connectors, spec);
  }

  private void attachConnectorToTable(ResourceId tableId, Connector connector) {
    var existing =
        table.getTable(GetTableRequest.newBuilder().setTableId(tableId).build()).getTable();
    var upstream =
        existing.getUpstream().toBuilder()
            .setConnectorId(connector.getResourceId())
            .setUri("dummy://ignored")
            .setTableDisplayName(connector.getDisplayName() + "_src")
            .build();
    var spec = TableSpec.newBuilder().setUpstream(upstream).build();
    var mask = FieldMask.newBuilder().addPaths("upstream").build();
    table.updateTable(
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(spec)
            .setUpdateMask(mask)
            .build());
  }

  private Set<String> listJobIds(String accountId) {
    return jobStore.list(accountId, 100, "", "", ALL_JOB_STATES).jobs.stream()
        .map(job -> job.jobId)
        .collect(java.util.stream.Collectors.toSet());
  }

  private static boolean await(Duration timeout, BooleanSupplier condition, Duration step) {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return true;
      }
      try {
        Thread.sleep(Math.max(1L, step.toMillis()));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return condition.getAsBoolean();
  }
}
