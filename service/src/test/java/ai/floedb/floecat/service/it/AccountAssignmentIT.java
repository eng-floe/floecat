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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.account.rpc.AccountAssignmentControlGrpc;
import ai.floedb.floecat.account.rpc.AccountOwnershipStatus;
import ai.floedb.floecat.account.rpc.AccountServingMode;
import ai.floedb.floecat.account.rpc.ApplyAssignmentRequest;
import ai.floedb.floecat.account.rpc.AssignmentPhase;
import ai.floedb.floecat.account.rpc.AssignmentStatus;
import ai.floedb.floecat.account.rpc.GetAssignmentStatusRequest;
import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetCatalogRequest;
import ai.floedb.floecat.catalog.rpc.MutinyTableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.EndQueryRequest;
import ai.floedb.floecat.query.rpc.GetQueryRequest;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.query.rpc.RenewQueryRequest;
import ai.floedb.floecat.service.common.AccountIds;
import ai.floedb.floecat.service.it.profiles.ManagedAssignmentProfile;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Drives a managed Floecat through {@code AccountAssignmentControl} only, the way the control plane
 * does, and checks what the public services do on each side of the assignment.
 */
@QuarkusTest
@TestProfile(ManagedAssignmentProfile.class)
class AccountAssignmentIT {

  @GrpcClient("floecat")
  AccountAssignmentControlGrpc.AccountAssignmentControlBlockingStub control;

  @GrpcClient("floecat")
  QueryServiceGrpc.QueryServiceBlockingStub queries;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @GrpcClient("floecat")
  MutinyTableStatisticsServiceGrpc.MutinyTableStatisticsServiceStub stats;

  @Inject TestDataResetter resetter;
  @Inject AccountRepository accountRepository;

  private String accountId;
  private String incarnation;
  private long epoch;

  @BeforeEach
  void resetStores() {
    // Release everything this process still serves before the store is wiped, so no remembered
    // fence outlives its shards; then create the account the dev principal resolves to.
    AssignmentStatus current = status();
    incarnation = current.getIncarnation();
    epoch = current.getEpoch() + 1;
    control.applyAssignment(apply(epoch, AssignmentPhase.AP_SERVING, List.of(), List.of()));
    resetter.wipeAll();
    accountId = AccountIds.randomAccountId();
    ResourceId rid =
        ResourceId.newBuilder()
            .setAccountId(accountId)
            .setId(accountId)
            .setKind(ResourceKind.RK_ACCOUNT)
            .build();
    accountRepository.create(
        Account.newBuilder()
            .setResourceId(rid)
            .setDisplayName(TestSupport.DEFAULT_SEED_ACCOUNT)
            .setCreatedAt(Timestamps.fromMillis(System.currentTimeMillis()))
            .build());
  }

  @Test
  void assignmentGatesQueriesAndWritesThroughApplyAssignmentOnly() {
    AssignmentStatus initial = status();
    assertEquals("floecat-it-0", initial.getMemberId());
    assertFalse(initial.getRecoveredFromStore());
    assertTrue(account(initial).isEmpty());

    // Not assigned: the query path refuses with the contract the control plane keys its route
    // invalidation on.
    ResourceId noCatalog =
        ResourceId.newBuilder()
            .setAccountId(accountId)
            .setId("missing")
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    assertNotAssigned(
        () ->
            queries.beginQuery(
                BeginQueryRequest.newBuilder().setDefaultCatalogId(noCatalog).build()));

    // The control plane pushes SERVING; the fence commits in the background.
    epoch++;
    control.applyAssignment(
        apply(epoch, AssignmentPhase.AP_SERVING, List.of(accountId), List.of(accountId)));
    AccountOwnershipStatus serving =
        awaitAccount(status -> status.getMode() == AccountServingMode.ASM_SERVING);
    assertTrue(serving.getGcAllowed());

    // Owned: mutations and pins are admitted.
    var cat = TestSupport.createCatalog(catalog, "assignment_it", "");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "sch", List.of("db"), "");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders",
            "s3://bucket/orders",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var snap =
        TestSupport.createFinalizedSnapshot(
            snapshot, stats, tbl.getResourceId(), 0L, System.currentTimeMillis() - 10_000L);
    var begun =
        queries.beginQuery(
            BeginQueryRequest.newBuilder()
                .setDefaultCatalogId(cat.getResourceId())
                .setTtlSeconds(60)
                .addInputs(
                    QueryInput.newBuilder()
                        .setTableId(tbl.getResourceId())
                        .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snap.getSnapshotId())))
                .build());
    String queryId = begun.getQuery().getQueryId();
    assertEquals(1, begun.getQuery().getSnapshots().getPinsCount());

    // A stale push is rejected and changes nothing.
    long stale = epoch - 1;
    assertCode(
        Status.Code.FAILED_PRECONDITION,
        () ->
            control.applyAssignment(
                apply(stale, AssignmentPhase.AP_SERVING, List.of(accountId), List.of())));
    assertEquals(epoch, status().getEpoch());

    // The account leaves this process: DRAINING with nothing in flight goes straight to UNASSIGNED.
    epoch++;
    control.applyAssignment(apply(epoch, AssignmentPhase.AP_DRAINING, List.of(), List.of()));
    awaitAccount(status -> status.getMode() != AccountServingMode.ASM_SERVING);
    assertTrue(
        account(status())
            .map(status -> status.getMode() == AccountServingMode.ASM_UNASSIGNED)
            .orElse(true));

    // Existing contexts are still served; reads fall through; new queries and writes are refused.
    queries.renewQuery(
        RenewQueryRequest.newBuilder().setQueryId(queryId).setTtlSeconds(60).build());
    assertEquals(
        queryId,
        queries
            .getQuery(GetQueryRequest.newBuilder().setQueryId(queryId).build())
            .getQuery()
            .getQueryId());
    assertEquals(
        cat.getResourceId(),
        catalog
            .getCatalog(GetCatalogRequest.newBuilder().setCatalogId(cat.getResourceId()).build())
            .getCatalog()
            .getResourceId());
    assertNotAssigned(
        () ->
            queries.beginQuery(
                BeginQueryRequest.newBuilder()
                    .setDefaultCatalogId(cat.getResourceId())
                    .addInputs(QueryInput.newBuilder().setTableId(tbl.getResourceId()))
                    .build()));
    // Every account-scoped mutation, not just the query path, must tell the control plane to
    // re-resolve.
    assertNotAssigned(() -> TestSupport.createCatalog(catalog, "assignment_it_after", ""));
    queries.endQuery(EndQueryRequest.newBuilder().setQueryId(queryId).build());
  }

  private AssignmentStatus status() {
    return control.getAssignmentStatus(GetAssignmentStatusRequest.getDefaultInstance()).getStatus();
  }

  private ApplyAssignmentRequest apply(
      long epoch, AssignmentPhase phase, List<String> accountIds, List<String> gcAllowed) {
    return ApplyAssignmentRequest.newBuilder()
        .setEpoch(epoch)
        .setPhase(phase)
        .addAllAccountIds(accountIds)
        .addAllGcAllowedAccountIds(gcAllowed)
        .setTargetIncarnation(incarnation)
        .build();
  }

  private Optional<AccountOwnershipStatus> account(AssignmentStatus status) {
    return status.getAccountsList().stream()
        .filter(account -> account.getAccountId().equals(accountId))
        .findFirst();
  }

  private AccountOwnershipStatus awaitAccount(Predicate<AccountOwnershipStatus> condition) {
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
    while (true) {
      Optional<AccountOwnershipStatus> current = account(status());
      AccountOwnershipStatus observed =
          current.orElse(
              AccountOwnershipStatus.newBuilder()
                  .setAccountId(accountId)
                  .setMode(AccountServingMode.ASM_UNASSIGNED)
                  .build());
      if (condition.test(observed)) {
        return observed;
      }
      if (System.nanoTime() > deadline) {
        throw new AssertionError("assignment did not settle: " + observed);
      }
      try {
        Thread.sleep(25L);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new AssertionError(interrupted);
      }
    }
  }

  private static void assertNotAssigned(Runnable call) {
    StatusRuntimeException failure = assertThrows(StatusRuntimeException.class, call::run);
    assertEquals(Status.Code.FAILED_PRECONDITION, failure.getStatus().getCode());
    assertTrue(
        failure.getStatus().getDescription().contains("floecat.not_assigned"),
        failure.getStatus().getDescription());
  }

  private static void assertCode(Status.Code code, Runnable call) {
    StatusRuntimeException failure = assertThrows(StatusRuntimeException.class, call::run);
    assertEquals(code, failure.getStatus().getCode(), failure.getStatus().getDescription());
  }
}
