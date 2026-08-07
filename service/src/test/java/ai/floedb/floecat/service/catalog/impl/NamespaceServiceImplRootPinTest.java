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
package ai.floedb.floecat.service.catalog.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.TopologyGraph;
import ai.floedb.floecat.service.error.impl.FloecatStatus;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableCleanupRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * DeleteNamespace decides everything from one read of the root: the subtree prefix a recursive drop
 * walks, and the path the emptiness gate probes for children. A path is not an identity — rename
 * the root and the name it vacated can be taken by a new namespace, at which point that prefix
 * leads into a subtree the request was never asked about. So the root has to be pinned to the
 * version that value came from, the way every descendant already is by {@code
 * pinDescendantToSubtree}.
 */
class NamespaceServiceImplRootPinTest {

  private static final ResourceId NAMESPACE_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("ns-1")
          .setKind(ResourceKind.RK_NAMESPACE)
          .build();
  private static final ResourceId CATALOG_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("cat-1")
          .setKind(ResourceKind.RK_CATALOG)
          .build();

  private NamespaceServiceImpl svc;
  private NamespaceRepository namespaceRepo;
  private TableCleanupRepository tableCleanupRepo;
  private RecursiveResourceDropper recursiveDropper;
  private MarkerStore markerStore;

  @BeforeEach
  void setup() {
    svc = new NamespaceServiceImpl();

    namespaceRepo = mock(NamespaceRepository.class);
    tableCleanupRepo = mock(TableCleanupRepository.class);
    recursiveDropper = mock(RecursiveResourceDropper.class);
    markerStore = mock(MarkerStore.class);
    PrincipalProvider principal = mock(PrincipalProvider.class);
    Authorizer authz = mock(Authorizer.class);
    CatalogOverlay overlay = mock(CatalogOverlay.class);

    svc.namespaceRepo = namespaceRepo;
    svc.tableRepo = mock(TableRepository.class);
    svc.tableCleanupRepo = tableCleanupRepo;
    svc.viewRepo = mock(ViewRepository.class);
    svc.recursiveDropper = recursiveDropper;
    svc.markerStore = markerStore;
    svc.principal = principal;
    svc.authz = authz;
    svc.overlay = overlay;
    svc.topology = mock(TopologyGraph.class);
    svc.metadataGraph = mock(UserGraph.class);

    var pc = mock(PrincipalContext.class);
    when(principal.get()).thenReturn(pc);
    when(pc.getCorrelationId()).thenReturn("corr");
    when(pc.getAccountId()).thenReturn("acct");
    doNothing().when(authz).require(any(), anyString());
    when(authz.allows(any(), anyString())).thenReturn(true);
    when(overlay.resolve(any())).thenReturn(Optional.empty());

    // The root as the request first reads it: db.n, at pointer version 7.
    when(namespaceRepo.metaForSafe(NAMESPACE_ID)).thenReturn(metaAt(7L));
    when(namespaceRepo.getByBlobUri("blob://ns-1"))
        .thenReturn(
            Optional.of(
                Namespace.newBuilder()
                    .setResourceId(NAMESPACE_ID)
                    .setCatalogId(CATALOG_ID)
                    .addAllParents(List.of("db"))
                    .setDisplayName("n")
                    .build()));
    when(markerStore.namespaceMarkerVersion(NAMESPACE_ID)).thenReturn(0L);
  }

  private static MutationMeta metaAt(long version) {
    return metaAt(version, "blob://ns-1");
  }

  private static MutationMeta metaAt(long version, String blobUri) {
    return MutationMeta.newBuilder()
        .setPointerKey("/accounts/acct/namespaces/by-id/ns-1")
        .setBlobUri(blobUri)
        .setPointerVersion(version)
        .setEtag("etag-" + version)
        .build();
  }

  private static Namespace namespaceValue() {
    return Namespace.newBuilder()
        .setResourceId(NAMESPACE_ID)
        .setCatalogId(CATALOG_ID)
        .addAllParents(List.of("db"))
        .setDisplayName("n")
        .build();
  }

  /**
   * The root moved between the read that produced the plan and the point where the plan is about to
   * be acted on. Nothing may be destroyed: the prefix the drop would walk is {@code db/n/}, which
   * now belongs to whatever took that name.
   */
  @Test
  void recursiveDeleteRefusesWhenTheRootMovedAfterItWasRead() {
    when(namespaceRepo.metaFor(NAMESPACE_ID)).thenReturn(metaAt(8L));

    var failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                svc.deleteNamespace(
                        DeleteNamespaceRequest.newBuilder()
                            .setNamespaceId(NAMESPACE_ID)
                            .setRecursive(true)
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.ABORTED, failure.getStatus().getCode());
    verify(recursiveDropper, never()).dropNamespaceContents(any(), anyLong(), any());
    verify(markerStore, never()).advanceNamespaceMarker(any(), anyLong());
  }

  /** And the same for a plain delete, which probes the same path for children. */
  @Test
  void plainDeleteRefusesWhenTheRootMovedAfterItWasRead() {
    when(namespaceRepo.metaFor(NAMESPACE_ID)).thenReturn(metaAt(8L));

    var failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                svc.deleteNamespace(
                        DeleteNamespaceRequest.newBuilder().setNamespaceId(NAMESPACE_ID).build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.ABORTED, failure.getStatus().getCode());
    verify(markerStore, never()).advanceNamespaceMarker(any(), anyLong());
  }

  @Test
  void plainDeletePreservesNamespaceWhileTableCleanupIsPending() {
    when(namespaceRepo.metaFor(NAMESPACE_ID)).thenReturn(metaAt(7L));
    when(tableCleanupRepo.hasAny(NAMESPACE_ID)).thenReturn(true);

    assertThrows(
        StatusRuntimeException.class,
        () ->
            svc.deleteNamespace(
                    DeleteNamespaceRequest.newBuilder().setNamespaceId(NAMESPACE_ID).build())
                .await()
                .indefinitely());

    verify(tableCleanupRepo, times(2)).hasAny(NAMESPACE_ID);
    verify(namespaceRepo, never()).deleteWithPrecondition(any(), anyLong(), any());
  }

  @Test
  void relocationRejectsADeepDescendantEvenWhenNoImmediateChildResolves() {
    when(namespaceRepo.hasAnyDescendantUnder("acct", "cat-1", List.of("db", "n"))).thenReturn(true);
    var desired = namespaceValue().toBuilder().setDisplayName("renamed").build();

    var failure =
        assertThrows(
            StatusRuntimeException.class,
            () -> svc.requireRelocationStrandsNothing(namespaceValue(), desired, true, "corr"));

    assertEquals(
        "namespace.children.would.strand", FloecatStatus.fromThrowable(failure).messageKey());
  }

  @Test
  void recursiveAndRequireEmptyReportsTheirDedicatedValidationError() {
    var failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                svc.deleteNamespace(
                        DeleteNamespaceRequest.newBuilder()
                            .setNamespaceId(NAMESPACE_ID)
                            .setRecursive(true)
                            .setRequireEmpty(true)
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, failure.getStatus().getCode());
    assertEquals(
        "namespace.recursive.require.empty.exclusive",
        FloecatStatus.fromThrowable(failure).messageKey());
  }

  @Test
  void deleteRejectsANamespaceFromAnotherTenantBeforeRepositoryAccess() {
    var foreignNamespace = NAMESPACE_ID.toBuilder().setAccountId("other-acct").build();

    var failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                svc.deleteNamespace(
                        DeleteNamespaceRequest.newBuilder()
                            .setNamespaceId(foreignNamespace)
                            .setRecursive(true)
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, failure.getStatus().getCode());
    verify(namespaceRepo, never()).metaForSafe(foreignNamespace);
    verify(recursiveDropper, never()).dropNamespaceContents(any(), anyLong(), any());
  }

  @Test
  void plainDeleteDrainsStaleTableCleanupBeforeItsEmptinessDecision() {
    when(namespaceRepo.metaFor(NAMESPACE_ID)).thenReturn(metaAt(7L));
    when(namespaceRepo.deleteWithPrecondition(any(), anyLong(), any())).thenReturn(true);
    when(tableCleanupRepo.hasAny(NAMESPACE_ID)).thenReturn(true, false);

    svc.deleteNamespace(DeleteNamespaceRequest.newBuilder().setNamespaceId(NAMESPACE_ID).build())
        .await()
        .indefinitely();

    verify(recursiveDropper).cleanupDeletedTablesInNamespace(NAMESPACE_ID);
    verify(namespaceRepo).deleteWithPrecondition(eq(NAMESPACE_ID), eq(7L), any());
  }

  /**
   * The pointer is live but names a blob that is not there. This is not an already-gone namespace:
   * the canonical pointer, the by-path row and the whole subtree are still present, so reporting
   * the idempotent success this used to fall through to told the caller a namespace had been
   * deleted while every trace of it remained. With the pointer unchanged, the blob is genuinely
   * absent and nothing can resolve what the namespace holds.
   */
  @Test
  void deleteRefusesWhenThePointerNamesABlobThatIsNotThere() {
    when(namespaceRepo.metaFor(NAMESPACE_ID)).thenReturn(metaAt(7L));
    when(namespaceRepo.getByBlobUri("blob://ns-1")).thenReturn(Optional.empty());

    var failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                svc.deleteNamespace(
                        DeleteNamespaceRequest.newBuilder()
                            .setNamespaceId(NAMESPACE_ID)
                            .setRecursive(true)
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.FAILED_PRECONDITION, failure.getStatus().getCode());
    verify(recursiveDropper, never()).dropNamespaceContents(any(), anyLong(), any());
    verify(namespaceRepo, never()).deleteWithPrecondition(any(), anyLong(), any());
  }

  /**
   * Same empty read, but the pointer has moved on: the blob it named was superseded and swept
   * between the two reads. That is ordinary, and the abort is retryable rather than terminal — so
   * what the caller sees is not a failure at all. The retry re-reads the pointer and works from the
   * blob it now names, which is the behaviour that distinguishes this from a genuinely absent blob.
   */
  @Test
  void deleteRetriesOntoTheNewBlobWhenTheOldOneWasSuperseded() {
    var superseded = new AtomicBoolean(false);
    when(namespaceRepo.getByBlobUri("blob://ns-1"))
        .thenAnswer(
            invocation -> {
              superseded.set(true);
              return Optional.empty();
            });
    when(namespaceRepo.metaForSafe(NAMESPACE_ID))
        .thenAnswer(invocation -> superseded.get() ? metaAt(8L, "blob://ns-1-next") : metaAt(7L));
    when(namespaceRepo.getByBlobUri("blob://ns-1-next")).thenReturn(Optional.of(namespaceValue()));
    when(namespaceRepo.metaFor(NAMESPACE_ID)).thenReturn(metaAt(8L, "blob://ns-1-next"));
    when(markerStore.advanceNamespaceMarker(eq(NAMESPACE_ID), anyLong())).thenReturn(true);
    when(markerStore.namespaceMarkerVersion(NAMESPACE_ID)).thenReturn(0L, 1L, 2L);
    when(namespaceRepo.deleteWithPrecondition(any(), anyLong(), any())).thenReturn(true);

    svc.deleteNamespace(
            DeleteNamespaceRequest.newBuilder()
                .setNamespaceId(NAMESPACE_ID)
                .setRecursive(true)
                .build())
        .await()
        .indefinitely();

    // The swept blob was read, and nothing was destroyed on the strength of it.
    verify(namespaceRepo).getByBlobUri("blob://ns-1");
    // The retry planned against the version the pointer had moved to, not the one it read first.
    verify(recursiveDropper).dropNamespaceContents(any(), eq(8L), any());
    verify(namespaceRepo).deleteWithPrecondition(eq(NAMESPACE_ID), eq(8L), any());
  }

  /** A root that has not moved proceeds, and the drop is pinned to the version that was read. */
  @Test
  void recursiveDeletePinsTheDropToTheVersionItRead() {
    when(namespaceRepo.metaFor(NAMESPACE_ID)).thenReturn(metaAt(7L));
    when(namespaceRepo.deleteWithPrecondition(any(), anyLong(), any())).thenReturn(true);

    svc.deleteNamespace(
            DeleteNamespaceRequest.newBuilder()
                .setNamespaceId(NAMESPACE_ID)
                .setRecursive(true)
                .build())
        .await()
        .indefinitely();

    verify(recursiveDropper).dropNamespaceContents(any(), eq(7L), any());
    // The final delete's CAS is pinned to that same version, not to a fresh read.
    verify(namespaceRepo).deleteWithPrecondition(eq(NAMESPACE_ID), eq(7L), any());
    // And the delete never moves the children marker. That is a publish-only write: doing it here
    // leaves a durable advance no child made, which a failed delete bequeaths to the next one.
    verify(markerStore, never()).advanceNamespaceMarker(any(), anyLong());
  }

  /**
   * The root counts itself the moment its own delete commits, not in the response. Everything after
   * that removal can throw — the marker delete, the cache evictions — and the root is already gone,
   * so counting it later left destroyed.total() at zero for a recursive delete of a leaf and sent
   * the failure through partialTeardownIfDestroyed unlabelled: "nothing committed", about a
   * namespace that had been removed.
   */
  @Test
  void aPostCommitMarkerFailureReportsCommittedDeletionCounts() {
    when(namespaceRepo.metaFor(NAMESPACE_ID)).thenReturn(metaAt(7L));
    when(namespaceRepo.deleteWithPrecondition(any(), anyLong(), any())).thenReturn(true);
    // The marker removal that follows the delete fails.
    doThrow(new RuntimeException("storage down"))
        .when(markerStore)
        .deleteNamespaceMarker(NAMESPACE_ID);

    var failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                svc.deleteNamespace(
                        DeleteNamespaceRequest.newBuilder()
                            .setNamespaceId(NAMESPACE_ID)
                            .setRecursive(true)
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.FAILED_PRECONDITION, failure.getStatus().getCode());
    var status = FloecatStatus.fromThrowable(failure);
    assertEquals("namespace.recursive.partial", status.messageKey());
    assertEquals("1", status.params().get("deleted_namespaces"));
  }

  /** A racing delete won the root CAS, so idempotent success must not count its work as ours. */
  @Test
  void aRecursiveDeleteDoesNotCountTheRootWhenAnotherDeleteWonTheCas() {
    when(namespaceRepo.metaFor(NAMESPACE_ID)).thenReturn(metaAt(7L));
    when(namespaceRepo.metaForSafe(NAMESPACE_ID))
        .thenReturn(metaAt(7L), metaAt(7L), metaAt(7L), metaAt(0L, ""));
    when(namespaceRepo.deleteWithPrecondition(any(), anyLong(), any())).thenReturn(false);

    var response =
        svc.deleteNamespace(
                DeleteNamespaceRequest.newBuilder()
                    .setNamespaceId(NAMESPACE_ID)
                    .setRecursive(true)
                    .build())
            .await()
            .indefinitely();

    assertEquals(0L, response.getMeta().getPointerVersion());
    assertEquals(0, response.getDeletedNamespaces());
    verify(markerStore).deleteNamespaceMarker(NAMESPACE_ID);
  }

  /** A plain delete reports its cleanup failure without inventing recursive-teardown counts. */
  @Test
  void aPlainDeleteDoesNotReportRecursivePartialWhenCleanupFailsAfterRemoval() {
    when(namespaceRepo.metaFor(NAMESPACE_ID)).thenReturn(metaAt(7L));
    when(namespaceRepo.deleteWithPrecondition(any(), anyLong(), any())).thenReturn(true);
    doThrow(new RuntimeException("storage down"))
        .when(markerStore)
        .deleteNamespaceMarker(NAMESPACE_ID);

    var failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                svc.deleteNamespace(
                        DeleteNamespaceRequest.newBuilder().setNamespaceId(NAMESPACE_ID).build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INTERNAL, failure.getStatus().getCode());
    assertFalse(
        failure.getStatus().getDescription().contains("1 namespace"),
        "plain delete must not be relabelled as recursive partial teardown");
  }
}
