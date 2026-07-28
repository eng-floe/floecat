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

package ai.floedb.floecat.service.repo.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.RepoTestPointerStores.DelegatingPointerStore;
import ai.floedb.floecat.service.repo.impl.RepoTestPointerStores.DuplicateKeyRejectingPointerStore;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The namespace child fence: publishing a child and deleting its namespace are mutually exclusive.
 *
 * <p>Each test drives the exact interleaving the fence exists for, rather than racing threads: the
 * deleter's emptiness scan is a read and can never join a CAS batch, so the window it leaves is
 * closed only by the two sides contending on the children marker inside their own batches. These
 * tests assert that contention directly, which is both deterministic and the actual invariant.
 */
class NamespaceChildFenceTest {

  private static final String ACCOUNT = "acct-1";
  private static final String CATALOG = "cat-1";
  private static final String NAMESPACE = "ns-1";

  private InMemoryPointerStore ptr;
  private InMemoryBlobStore blobs;
  private MarkerStore markers;
  private NamespaceRepository namespaceRepo;
  private ViewRepository viewRepo;
  private ResourceId namespaceId;

  @BeforeEach
  void setUp() {
    ptr = new InMemoryPointerStore();
    blobs = new InMemoryBlobStore();
    markers = new MarkerStore();
    markers.pointerStore = ptr;
    namespaceRepo = new NamespaceRepository(ptr, blobs);
    viewRepo = new ViewRepository(ptr, blobs);

    namespaceId = resourceId(NAMESPACE, ResourceKind.RK_NAMESPACE);
    namespaceRepo.create(
        Namespace.newBuilder()
            .setResourceId(namespaceId)
            .setCatalogId(resourceId(CATALOG, ResourceKind.RK_CATALOG))
            .setDisplayName("sales")
            .build());
  }

  private static ResourceId resourceId(String id, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId(ACCOUNT).setId(id).setKind(kind).build();
  }

  private static View view(String id, String name) {
    return View.newBuilder()
        .setResourceId(resourceId(id, ResourceKind.RK_VIEW))
        .setCatalogId(resourceId(CATALOG, ResourceKind.RK_CATALOG))
        .setNamespaceId(resourceId(NAMESPACE, ResourceKind.RK_NAMESPACE))
        .setDisplayName(name)
        .build();
  }

  private long markerVersion() {
    return markers.namespaceMarkerVersion(namespaceId);
  }

  private boolean namespaceExists() {
    return namespaceRepo.getById(namespaceId).isPresent();
  }

  @Test
  void childCreateAdvancesTheMarkerInsideItsOwnBatch() {
    long before = markerVersion();

    viewRepo.create(view("view-1", "orders"), markers.namespaceChildGuard(namespaceId));

    assertThat(viewRepo.getById(resourceId("view-1", ResourceKind.RK_VIEW))).isPresent();
    // The marker moved as part of the create, not after it — that ordering is the whole fence.
    assertThat(markerVersion()).isEqualTo(before + 1);
  }

  @Test
  void deleteFailsWhenAChildWasPublishedAfterTheEmptinessScan() {
    // Deleter: scan finds the namespace empty, then takes ownership of the marker exactly as
    // DeleteNamespace does before its final checks.
    long scanned = markerVersion();
    assertThat(markers.advanceNamespaceMarker(namespaceId, scanned)).isTrue();
    var deleteGuard = markers.namespaceDeleteGuard(namespaceId, scanned + 1);

    // Creator slips in after that scan — precisely the window that used to orphan the view.
    viewRepo.create(view("view-1", "orders"), markers.namespaceChildGuard(namespaceId));

    assertThatThrownBy(() -> namespaceRepo.delete(namespaceId, deleteGuard))
        .isInstanceOf(BaseResourceRepository.BatchGuardFailedException.class)
        .hasMessageContaining(NAMESPACE);

    // Neither side is orphaned: the view is alive and so is the namespace holding it.
    assertThat(namespaceExists()).isTrue();
    assertThat(viewRepo.getById(resourceId("view-1", ResourceKind.RK_VIEW))).isPresent();
  }

  @Test
  void childCreateFailsWhenTheNamespaceWasDeletedFirst() {
    // Creator resolves the namespace and captures its fence...
    var childGuard = markers.namespaceChildGuard(namespaceId);

    // ...but the delete commits first.
    long scanned = markerVersion();
    assertThat(
            namespaceRepo.delete(namespaceId, markers.namespaceDeleteGuard(namespaceId, scanned)))
        .isTrue();

    assertThatThrownBy(() -> viewRepo.create(view("view-1", "orders"), childGuard))
        .isInstanceOf(BaseResourceRepository.BatchGuardFailedException.class)
        .hasMessageContaining(NAMESPACE);

    // No orphan: the view never became visible under the deleted namespace, by id or by name.
    assertThat(viewRepo.getById(resourceId("view-1", ResourceKind.RK_VIEW))).isEmpty();
    assertThat(ptr.get(Keys.viewPointerByName(ACCOUNT, CATALOG, NAMESPACE, "orders"))).isEmpty();
    assertThat(ptr.get(Keys.relationPointerByName(ACCOUNT, CATALOG, NAMESPACE, "orders")))
        .isEmpty();
  }

  @Test
  void guardIsRefusedOutrightForAnAlreadyDeletedNamespace() {
    namespaceRepo.delete(namespaceId);

    // A zero pointer version must never degrade to "check absent" and let the child through.
    assertThatThrownBy(() -> markers.namespaceChildGuard(namespaceId))
        .isInstanceOf(BaseResourceRepository.BatchGuardFailedException.class)
        .hasMessageContaining("no longer exists");
  }

  @Test
  void siblingCreatesContendOnTheMarkerButBothStillCommit() {
    // Both creators capture the fence at the same marker version, so the second one's batch
    // necessarily loses. Benign contention must be absorbed by a re-read, not surfaced as failure —
    // otherwise fencing would make concurrent DDL in one namespace fail spuriously.
    var firstGuard = markers.namespaceChildGuard(namespaceId);
    var secondGuard = markers.namespaceChildGuard(namespaceId);

    viewRepo.create(view("view-1", "orders"), firstGuard);
    assertThatCode(() -> viewRepo.create(view("view-2", "returns"), secondGuard))
        .doesNotThrowAnyException();

    assertThat(viewRepo.getById(resourceId("view-1", ResourceKind.RK_VIEW))).isPresent();
    assertThat(viewRepo.getById(resourceId("view-2", ResourceKind.RK_VIEW))).isPresent();
    assertThat(markerVersion()).isEqualTo(2L);
  }

  @Test
  void nameCollisionIsReportedAsSuchEvenWhileTheMarkerIsContended() {
    // A create that both collides on its name AND keeps losing the fence. Occasional contention
    // self-corrects on the next attempt, so this reproduces the case that does not: a namespace
    // busy
    // enough that the marker moves before every attempt. Asking the guard first then burned the
    // whole
    // retry budget and reported guard contention, so the client never learned the name was taken.
    // The name's owner is created without contention; only the colliding create faces the busy
    // namespace, or the setup would exhaust its own retry budget too.
    viewRepo.create(view("view-1", "orders"), markers.namespaceChildGuard(namespaceId));

    var contendedViews =
        new ViewRepository(new MarkerMovingPointerStore(ptr, this::bumpMarkerAsIfBySibling), blobs);

    assertThatThrownBy(
            () ->
                contendedViews.create(
                    view("view-2", "orders"), markers.namespaceChildGuard(namespaceId)))
        .isInstanceOf(BaseResourceRepository.NameConflictException.class)
        .hasMessageContaining("pointer bound to different blob");
  }

  private void bumpMarkerAsIfBySibling() {
    markers.bumpNamespaceMarker(namespaceId);
  }

  /** Advances the children marker before every batch, as a namespace under constant DDL would. */
  private static final class MarkerMovingPointerStore extends DelegatingPointerStore {
    private final Runnable moveMarker;

    private MarkerMovingPointerStore(PointerStore delegate, Runnable moveMarker) {
      super(delegate);
      this.moveMarker = moveMarker;
    }

    @Override
    public boolean compareAndSetBatch(List<PointerStore.CasOp> ops) {
      moveMarker.run();
      return super.compareAndSetBatch(ops);
    }
  }

  @Test
  void deletionOfTheNamespaceOutranksACollisionInsideIt() {
    // Both would refuse the create, but the parent's fate is the more fundamental fact: the guarded
    // namespace is gone, so a retry re-resolves it and reports NOT_FOUND rather than a name that is
    // about to cease to exist.
    viewRepo.create(view("view-1", "orders"), markers.namespaceChildGuard(namespaceId));

    var guard = markers.namespaceChildGuard(namespaceId);
    assertThat(namespaceRepo.delete(namespaceId)).isTrue();

    assertThatThrownBy(() -> viewRepo.create(view("view-2", "orders"), guard))
        .isInstanceOf(BaseResourceRepository.BatchGuardFailedException.class);
  }

  @Test
  void deleteCommitsWhenNothingWasPublishedSinceTheScan() {
    long scanned = markerVersion();

    assertThat(
            namespaceRepo.delete(namespaceId, markers.namespaceDeleteGuard(namespaceId, scanned)))
        .isTrue();
    assertThat(namespaceExists()).isFalse();
  }

  @Test
  void guardCheckOnAKeyTheMutationAlreadyPinsIsDroppedFromTheBatch() {
    // A namespace republished under a parent path that resolves to itself puts the same pointer key
    // in the batch twice — once as the update's own CAS, once as the guard's check. DynamoDB
    // rejects
    // duplicate keys in a transaction, so the redundant check has to be dropped rather than sent.
    var selfGuard = markers.namespaceChildGuard(namespaceId);
    var meta = namespaceRepo.metaFor(namespaceId);

    var moved =
        namespaceRepo.getById(namespaceId).orElseThrow().toBuilder()
            .setDescription("renamed in place")
            .build();

    var rejectingDuplicates = new DuplicateKeyRejectingPointerStore(ptr);
    var guardedRepo = new NamespaceRepository(rejectingDuplicates, blobs);

    assertThatCode(() -> guardedRepo.update(moved, meta.getPointerVersion(), selfGuard))
        .doesNotThrowAnyException();
  }

  @Test
  void deleteGuardToleratesANamespaceThatNeverHadAChild() {
    // No child was ever published, so the marker pointer does not exist at all; the fence has to
    // express "still absent" rather than a version, which CasCheck cannot represent.
    assertThat(ptr.get(Keys.namespaceChildrenMarker(ACCOUNT, NAMESPACE))).isEmpty();

    assertThat(namespaceRepo.delete(namespaceId, markers.namespaceDeleteGuard(namespaceId, 0L)))
        .isTrue();
    assertThat(namespaceExists()).isFalse();
  }
}
