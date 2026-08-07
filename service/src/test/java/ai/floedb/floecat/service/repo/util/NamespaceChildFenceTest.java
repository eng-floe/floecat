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
import ai.floedb.floecat.service.repo.model.PointerReferences;
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
    // Deleter: reads the marker and scans, exactly as DeleteNamespace does. It does not advance the
    // marker — only a publish moves it, and the delete only needs the version its scan ran against.
    long scanned = markerVersion();
    var deleteGuard = markers.namespaceChildrenUnchangedGuard(namespaceId, scanned);

    // Creator slips in after that scan — precisely the window that used to orphan the view.
    viewRepo.create(view("view-1", "orders"), markers.namespaceChildGuard(namespaceId));

    assertThatThrownBy(() -> namespaceRepo.delete(namespaceId, deleteGuard))
        .isInstanceOf(BaseResourceRepository.BatchGuardFailedException.class)
        .hasMessageContaining(NAMESPACE);

    // Neither side is orphaned: the view is alive and so is the namespace holding it.
    assertThat(namespaceExists()).isTrue();
    assertThat(viewRepo.getById(resourceId("view-1", ResourceKind.RK_VIEW))).isPresent();
  }

  /**
   * A relocation carries the same guard for the same reason. Renaming a namespace moves only its
   * own by-path row; a child indexed beneath the old path does not follow, and once the parent name
   * is vacated nothing resolves that prefix — so a child published after the relocation scan is
   * unreachable rather than merely misplaced. The scan cannot join the batch, so the marker has to.
   */
  @Test
  void relocationFailsWhenAChildWasPublishedAfterTheStrandingScan() {
    // Renamer: the scan finds no children to strand, and captures the marker it decided that on.
    long scanned = markerVersion();
    var renameGuard = markers.namespaceChildrenUnchangedGuard(namespaceId, scanned);

    // Creator publishes into the namespace after that scan, advancing the marker in its own batch.
    viewRepo.create(view("view-1", "orders"), markers.namespaceChildGuard(namespaceId));

    var renamed =
        namespaceRepo.getById(namespaceId).orElseThrow().toBuilder()
            .setDisplayName("sales-2026")
            .build();
    long version = namespaceRepo.metaFor(namespaceId).getPointerVersion();

    assertThatThrownBy(() -> namespaceRepo.update(renamed, version, renameGuard))
        .isInstanceOf(BaseResourceRepository.BatchGuardFailedException.class)
        .hasMessageContaining(NAMESPACE);

    // The namespace kept the name its child is indexed under, so the child is still reachable.
    assertThat(namespaceRepo.getById(namespaceId).orElseThrow().getDisplayName())
        .isEqualTo("sales");
    assertThat(viewRepo.getById(resourceId("view-1", ResourceKind.RK_VIEW))).isPresent();
  }

  /** With no child published in the window, the same guarded relocation commits. */
  @Test
  void relocationCommitsWhenNoChildAppearedInTheWindow() {
    long scanned = markerVersion();
    var renameGuard = markers.namespaceChildrenUnchangedGuard(namespaceId, scanned);

    var renamed =
        namespaceRepo.getById(namespaceId).orElseThrow().toBuilder()
            .setDisplayName("sales-2026")
            .build();
    long version = namespaceRepo.metaFor(namespaceId).getPointerVersion();

    assertThat(namespaceRepo.update(renamed, version, renameGuard)).isTrue();
    assertThat(namespaceRepo.getById(namespaceId).orElseThrow().getDisplayName())
        .isEqualTo("sales-2026");
  }

  @Test
  void childCreateFailsWhenTheNamespaceWasDeletedFirst() {
    // Creator resolves the namespace and captures its fence...
    var childGuard = markers.namespaceChildGuard(namespaceId);

    // ...but the delete commits first.
    long scanned = markerVersion();
    assertThat(
            namespaceRepo.delete(
                namespaceId, markers.namespaceChildrenUnchangedGuard(namespaceId, scanned)))
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
  void guardRefusesThePostMovePointerVersionForAPreMoveMembershipCheck() {
    String pointerKey = Keys.namespacePointerById(ACCOUNT, NAMESPACE);
    String resolvedBlob = ptr.get(pointerKey).orElseThrow().getBlobUri();
    Namespace moved =
        namespaceRepo.getById(namespaceId).orElseThrow().toBuilder()
            .setCatalogId(resourceId("cat-2", ResourceKind.RK_CATALOG))
            .build();
    assertThat(namespaceRepo.update(moved, 1L)).isTrue();

    assertThatThrownBy(() -> markers.namespaceChildGuard(namespaceId, resolvedBlob))
        .isInstanceOf(BaseResourceRepository.BatchGuardFailedException.class)
        .hasMessageContaining("changed after it was resolved");
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
    // The same move a real publisher makes, through the same API: read the marker, advance it.
    markers.advanceNamespaceMarker(namespaceId, markers.namespaceMarkerVersion(namespaceId));
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
            namespaceRepo.delete(
                namespaceId, markers.namespaceChildrenUnchangedGuard(namespaceId, scanned)))
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

    assertThat(
            namespaceRepo.delete(
                namespaceId, markers.namespaceChildrenUnchangedGuard(namespaceId, 0L)))
        .isTrue();
    assertThat(namespaceExists()).isFalse();
  }

  /**
   * Account teardown's mirror image of every other guard here: it does not pin the things it is
   * removing — they are all going — but it does rest on the account being gone, and account ids are
   * caller-supplied and reusable. So "still absent" has to be a precondition the removals carry,
   * not a read taken before them.
   */
  @Test
  void anAbsenceGuardCommitsWhileTheRowIsGoneAndRefusesOnceItIsBack() {
    String accountKey = Keys.accountPointerById(ACCOUNT);
    var accountGone = markers.pointerAbsentGuard("account " + ACCOUNT, accountKey);
    assertThat(ptr.get(accountKey)).isEmpty();

    viewRepo.create(view("view-1", "orders"));
    assertThat(viewRepo.delete(resourceId("view-1", ResourceKind.RK_VIEW), accountGone)).isTrue();

    // The id is created again — an undelete, or a fresh account reusing it — while the same sweep
    // carries on. Its next removal must not commit.
    viewRepo.create(view("view-2", "returns"));
    ptr.compareAndSet(
        accountKey, 0L, PointerReferences.opaqueMarkerPointer(accountKey, accountKey, 1L));

    assertThatThrownBy(
            () -> viewRepo.delete(resourceId("view-2", ResourceKind.RK_VIEW), accountGone))
        .isInstanceOf(BaseResourceRepository.BatchGuardFailedException.class)
        .hasMessageContaining(ACCOUNT);
    // The new account keeps what it owns.
    assertThat(viewRepo.getById(resourceId("view-2", ResourceKind.RK_VIEW))).isPresent();
  }

  /**
   * Version 0 means absent everywhere else in MarkerStore, so a guard pinned to it asserts absence
   * rather than being rejected by CasCheck's constructor. Unreachable through today's callers,
   * which all refuse a zero version before building a guard — this is what keeps a future one from
   * getting an IllegalArgumentException where a clean BROKEN belongs.
   */
  @Test
  void aGuardPinnedToVersionZeroAssertsAbsenceRatherThanThrowing() {
    String accountKey = Keys.accountPointerById(ACCOUNT);

    var absent = markers.pointerPinnedGuard("account " + ACCOUNT, accountKey, 0L);
    assertThat(absent.ops()).singleElement().isInstanceOf(PointerStore.CasCheckAbsent.class);
    assertThat(absent.reevaluate()).isEqualTo(BatchGuard.Outcome.HOLDS);

    // Same for the namespace flavour — and here the row does exist, so "pinned at absent" is broken
    // rather than held. Still an outcome, not an exception.
    var pinnedToAbsent = markers.namespacePinnedGuard(namespaceId, 0L);
    assertThat(pinnedToAbsent.ops())
        .singleElement()
        .isInstanceOf(PointerStore.CasCheckAbsent.class);
    assertThat(pinnedToAbsent.reevaluate()).isEqualTo(BatchGuard.Outcome.BROKEN);
  }
}
