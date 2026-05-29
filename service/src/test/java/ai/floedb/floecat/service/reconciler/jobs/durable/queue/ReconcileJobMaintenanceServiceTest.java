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

package ai.floedb.floecat.service.reconciler.jobs.durable.queue;

import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeasedJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobLease;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobExecutionLoader;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileLeaseStateCodec;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.CanonicalPointerSnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.IntToLongFunction;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

class ReconcileJobMaintenanceServiceTest {
  @Test
  void refreshDirtyParentsAdvancesPaginationTokenUnderChurn() {
    PointerStore pointerStore = new TestPointerStore();
    ReconcileJobMaintenanceService service = new ReconcileJobMaintenanceService();
    List<String> refreshed = new ArrayList<>();
    AtomicInteger churnCount = new AtomicInteger();

    putDirtyMarker(pointerStore, "acct", "a");
    putDirtyMarker(pointerStore, "acct", "z");

    service.bind(
        new NoopLeaseStore(),
        pointerStore,
        (entry, nowMs) -> {},
        (accountId, parentJobId) -> {
          refreshed.add(parentJobId);
          if (parentJobId.startsWith("a") && churnCount.getAndIncrement() < 100) {
            String nextParentId = "a" + String.format("%03d", churnCount.get());
            putDirtyMarker(pointerStore, accountId, nextParentId);
          }
        },
        1,
        5_000L);

    service.runProjectionMaintenanceOnce(200L);

    assertTrue(refreshed.contains("a"));
    assertTrue(
        refreshed.contains("z"),
        "dirty-parent refresh should reach later markers even when earlier markers keep adding"
            + " more work");
  }

  private static void putDirtyMarker(
      PointerStore pointerStore, String accountId, String parentJobId) {
    String key = Keys.reconcileDirtyParentPointer(accountId, parentJobId);
    String payload = accountId + "\n" + parentJobId;
    long nextVersion = pointerStore.get(key).map(Pointer::getVersion).orElse(0L) + 1L;
    pointerStore.compareAndSet(
        key,
        nextVersion - 1L,
        Pointer.newBuilder().setKey(key).setBlobUri(payload).setVersion(nextVersion).build());
  }

  private static final class TestPointerStore implements PointerStore {
    private final Map<String, Pointer> pointers =
        Collections.synchronizedSortedMap(new TreeMap<>());

    @Override
    public Optional<Pointer> get(String key) {
      return Optional.ofNullable(pointers.get(key));
    }

    @Override
    public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
      Pointer current = pointers.get(key);
      long currentVersion = current == null ? 0L : current.getVersion();
      if (currentVersion != expectedVersion) {
        return false;
      }
      pointers.put(key, next.toBuilder().setKey(key).setVersion(expectedVersion + 1L).build());
      return true;
    }

    @Override
    public boolean delete(String key) {
      return pointers.remove(key) != null;
    }

    @Override
    public boolean compareAndDelete(String key, long expectedVersion) {
      Pointer current = pointers.get(key);
      if (current == null || current.getVersion() != expectedVersion) {
        return false;
      }
      pointers.remove(key);
      return true;
    }

    @Override
    public boolean compareAndSetBatch(List<CasOp> ops) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Pointer> listPointersByPrefix(
        String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
      String effectivePrefix = prefix == null ? "" : prefix;
      List<String> keys = new ArrayList<>();
      synchronized (pointers) {
        for (String key : pointers.keySet()) {
          if (key.startsWith(effectivePrefix)
              && (pageToken == null || pageToken.isBlank() || key.compareTo(pageToken) > 0)) {
            keys.add(key);
          }
        }
      }
      int end = Math.min(keys.size(), Math.max(1, limit));
      List<Pointer> page = new ArrayList<>(end);
      for (int i = 0; i < end; i++) {
        page.add(pointers.get(keys.get(i)));
      }
      if (nextTokenOut != null) {
        nextTokenOut.setLength(0);
        if (end < keys.size()) {
          nextTokenOut.append(keys.get(end - 1));
        }
      }
      return page;
    }

    @Override
    public int deleteByPrefix(String prefix) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int countByPrefix(String prefix) {
      int count = 0;
      String effectivePrefix = prefix == null ? "" : prefix;
      synchronized (pointers) {
        for (String key : pointers.keySet()) {
          if (key.startsWith(effectivePrefix)) {
            count++;
          }
        }
      }
      return count;
    }

    @Override
    public boolean isEmpty() {
      return pointers.isEmpty();
    }
  }

  private static final class NoopLeaseStore implements ReconcileLeaseStore {
    @Override
    public void bind(
        ReconcileLeaseBackend leaseBackend,
        ReconcileJobExecutionLoader executionLoader,
        ReconcileLeaseStateCodec leaseStateCodec,
        int casMax,
        long leaseMs,
        long leaseRenewGraceMs,
        ReconcileJobIndexStore jobIndexStore,
        CanonicalJobMutator mutateCanonicalJob,
        Predicate<String> isTerminalState,
        BiConsumer<StoredReconcileJob, StoredReconcileJob> assertImmutableJobIdentityPreserved,
        int maxAttempts,
        IntToLongFunction backoffMs) {}

    @Override
    public Optional<LeasedJob> leaseCanonical(
        String canonicalPointerKey,
        String readyPointerKey,
        long now,
        CanonicalPointerSnapshot initialSnapshot,
        StoredReconcileJob initialRecord) {
      return Optional.empty();
    }

    @Override
    public boolean hasActiveLease(
        String jobId,
        String leaseEpoch,
        StoredReconcileJob current,
        String context,
        boolean allowWaitingState,
        boolean logMissingLease,
        boolean allowExpiredWithinGrace) {
      return false;
    }

    @Override
    public boolean hasLiveLease(StoredReconcileJob record, boolean allowCancelling, long now) {
      return false;
    }

    @Override
    public Optional<StoredJobLease> loadLease(String accountId, String jobId) {
      return Optional.empty();
    }

    @Override
    public Optional<StoredJobLease> loadLease(StoredReconcileJob record) {
      return Optional.empty();
    }

    @Override
    public Optional<StoredJobLease> mutateLease(
        String accountId, String jobId, UnaryOperator<StoredJobLease> mutator) {
      return Optional.empty();
    }

    @Override
    public Optional<StoredJobLease> renewLeaseIfEpochMatches(
        String accountId, String jobId, String leaseEpoch) {
      return Optional.empty();
    }

    @Override
    public LeaseExpiryScanPage scanExpiredLeasePointersPage(
        long nowMs, int pageSize, String pageToken) {
      return new LeaseExpiryScanPage(List.of(), "");
    }

    @Override
    public void reclaimExpiredLease(LeaseExpiryEntry leaseExpiryEntry, long nowMs) {}

    @Override
    public boolean clearLeaseIfEpochMatches(String accountId, String jobId, String leaseEpoch) {
      return false;
    }

    @Override
    public boolean tryAcquireLaneLease(
        StoredReconcileJob record, String canonicalPointerKey, long nowMs) {
      return false;
    }

    @Override
    public void clearLaneLeaseIfOwned(StoredReconcileJob record, String expectedReference) {}

    @Override
    public boolean tryAcquireSnapshotLease(
        StoredReconcileJob record, String canonicalPointerKey, long nowMs) {
      return false;
    }

    @Override
    public void clearSnapshotLeaseIfOwned(StoredReconcileJob record, String expectedReference) {}

    @Override
    public String leaseExpiryPointerKey(StoredJobLease lease) {
      return "";
    }

    @Override
    public String leaseExpiryPointerKey(long expiresAtMs, String accountId, String jobId) {
      return "";
    }
  }
}
