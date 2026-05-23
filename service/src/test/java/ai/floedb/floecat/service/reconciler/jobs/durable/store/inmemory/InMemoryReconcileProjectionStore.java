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

package ai.floedb.floecat.service.reconciler.jobs.durable.store.inmemory;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobContribution;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileProjectionBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileProjectionStore;
import ai.floedb.floecat.service.repo.model.Keys;
import java.util.List;
import org.jboss.logging.Logger;

/**
 * Test-scope adapter that uses composition rather than inheriting the production implementation
 * directly.
 */
public final class InMemoryReconcileProjectionStore implements ReconcileProjectionStore {
  private static final Logger LOG = Logger.getLogger(InMemoryReconcileProjectionStore.class);

  private final InMemoryReconcileProjectionState state = new InMemoryReconcileProjectionState();

  private ReconcileProjectionBackend projectionBackend;
  private ReconcilePayloadStore payloadStore;
  private int casMax;

  @Override
  public void bind(
      ReconcileProjectionBackend projectionBackend,
      ReconcilePayloadStore payloadStore,
      int casMax) {
    this.projectionBackend = projectionBackend;
    this.payloadStore = payloadStore;
    this.casMax = casMax;
  }

  @Override
  public List<StoredJobContribution> loadDirectContributions(String accountId, String parentJobId) {
    if (blank(accountId) || blank(parentJobId)) {
      return List.of();
    }
    return state.loadDirectContributions(accountId, parentJobId);
  }

  @Override
  public boolean upsertContribution(StoredJobContribution contribution) {
    if (contribution == null
        || blank(contribution.accountId)
        || blank(contribution.parentJobId)
        || blank(contribution.childJobId)) {
      return false;
    }
    state.upsertContribution(contribution);
    return upsertMirroredPointer(
        Keys.reconcileJobContributionPointer(
            contribution.accountId, contribution.parentJobId, contribution.childJobId),
        payloadStore.encodeInlineJobContribution(contribution));
  }

  @Override
  public boolean upsertFileGroupResultReference(String accountId, String jobId, String blobUri) {
    if (blank(accountId) || blank(jobId) || blank(blobUri)) {
      return false;
    }
    state.upsertResultBlobUri(accountId, jobId, blobUri);
    return upsertMirroredPointer(Keys.reconcileJobResultPointerById(accountId, jobId), blobUri);
  }

  private boolean upsertMirroredPointer(String pointerKey, String reference) {
    for (int i = 0; i < casMax; i++) {
      Pointer existing = projectionBackend.loadPointer(pointerKey).orElse(null);
      if (existing == null) {
        Pointer created =
            Pointer.newBuilder().setKey(pointerKey).setBlobUri(reference).setVersion(1L).build();
        if (projectionBackend.compareAndSetBatch(
            new ReconcileProjectionBackend.ProjectionWriteBatch(
                List.of(
                    new ReconcileProjectionBackend.ProjectionUpsert(
                        pointerKey, 0L, created.getBlobUri()))))) {
          return true;
        }
        continue;
      }
      if (reference.equals(existing.getBlobUri())) {
        return true;
      }
      Pointer next =
          Pointer.newBuilder()
              .setKey(pointerKey)
              .setBlobUri(reference)
              .setVersion(existing.getVersion() + 1L)
              .build();
      if (projectionBackend.compareAndSetBatch(
          new ReconcileProjectionBackend.ProjectionWriteBatch(
              List.of(
                  new ReconcileProjectionBackend.ProjectionUpsert(
                      pointerKey, existing.getVersion(), next.getBlobUri()))))) {
        return true;
      }
    }
    LOG.warnf("Failed to mirror in-memory reconcile projection pointer key=%s", pointerKey);
    return false;
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
