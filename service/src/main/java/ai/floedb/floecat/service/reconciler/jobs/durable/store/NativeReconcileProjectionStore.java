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

import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobContribution;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.jboss.logging.Logger;

@ApplicationScoped
public class NativeReconcileProjectionStore implements ReconcileProjectionStore {
  private static final Logger LOG = Logger.getLogger(NativeReconcileProjectionStore.class);

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
    return projectionBackend.listContributions(accountId, parentJobId).stream()
        .map(ReconcileProjectionBackend.ContributionSnapshot::blobUri)
        .map(payloadStore::readInlineJobContribution)
        .flatMap(Optional::stream)
        .filter(
            contribution ->
                accountId.equals(contribution.accountId)
                    && parentJobId.equals(contribution.parentJobId))
        .collect(Collectors.toList());
  }

  @Override
  public boolean upsertContribution(StoredJobContribution contribution) {
    if (contribution == null
        || blank(contribution.accountId)
        || blank(contribution.parentJobId)
        || blank(contribution.childJobId)) {
      return false;
    }
    String blobUri = payloadStore.encodeInlineJobContribution(contribution);
    for (int i = 0; i < casMax; i++) {
      ReconcileProjectionBackend.ContributionSnapshot existing =
          projectionBackend
              .loadContribution(
                  contribution.accountId, contribution.parentJobId, contribution.childJobId)
              .orElse(null);
      if (existing != null && blobUri.equals(existing.blobUri())) {
        return true;
      }
      long expectedVersion = existing == null ? 0L : existing.version();
      if (projectionBackend.compareAndSetBatch(
          new ReconcileProjectionBackend.ProjectionWriteBatch(
              List.of(
                  new ReconcileProjectionBackend.ContributionUpsert(
                      contribution.accountId,
                      contribution.parentJobId,
                      contribution.childJobId,
                      expectedVersion,
                      blobUri))))) {
        return true;
      }
    }
    LOG.warnf(
        "Failed to upsert reconcile contribution account=%s parent=%s child=%s after CAS retries",
        contribution.accountId, contribution.parentJobId, contribution.childJobId);
    return false;
  }

  @Override
  public Optional<String> loadFileGroupResultReference(String accountId, String jobId) {
    if (blank(accountId) || blank(jobId)) {
      return Optional.empty();
    }
    return projectionBackend
        .loadResultReference(accountId, jobId)
        .map(ReconcileProjectionBackend.ResultReferenceSnapshot::blobUri)
        .filter(blobUri -> !blank(blobUri));
  }

  @Override
  public boolean upsertFileGroupResultReference(String accountId, String jobId, String blobUri) {
    if (blank(accountId) || blank(jobId) || blank(blobUri)) {
      return false;
    }
    for (int i = 0; i < casMax; i++) {
      ReconcileProjectionBackend.ResultReferenceSnapshot existing =
          projectionBackend.loadResultReference(accountId, jobId).orElse(null);
      if (existing != null && blobUri.equals(existing.blobUri())) {
        return true;
      }
      long expectedVersion = existing == null ? 0L : existing.version();
      if (projectionBackend.compareAndSetBatch(
          new ReconcileProjectionBackend.ProjectionWriteBatch(
              List.of(
                  new ReconcileProjectionBackend.ResultReferenceUpsert(
                      accountId, jobId, expectedVersion, blobUri))))) {
        return true;
      }
    }
    LOG.warnf(
        "Failed to upsert reconcile file-group result reference account=%s job=%s after CAS retries",
        accountId, jobId);
    return false;
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
