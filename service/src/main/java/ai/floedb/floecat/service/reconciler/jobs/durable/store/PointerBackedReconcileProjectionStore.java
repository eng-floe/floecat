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

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobContribution;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.service.repo.model.Keys;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import org.jboss.logging.Logger;

@ApplicationScoped
public class PointerBackedReconcileProjectionStore implements ReconcileProjectionStore {
  private static final Logger LOG = Logger.getLogger(PointerBackedReconcileProjectionStore.class);

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
    String prefix = Keys.reconcileJobContributionPointerPrefix(accountId, parentJobId);
    List<StoredJobContribution> out = new ArrayList<>();
    String token = "";
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers = projectionBackend.listPointersByPrefix(prefix, 256, token, next);
      if (pointers.isEmpty()) {
        break;
      }
      for (Pointer pointer : pointers) {
        readContribution(pointer)
            .filter(
                contribution ->
                    accountId.equals(contribution.accountId)
                        && parentJobId.equals(contribution.parentJobId))
            .ifPresent(out::add);
      }
      token = next.toString();
      if (token.isBlank()) {
        break;
      }
    }
    return out;
  }

  @Override
  public boolean upsertContribution(StoredJobContribution contribution) {
    if (contribution == null
        || blank(contribution.accountId)
        || blank(contribution.parentJobId)
        || blank(contribution.childJobId)) {
      return false;
    }
    String pointerKey =
        Keys.reconcileJobContributionPointer(
            contribution.accountId, contribution.parentJobId, contribution.childJobId);
    return upsertReferencePointer(
        pointerKey, payloadStore.encodeInlineJobContribution(contribution));
  }

  @Override
  public boolean upsertFileGroupResultReference(String accountId, String jobId, String blobUri) {
    if (blank(accountId) || blank(jobId) || blank(blobUri)) {
      return false;
    }
    return upsertReferencePointer(Keys.reconcileJobResultPointerById(accountId, jobId), blobUri);
  }

  private boolean upsertReferencePointer(String pointerKey, String reference) {
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
    LOG.warnf("Failed to upsert reconcile projection pointer key=%s after CAS retries", pointerKey);
    return false;
  }

  private java.util.Optional<StoredJobContribution> readContribution(Pointer pointer) {
    if (pointer == null || blank(pointer.getBlobUri())) {
      return java.util.Optional.empty();
    }
    return payloadStore.readInlineJobContribution(pointer.getBlobUri());
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
