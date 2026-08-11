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

package ai.floedb.floecat.service.reconciler.jobs.durable.projection;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class ReconcileJobProjectionStore {
  private static final int CAS_MAX = 8;

  public record ProjectionSnapshot(StoredReconcileJobProjection projection, long pointerVersion) {}

  private PointerStore pointerStore;
  private ReconcilePayloadStore payloadStore;
  private ReconcileJobIndexStore jobIndexStore;

  public void bind(
      PointerStore pointerStore,
      ReconcilePayloadStore payloadStore,
      ReconcileJobIndexStore jobIndexStore) {
    this.pointerStore = pointerStore;
    this.payloadStore = payloadStore;
    this.jobIndexStore = jobIndexStore;
  }

  public Optional<StoredReconcileJobProjection> load(String accountId, String jobId) {
    return Optional.ofNullable(loadSnapshot(accountId, jobId).projection());
  }

  public ProjectionSnapshot loadSnapshot(String accountId, String jobId) {
    if (blank(accountId) || blank(jobId)) {
      return new ProjectionSnapshot(null, 0L);
    }
    Pointer pointer =
        pointerStore.get(Keys.reconcileJobProjectionPointer(accountId, jobId)).orElse(null);
    if (pointer == null) {
      return new ProjectionSnapshot(null, 0L);
    }
    return new ProjectionSnapshot(
        payloadStore.readInlineJobProjection(pointer.getBlobUri()).orElse(null),
        pointer.getVersion());
  }

  public void upsert(StoredReconcileJobProjection projection) {
    if (projection == null || blank(projection.accountId()) || blank(projection.jobId())) {
      return;
    }
    String key = Keys.reconcileJobProjectionPointer(projection.accountId(), projection.jobId());
    String blobUri = payloadStore.encodeInlineJobProjection(projection);
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer current = pointerStore.get(key).orElse(null);
      long expectedVersion = current == null ? 0L : current.getVersion();
      StoredReconcileJobProjection currentProjection =
          current == null
              ? null
              : payloadStore.readInlineJobProjection(current.getBlobUri()).orElse(null);
      if (currentProjection != null
          && currentProjection.appliedGeneration() > projection.appliedGeneration()) {
        return;
      }
      // Same-generation writers converge via last-writer-wins.
      if (current != null && blobUri.equals(current.getBlobUri())) {
        return;
      }
      Pointer next = PointerReferences.inlineJsonPointer(key, blobUri, expectedVersion + 1L);
      if (pointerStore.compareAndSet(key, expectedVersion, next)) {
        return;
      }
    }
    throw new IllegalStateException(
        "Failed to upsert reconcile job projection for job " + projection.jobId());
  }

  public boolean upsertWithCanonicalMutation(
      ProjectionSnapshot expected,
      StoredReconcileJobProjection projection,
      ReconcileJobIndexStore.JobIndexWriteBatch canonicalMutation,
      List<PointerStore.CasOp> additionalPointerOps) {
    if (projection == null || blank(projection.accountId()) || blank(projection.jobId())) {
      return true;
    }
    String key = Keys.reconcileJobProjectionPointer(projection.accountId(), projection.jobId());
    String blobUri = payloadStore.encodeInlineJobProjection(projection);
    long expectedVersion = expected == null ? 0L : expected.pointerVersion();
    java.util.ArrayList<PointerStore.CasOp> pointerOps = new java.util.ArrayList<>();
    pointerOps.add(
        new PointerStore.CasUpsert(
            key,
            expectedVersion,
            PointerReferences.inlineJsonPointer(key, blobUri, expectedVersion + 1L)));
    if (additionalPointerOps != null) {
      pointerOps.addAll(additionalPointerOps);
    }
    return jobIndexStore.compareAndSetBatchWithPointerOps(canonicalMutation, pointerOps);
  }

  public void delete(String accountId, String jobId) {
    if (!blank(accountId) && !blank(jobId)) {
      pointerStore.delete(Keys.reconcileJobProjectionPointer(accountId, jobId));
    }
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
