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

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.SNAPSHOT;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.TABLE;

import ai.floedb.floecat.catalog.rpc.BlobRef;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import org.jboss.logging.Logger;

@ApplicationScoped
public class CurrentSnapshotPointerService {
  private static final Logger LOG = Logger.getLogger(CurrentSnapshotPointerService.class);

  @Inject SnapshotRepository snapshotRepo;
  @Inject TableRootWriter rootWriter;

  public void maybeAdvance(ResourceId tableId, long snapshotId, String corr) {
    maybeAdvance(tableId, loadCandidate(tableId, snapshotId, corr), corr);
  }

  public void publishCaptureManifest(
      ResourceId tableId, long snapshotId, BlobRef manifestRef, String corr) {
    long totalStartNanos = System.nanoTime();
    long loadNanos = 0L;
    long pointerNanos = 0L;
    long rootNanos = 0L;
    String outcome = "failed";
    try {
      long loadStartNanos = System.nanoTime();
      Snapshot candidate;
      try {
        candidate = loadCandidate(tableId, snapshotId, corr);
      } finally {
        loadNanos = System.nanoTime() - loadStartNanos;
      }
      long pointerStartNanos = System.nanoTime();
      SnapshotRepository.CurrentSnapshotPointerUpdateResult result;
      try {
        result = snapshotRepo.maybeAdvanceCurrentSnapshotPointer(tableId, candidate);
      } finally {
        pointerNanos = System.nanoTime() - pointerStartNanos;
      }
      if (result == null) {
        outcome = "no_result";
        return;
      }
      switch (result) {
        case TABLE_MISSING -> throw GrpcErrors.notFound(corr, TABLE, Map.of("id", tableId.getId()));
        case CONFLICT -> throw GrpcErrors.aborted(corr, Map.of("id", tableId.getId()));
        default -> {}
      }
      long rootStartNanos = System.nanoTime();
      try {
        rootWriter.commitSnapshotCapture(tableId, candidate, manifestRef);
      } finally {
        rootNanos = System.nanoTime() - rootStartNanos;
      }
      outcome = result.name().toLowerCase(java.util.Locale.ROOT);
    } finally {
      long totalNanos = System.nanoTime() - totalStartNanos;
      long otherNanos = Math.max(0L, totalNanos - loadNanos - pointerNanos - rootNanos);
      LOG.infof(
          "snapshot_capture_publish_timing jobId=%s snapshotId=%d outcome=%s totalMs=%.3f"
              + " snapshotLoadMs=%.3f pointerAdvanceMs=%.3f rootCommitMs=%.3f otherMs=%.3f",
          corr,
          snapshotId,
          outcome,
          totalNanos / 1_000_000.0,
          loadNanos / 1_000_000.0,
          pointerNanos / 1_000_000.0,
          rootNanos / 1_000_000.0,
          otherNanos / 1_000_000.0);
    }
  }

  private Snapshot loadCandidate(ResourceId tableId, long snapshotId, String corr) {
    return snapshotRepo
        .getById(tableId, snapshotId)
        .orElseThrow(
            () ->
                GrpcErrors.notFound(
                    corr,
                    SNAPSHOT,
                    Map.of(
                        "table_id", tableId.getId(),
                        "id", Long.toString(snapshotId))));
  }

  /**
   * Advance the current-snapshot pointer to {@code candidate} if it should become current, and
   * record the snapshot's entry on the table root. Every snapshot write funnels through here —
   * create, finalize, in-place update, reconcile — and the root entry upsert re-captures the
   * snapshot's immutable blob identity each time, so an in-place rewrite of the current snapshot
   * refreshes the pinned identity with no separate republish step.
   */
  public void maybeAdvance(ResourceId tableId, Snapshot candidate, String corr) {
    var result = snapshotRepo.maybeAdvanceCurrentSnapshotPointer(tableId, candidate);
    if (result == null) {
      return;
    }
    switch (result) {
      case TABLE_MISSING -> throw GrpcErrors.notFound(corr, TABLE, Map.of("id", tableId.getId()));
      case CONFLICT -> throw GrpcErrors.aborted(corr, Map.of("id", tableId.getId()));
      default -> {}
    }
    rootWriter.commitSnapshotEntry(tableId, candidate);
    if (result == SnapshotRepository.CurrentSnapshotPointerUpdateResult.UPDATED) {
      // The committed current pointer just moved onto `candidate`. commitSnapshotEntry advances the
      // root's current_snapshot_id at registration; what the finalize gate defers is query-time
      // VISIBILITY (an entry without a stats_generation_ref is not pinnable). An in-place update of
      // an ALREADY-finalized snapshot (e.g. UpdateSnapshot bumping upstream_created_at) triggers no
      // later finalize to attach that generation ref, so reconcile it now through the finalize
      // path: a no-op when the gate is off, when the candidate is not yet finalized (its own
      // finalize will publish it), or when the root entry already carries the ref.
      rootWriter.commitStatsGeneration(tableId, candidate.getSnapshotId());
    }
  }
}
