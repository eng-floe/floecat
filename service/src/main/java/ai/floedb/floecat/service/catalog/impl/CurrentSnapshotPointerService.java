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

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import org.jboss.logging.Logger;

@ApplicationScoped
public class CurrentSnapshotPointerService {
  private static final Logger LOG = Logger.getLogger(CurrentSnapshotPointerService.class);

  private enum GenerationReconcile {
    WHEN_POINTER_MOVES,
    ALWAYS_AFTER_ENTRY,
    NEVER
  }

  /**
   * Phase timings of one advance, for publication diagnostics. {@code pointerResult} is the
   * current-pointer outcome ({@code null} when the repository declined to evaluate the candidate),
   * {@code entryNanos} covers snapshot reads plus the root entry commit, and {@code
   * generationNanos} is zero when the generation commit was skipped.
   */
  public record AdvanceTrace(
      SnapshotRepository.CurrentSnapshotPointerUpdateResult pointerResult,
      long pointerNanos,
      long entryNanos,
      long generationNanos,
      boolean generationCommitted) {}

  @Inject SnapshotRepository snapshotRepo;
  @Inject TableRootWriter rootWriter;

  public void maybeAdvance(ResourceId tableId, long snapshotId, String corr) {
    maybeAdvance(tableId, loadCandidate(tableId, snapshotId, corr), corr);
  }

  /**
   * Advances a snapshot whose stats generation was committed to the table root immediately before
   * this call. The snapshot-entry upsert preserves that ref, so the defensive generation
   * reconciliation used for arbitrary in-place snapshot updates would only repeat a root read and
   * manifest lookup.
   */
  public AdvanceTrace maybeAdvanceAfterStatsGenerationCommit(
      ResourceId tableId, Snapshot candidate, String corr) {
    return maybeAdvance(tableId, candidate, corr, GenerationReconcile.NEVER);
  }

  /**
   * Advances a snapshot after a stats-generation commit found no table-root entry. This includes a
   * newly created snapshot and a retried publication whose fence already created the snapshot in
   * the snapshot store. Register the entry first, then attach the active generation regardless of
   * whether the current pointer moves. The latter is required for finalized AS_OF visibility of an
   * out-of-order snapshot.
   */
  public AdvanceTrace maybeAdvanceAfterMissingRootEntry(
      ResourceId tableId, Snapshot candidate, String corr) {
    return maybeAdvance(tableId, candidate, corr, GenerationReconcile.ALWAYS_AFTER_ENTRY);
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
    maybeAdvance(tableId, candidate, corr, GenerationReconcile.WHEN_POINTER_MOVES);
  }

  private AdvanceTrace maybeAdvance(
      ResourceId tableId,
      Snapshot candidate,
      String corr,
      GenerationReconcile generationReconcile) {
    long pointerStartedNanos = System.nanoTime();
    var result = snapshotRepo.maybeAdvanceCurrentSnapshotPointer(tableId, candidate);
    long pointerNanos = System.nanoTime() - pointerStartedNanos;
    if (result == null) {
      return new AdvanceTrace(null, pointerNanos, 0L, 0L, false);
    }
    switch (result) {
      case TABLE_MISSING -> throw GrpcErrors.notFound(corr, TABLE, Map.of("id", tableId.getId()));
      case CONFLICT ->
          throw new BaseResourceRepository.AbortRetryableException(
              "current snapshot pointer changed for table " + tableId.getId());
      default -> {}
    }
    long entryStartedNanos = System.nanoTime();
    rootWriter.commitSnapshotEntry(tableId, candidate.getSnapshotId());
    long entryNanos = System.nanoTime() - entryStartedNanos;
    if (generationReconcile == GenerationReconcile.ALWAYS_AFTER_ENTRY
        || (generationReconcile == GenerationReconcile.WHEN_POINTER_MOVES
            && result == SnapshotRepository.CurrentSnapshotPointerUpdateResult.UPDATED)) {
      // The committed current pointer just moved onto `candidate`. commitSnapshotEntry advances the
      // root's current_snapshot_id at registration; what the finalize gate defers is query-time
      // VISIBILITY (an entry without a stats_generation_ref is not pinnable). An in-place update of
      // an ALREADY-finalized snapshot (e.g. UpdateSnapshot bumping upstream_created_at) triggers no
      // later finalize to attach that generation ref, so reconcile it now through the finalize
      // path: a no-op when the gate is off, when the candidate is not yet finalized (its own
      // finalize will publish it), or when the root entry already carries the ref.
      //
      // A failed publication leaves a durable root-resync marker before propagating. A retry that
      // now observes UNCHANGED still skips this synchronous publication, so visibility may converge
      // on the GC cadence. Removing the guard would close that window at the cost of an extra root
      // read and commit attempt for every snapshot write; keep that measurable trade separate.
      long generationStartedNanos = System.nanoTime();
      rootWriter.commitStatsGeneration(tableId, candidate.getSnapshotId());
      return new AdvanceTrace(
          result, pointerNanos, entryNanos, System.nanoTime() - generationStartedNanos, true);
    }
    return new AdvanceTrace(result, pointerNanos, entryNanos, 0L, false);
  }
}
