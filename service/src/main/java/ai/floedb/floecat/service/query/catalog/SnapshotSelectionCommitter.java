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

package ai.floedb.floecat.service.query.catalog;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.metagraph.model.GraphNodeKind;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.query.rpc.RelationPinSet;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.SnapshotSelections;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.query.resolver.QueryInputResolver;
import ai.floedb.floecat.service.query.resolver.QueryInputResolver.SnapshotSelectionMemo;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.function.BooleanSupplier;
import org.jboss.logging.Logger;

/**
 * The snapshot-selection transaction for one GetUserObjects stream, driven per chunk. The conductor
 * calls {@link #accumulate} as each chunk's relations are gathered (collect the resolver's
 * selections and fold them into the pending set) and {@link #commit} before the chunk's stats are
 * warmed (write the pending set durably to the QueryContext). Owns the mutable selection state —
 * {@code pendingSelections} plus the per-request snapshot-selection memo — and records the
 * selection-collect / selection-commit timers into the shared request {@link TimingAccumulator}.
 *
 * <p>Snapshot selections are accumulated here before they are written to the process-local query
 * context. They are not GC roots; retention-aware durable reachability owns object lifetime.
 */
final class SnapshotSelectionCommitter {

  private static final Logger LOG = Logger.getLogger(SnapshotSelectionCommitter.class);

  private final QueryInputResolver inputResolver;
  private final QueryContextStore queryStore;
  private final QueryContext ctx;
  private final String correlationId;
  private final TimingAccumulator timings;

  // First-touch snapshot per relation id, shared with the resolver so a relation pins to one
  // snapshot for the life of the request.
  private final SnapshotSelectionMemo snapshotSelectionMemo = new SnapshotSelectionMemo();
  private final Object pendingSelectionsLock = new Object();

  // Pins gathered but not yet made durable; folded across chunks, drained by commit().
  private RelationPinSet pendingSelections = RelationPinSet.getDefaultInstance();

  SnapshotSelectionCommitter(
      QueryInputResolver inputResolver,
      QueryContextStore queryStore,
      QueryContext ctx,
      String correlationId,
      TimingAccumulator timings) {
    this.inputResolver = inputResolver;
    this.queryStore = queryStore;
    this.ctx = ctx;
    this.correlationId = correlationId;
    this.timings = timings;
  }

  /**
   * Resolve snapshot selections for the chunk's relations and fold them into the pending set.
   * Records the selection-collect timing into the shared tally and the selection sub-phase counters
   * into {@code diagnostics}.
   */
  void accumulate(List<ResolvedRelation> toPin, PhaseDiagnostics diagnostics) {
    accumulate(toPin, diagnostics, () -> false);
  }

  /** Resolve selections for a chunk while observing cancellation. */
  void accumulate(
      List<ResolvedRelation> toPin, PhaseDiagnostics diagnostics, BooleanSupplier cancelled) {
    long pinStartNs = System.nanoTime();
    RelationPinSet chunkPins = RelationPinSet.getDefaultInstance();
    try {
      throwIfCancelled(cancelled);
      chunkPins = collectChunkPins(toPin, diagnostics, cancelled);
      throwIfCancelled(cancelled);
      long accumulateStartNs = System.nanoTime();
      boolean accumulated;
      try {
        accumulated = accumulateChunkPins(chunkPins, cancelled);
      } finally {
        diagnostics.nanos("snapshot.accumulate", System.nanoTime() - accumulateStartNs);
      }
      if (!accumulated) {
        throw new CancellationException("snapshot selection accumulation cancelled");
      }
    } finally {
      timings.addPinCollectNanos(System.nanoTime() - pinStartNs);
    }
  }

  /** Make the accumulated selections durable on the QueryContext. */
  void commit() {
    commit(() -> false);
  }

  /**
   * Make pending selections durable unless cancellation stops the request. Cancellation discards
   * the pending selection set and leaves the query context unchanged.
   */
  void commit(BooleanSupplier cancelled) {
    long pinCommitStartNs = System.nanoTime();
    try {
      commitChunkPins(cancelled);
    } finally {
      timings.addPinCommitNanos(System.nanoTime() - pinCommitStartNs);
    }
  }

  /** Pending (not-yet-committed) pin count, for the driver's per-chunk debug log. */
  int pendingSelectionCount() {
    synchronized (pendingSelectionsLock) {
      return pendingSelections.getPinsCount();
    }
  }

  /** Detach selections that cancellation must discard without blocking on a store operation. */
  RelationPinSet detachPendingSelections() {
    synchronized (pendingSelectionsLock) {
      RelationPinSet detached = pendingSelections;
      pendingSelections = RelationPinSet.getDefaultInstance();
      return detached;
    }
  }

  private RelationPinSet collectChunkPins(
      List<ResolvedRelation> relations, PhaseDiagnostics diagnostics, BooleanSupplier cancelled) {
    throwIfCancelled(cancelled);

    if (relations == null || relations.isEmpty()) {
      return RelationPinSet.getDefaultInstance();
    }
    diagnostics.add("snapshot.relations", relations.size());
    List<QueryInput> inputs = new ArrayList<>(relations.size());
    long buildInputsStartNs = System.nanoTime();
    for (ResolvedRelation relation : relations) {
      QueryInput input = buildCanonicalQueryInput(relation);
      if (input != null) {
        inputs.add(input);
      }
    }
    diagnostics.nanos("pin.build_inputs", System.nanoTime() - buildInputsStartNs);
    diagnostics.add("snapshot.inputs", inputs.size());
    if (inputs.isEmpty()) {
      return RelationPinSet.getDefaultInstance();
    }
    long asOfStartNs = System.nanoTime();
    var asOfDefault = ctx.parseAsOfDefault(correlationId);
    diagnostics.nanos("snapshot.asof_default", System.nanoTime() - asOfStartNs);
    long resolverStartNs = System.nanoTime();
    var resolution =
        inputResolver.resolveInputs(
            ctx.getQueryId(),
            correlationId,
            inputs,
            asOfDefault,
            Optional.of(ctx.getQueryDefaultCatalogId()),
            snapshotSelectionMemo,
            diagnostics,
            cancelled);
    diagnostics.nanos("snapshot.resolver", System.nanoTime() - resolverStartNs);
    RelationPinSet incoming = resolution.relationPinSet();
    RelationPinSet pins = incoming == null ? RelationPinSet.getDefaultInstance() : incoming;
    throwIfCancelled(cancelled);
    diagnostics.add("snapshot.output_selections", pins.getPinsCount());
    return pins;
  }

  private QueryInput buildCanonicalQueryInput(ResolvedRelation relation) {
    // Built-in system relations are not version-pinned in query context snapshots.
    if (relation.node().origin() == GraphNodeOrigin.SYSTEM) {
      return null;
    }
    QueryInput.Builder builder;
    GraphNodeKind kind = relation.node().kind();
    if (kind == GraphNodeKind.TABLE) {
      builder = QueryInput.newBuilder().setTableId(relation.relationId());
    } else if (kind == GraphNodeKind.VIEW) {
      builder = QueryInput.newBuilder().setViewId(relation.relationId());
    } else {
      return null;
    }
    if (relation.selectedInput().hasSnapshot()) {
      builder.setSnapshot(relation.selectedInput().getSnapshot());
    }
    return builder.build();
  }

  // Track every pin that must be durable before the next chunk is emitted.
  private boolean accumulateChunkPins(RelationPinSet incomingPins, BooleanSupplier cancelled) {
    if (incomingPins == null || incomingPins.getPinsCount() == 0) {
      return true;
    }
    RelationPinSet accumulatedPins = RelationPinSet.getDefaultInstance();
    boolean cancelledBeforeMerge;
    synchronized (pendingSelectionsLock) {
      cancelledBeforeMerge = cancelled.getAsBoolean();
      if (cancelledBeforeMerge) {
        accumulatedPins = RelationPinSet.getDefaultInstance();
      } else {
        accumulatedPins = pendingSelections;
        try {
          pendingSelections =
              SnapshotSelections.mergeSets(accumulatedPins, incomingPins, correlationId);
        } catch (RuntimeException | Error e) {
          pendingSelections = RelationPinSet.getDefaultInstance();
          throw e;
        }
      }
    }
    if (cancelledBeforeMerge) {
      return false;
    }
    return true;
  }

  private void commitChunkPins(BooleanSupplier cancelled) {
    RelationPinSet toCommit;
    boolean cancelledBeforeCommit;
    synchronized (pendingSelectionsLock) {
      cancelledBeforeCommit = cancelled.getAsBoolean();
      if (cancelledBeforeCommit) {
        toCommit = pendingSelections;
        pendingSelections = RelationPinSet.getDefaultInstance();
      } else if (pendingSelections.getPinsCount() == 0) {
        return;
      } else {
        toCommit = pendingSelections;
        pendingSelections = RelationPinSet.getDefaultInstance();
      }
    }
    if (cancelledBeforeCommit) {
      throw new CancellationException("query pin commit cancelled");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debugf(
          "Committing chunk snapshot selections query_id=%s selection_count=%d",
          ctx.getQueryId(), toCommit.getPinsCount());
    }
    Optional<QueryContext> updated;
    try {
      updated =
          queryStore.update(
              ctx.getQueryId(),
              existing -> {
                if (cancelled.getAsBoolean()) {
                  throw new CancellationException("query pin commit cancelled");
                }
                return mergeRelationPins(existing, toCommit, correlationId);
              });
    } catch (RuntimeException | Error e) {
      throw e;
    }
    if (updated.isEmpty()) {
      LOG.warnf("Failed to commit chunk pins query_id=%s query context missing", ctx.getQueryId());
      throw GrpcErrors.notFound(
          correlationId, QUERY_NOT_FOUND, Map.of("query_id", ctx.getQueryId()));
    }
    if (LOG.isDebugEnabled()) {
      LOG.debugf("Committed chunk pins query_id=%s", ctx.getQueryId());
    }
  }

  /** Stop the transaction before it creates a pin set that cannot be committed. */
  private static void throwIfCancelled(BooleanSupplier cancelled) {
    if (cancelled.getAsBoolean()) {
      throw new CancellationException("query pin collection cancelled");
    }
  }

  private QueryContext mergeRelationPins(
      QueryContext existing, RelationPinSet incoming, String correlationId) {
    if (incoming == null || incoming.getPinsCount() == 0) {
      return existing;
    }
    RelationPinSet current = existing.parseSnapshotSelections(correlationId);
    RelationPinSet merged = SnapshotSelections.mergeSets(current, incoming, correlationId);
    if (current.equals(merged)) {
      return existing;
    }
    return existing.toBuilder().relationPins(merged.toByteArray()).build();
  }
}
