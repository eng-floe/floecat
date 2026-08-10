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
import ai.floedb.floecat.service.query.QueryPins;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.query.resolver.QueryInputResolver;
import ai.floedb.floecat.service.query.resolver.QueryInputResolver.CurrentSnapshotPinCache;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.function.BooleanSupplier;
import org.jboss.logging.Logger;

/**
 * The pin-durability transaction for one GetUserObjects stream, driven per chunk. The conductor
 * calls {@link #accumulate} as each chunk's relations are gathered (collect the resolver's pins and
 * fold them into the pending set) and {@link #commit} before the chunk's stats are warmed (write
 * the pending set durably to the QueryContext). Owns the mutable pin state — {@code
 * pendingChunkPins} plus the per-request snapshot-pin memo — and records the pin-collect /
 * pin-commit timers into the shared request {@link TimingAccumulator}.
 *
 * <p>The transient-GC-root invariant lives here: the resolver registers each pin's blob as a
 * transient GC root at resolution, protecting it across the collect→commit window; {@link #commit}
 * turns the QueryContext into a durable root, and every failure arm releases those transient roots
 * so a failed transaction cannot pin blobs forever.
 */
final class ChunkPinBarrier {

  private static final Logger LOG = Logger.getLogger(ChunkPinBarrier.class);

  private final QueryInputResolver inputResolver;
  private final QueryContextStore queryStore;
  private final QueryContext ctx;
  private final String correlationId;
  private final TimingAccumulator timings;

  // First-touch snapshot per relation id, shared with the resolver so a relation pins to one
  // snapshot for the life of the request.
  private final CurrentSnapshotPinCache currentSnapshotPinCache = new CurrentSnapshotPinCache();
  private final Object pendingPinsLock = new Object();

  // Pins gathered but not yet made durable; folded across chunks, drained by commit().
  private RelationPinSet pendingChunkPins = RelationPinSet.getDefaultInstance();

  ChunkPinBarrier(
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
   * Resolve pins for the chunk's relations and fold them into the pending set. Records the
   * pin-collect timing into the shared tally and the {@code pin.*} sub-phase counters into {@code
   * diagnostics}.
   */
  void accumulate(List<ResolvedRelation> toPin, PhaseDiagnostics diagnostics) {
    accumulate(toPin, diagnostics, () -> false);
  }

  /**
   * Resolve pins for a chunk while observing cancellation. Any roots collected before cancellation
   * are released because they will not become durable on the query context.
   */
  void accumulate(
      List<ResolvedRelation> toPin, PhaseDiagnostics diagnostics, BooleanSupplier cancelled) {
    long pinStartNs = System.nanoTime();
    RelationPinSet chunkPins = RelationPinSet.getDefaultInstance();
    boolean handedOff = false;
    try {
      throwIfCancelled(cancelled);
      chunkPins = collectChunkPins(toPin, diagnostics, cancelled);
      throwIfCancelled(cancelled);
      long accumulateStartNs = System.nanoTime();
      boolean accumulated;
      try {
        // From this point the barrier owns the incoming roots on every outcome: a successful merge
        // keeps them pending, cancellation releases them, and a failed merge releases them together
        // with the previously pending roots in one store call.
        handedOff = true;
        accumulated = accumulateChunkPins(chunkPins, cancelled);
      } finally {
        diagnostics.nanos("pin.accumulate", System.nanoTime() - accumulateStartNs);
      }
      if (!accumulated) {
        throw new CancellationException("query pin accumulation cancelled");
      }
    } finally {
      if (!handedOff) {
        releaseRoots(chunkPins);
      }
      timings.addPinCollectNanos(System.nanoTime() - pinStartNs);
    }
  }

  /** Make the accumulated pins durable on the QueryContext. Records the pin-commit timing. */
  void commit() {
    commit(() -> false);
  }

  /**
   * Make pending pins durable unless cancellation stops the request. Cancellation releases their
   * transient roots and leaves the query context unchanged.
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
  int pendingPinCount() {
    synchronized (pendingPinsLock) {
      return pendingChunkPins.getPinsCount();
    }
  }

  /** Detach roots that cancellation must release without blocking on a store operation. */
  RelationPinSet detachPendingPins() {
    synchronized (pendingPinsLock) {
      RelationPinSet detached = pendingChunkPins;
      pendingChunkPins = RelationPinSet.getDefaultInstance();
      return detached;
    }
  }

  private RelationPinSet collectChunkPins(
      List<ResolvedRelation> relations, PhaseDiagnostics diagnostics, BooleanSupplier cancelled) {
    throwIfCancelled(cancelled);

    if (relations == null || relations.isEmpty()) {
      return RelationPinSet.getDefaultInstance();
    }
    diagnostics.add("pin.relations", relations.size());
    List<QueryInput> inputs = new ArrayList<>(relations.size());
    long buildInputsStartNs = System.nanoTime();
    for (ResolvedRelation relation : relations) {
      QueryInput input = buildCanonicalQueryInput(relation);
      if (input != null) {
        inputs.add(input);
      }
    }
    diagnostics.nanos("pin.build_inputs", System.nanoTime() - buildInputsStartNs);
    diagnostics.add("pin.inputs", inputs.size());
    if (inputs.isEmpty()) {
      return RelationPinSet.getDefaultInstance();
    }
    long asOfStartNs = System.nanoTime();
    var asOfDefault = ctx.parseAsOfDefault(correlationId);
    diagnostics.nanos("pin.asof_default", System.nanoTime() - asOfStartNs);
    long resolverStartNs = System.nanoTime();
    var resolution =
        inputResolver.resolveInputs(
            ctx.getQueryId(),
            correlationId,
            inputs,
            asOfDefault,
            Optional.of(ctx.getQueryDefaultCatalogId()),
            currentSnapshotPinCache,
            diagnostics,
            cancelled);
    diagnostics.nanos("pin.resolver", System.nanoTime() - resolverStartNs);
    RelationPinSet incoming = resolution.relationPinSet();
    RelationPinSet pins = incoming == null ? RelationPinSet.getDefaultInstance() : incoming;
    boolean handedOff = false;
    try {
      throwIfCancelled(cancelled);
      diagnostics.add("pin.output_pins", pins.getPinsCount());
      handedOff = true;
      return pins;
    } finally {
      if (!handedOff) {
        releaseRoots(pins);
      }
    }
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
    try {
      synchronized (pendingPinsLock) {
        cancelledBeforeMerge = cancelled.getAsBoolean();
        if (cancelledBeforeMerge) {
          accumulatedPins = RelationPinSet.getDefaultInstance();
        } else {
          accumulatedPins = pendingChunkPins;
          try {
            pendingChunkPins = QueryPins.mergeSets(accumulatedPins, incomingPins, correlationId);
          } catch (RuntimeException | Error e) {
            pendingChunkPins = RelationPinSet.getDefaultInstance();
            throw e;
          }
        }
      }
    } catch (RuntimeException | Error e) {
      releaseRoots(accumulatedPins.toBuilder().addAllPins(incomingPins.getPinsList()).build());
      throw e;
    }
    if (cancelledBeforeMerge) {
      releaseRoots(incomingPins);
      return false;
    }
    return true;
  }

  private void commitChunkPins(BooleanSupplier cancelled) {
    RelationPinSet toCommit;
    boolean cancelledBeforeCommit;
    synchronized (pendingPinsLock) {
      cancelledBeforeCommit = cancelled.getAsBoolean();
      if (cancelledBeforeCommit) {
        toCommit = pendingChunkPins;
        pendingChunkPins = RelationPinSet.getDefaultInstance();
      } else if (pendingChunkPins.getPinsCount() == 0) {
        return;
      } else {
        toCommit = pendingChunkPins;
        pendingChunkPins = RelationPinSet.getDefaultInstance();
      }
    }
    if (cancelledBeforeCommit) {
      releaseRoots(toCommit);
      throw new CancellationException("query pin commit cancelled");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debugf(
          "Committing chunk pins query_id=%s pin_count=%d",
          ctx.getQueryId(), toCommit.getPinsCount());
    }
    // The resolver registered these pins' blobs as transient GC roots at resolution, so they are
    // protected across the collect→commit window; this update makes the context a durable root.
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
      queryStore.releaseResolvingPinBlobs(ctx.getQueryId(), QueryPins.gcRootUris(toCommit));
      throw e;
    }
    if (updated.isEmpty()) {
      queryStore.releaseResolvingPinBlobs(ctx.getQueryId(), QueryPins.gcRootUris(toCommit));
      LOG.warnf("Failed to commit chunk pins query_id=%s query context missing", ctx.getQueryId());
      throw GrpcErrors.notFound(
          correlationId, QUERY_NOT_FOUND, Map.of("query_id", ctx.getQueryId()));
    }
    if (LOG.isDebugEnabled()) {
      LOG.debugf("Committed chunk pins query_id=%s", ctx.getQueryId());
    }
  }

  /** Release the transient roots represented by {@code pins}, if any. */
  private void releaseRoots(RelationPinSet pins) {
    if (pins != null && pins.getPinsCount() > 0) {
      queryStore.releaseResolvingPinBlobs(ctx.getQueryId(), QueryPins.gcRootUris(pins));
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
    RelationPinSet current = existing.parseRelationPins(correlationId);
    RelationPinSet merged = QueryPins.mergeSets(current, incoming, correlationId);
    if (current.equals(merged)) {
      return existing;
    }
    return existing.toBuilder().relationPins(merged.toByteArray()).build();
  }
}
