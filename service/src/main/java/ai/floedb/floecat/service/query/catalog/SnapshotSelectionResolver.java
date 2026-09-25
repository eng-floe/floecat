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
 * distributed under the License is distributed on an "AS-IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.query.catalog;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.PinKind;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.impl.QueryContext;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.LongFunction;

/**
 * Resolves the resolved snapshot id for the stats/constraints path from the query context.
 *
 * <p>This intentionally consumes only the snapshot id, not the full pin blob identity: a table pin
 * is validated (its immutable table and snapshot blobs confirmed present at the captured versions)
 * once, when it is created, and every selection is blob-backed. Snapshot retention is the GC safety
 * boundary; query state is only a local optimization. Stats are best-effort and may be stale, so
 * the hot stats path does not re-run per-read blob validation; the snapshot id alone scopes the
 * lookup coherently.
 */
final class SnapshotSelectionResolver implements SnapshotSelectionLookup {

  private final QueryContextStore queryStore;
  private final QueryContext initialCtx;
  private final String queryId;
  private final String correlationId;

  SnapshotSelectionResolver(QueryContextStore queryStore, QueryContext ctx, String correlationId) {
    this.queryStore = queryStore;
    this.initialCtx = ctx;
    // ctx may be null (no query-scoped context on this request); tolerate it here and in
    // liveContext() rather than NPE — an absent context simply means no snapshot pin.
    this.queryId = ctx == null ? "" : ctx.getQueryId();
    this.correlationId = correlationId;
  }

  @Override
  public OptionalLong resolvedSnapshotId(ResourceId tableId) {
    QueryContext ctx = liveContext();
    if (ctx == null) {
      return OptionalLong.empty();
    }
    // Every pin resolves to a concrete snapshot at construction, so a present pin always carries
    // its snapshot id (0 is a real id, never a sentinel — absence is the empty Optional).
    return ctx.findSnapshotPin(tableId, correlationId)
        .map(p -> OptionalLong.of(p.getSnapshotId()))
        .orElseGet(OptionalLong::empty);
  }

  @Override
  public Optional<com.google.protobuf.Timestamp> resolvedSnapshotIngestedAt(ResourceId tableId) {
    QueryContext ctx = liveContext();
    if (ctx == null) {
      return Optional.empty();
    }
    return ctx.findResolvedSnapshot(tableId, correlationId)
        .filter(TablePin::hasIngestedAt)
        .map(TablePin::getIngestedAt);
  }

  @Override
  public Optional<ResolvedConstraintsRef> resolvedConstraintsRef(ResourceId tableId) {
    QueryContext ctx = liveContext();
    if (ctx == null) {
      return Optional.empty();
    }
    return ctx.findResolvedSnapshot(tableId, correlationId)
        .filter(pin -> !pin.getConstraintsRefUri().isEmpty())
        .map(
            pin ->
                new ResolvedConstraintsRef(
                    pin.getConstraintsRefUri(), pin.getConstraintsRefVersion()));
  }

  @Override
  public Optional<String> resolvedStatsGenerationRef(ResourceId tableId) {
    QueryContext ctx = liveContext();
    if (ctx == null) {
      return Optional.empty();
    }
    return ctx.findResolvedSnapshot(tableId, correlationId)
        .map(pin -> pin.getStatsGenerationRefUri())
        .filter(uri -> !uri.isBlank());
  }

  /** Whether this pin represents the current SQL view rather than retained history. */
  boolean currentSnapshotIsPinned(ResourceId tableId) {
    QueryContext ctx = liveContext();
    if (ctx == null) {
      return false;
    }
    return ctx.findResolvedSnapshot(tableId, correlationId)
        .map(
            pin ->
                pin.getPinKind() == PinKind.PIN_KIND_CURRENT
                    || pin.getPinKind() == PinKind.PIN_KIND_UNSPECIFIED)
        .orElse(false);
  }

  <T> Optional<T> withPinnedSnapshot(
      ResourceId tableId, LongFunction<Optional<T>> snapshotScopedLookup) {
    QueryContext ctx = liveContext();
    if (ctx == null) {
      return Optional.empty();
    }
    return ctx.findSnapshotPin(tableId, correlationId)
        .flatMap(pin -> snapshotScopedLookup.apply(pin.getSnapshotId()));
  }

  /** May return {@code null} when the request carried no context and the store has no entry. */
  private QueryContext liveContext() {
    return queryStore.get(queryId).orElse(initialCtx);
  }
}
