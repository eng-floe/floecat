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

import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import java.util.concurrent.atomic.LongAdder;

/**
 * The single in-flight telemetry tally for one GetUserObjects request: every phase timer plus every
 * found/not-found and cache counter. The request-level instance lives on the driver; each parallel
 * build task keeps its own instance that the driver folds back in via {@link #mergeFrom} once the
 * task has joined. Every slot is a {@link LongAdder}, so concurrent selection updates, driver
 * updates, and per-task merges are lock-free and thread-safe.
 */
final class TimingAccumulator {
  private final LongAdder statsLookupNanos = new LongAdder();
  private final LongAdder decorateRelationNanos = new LongAdder();
  private final LongAdder decorateViewNanos = new LongAdder();
  private final LongAdder decorateColumnsNanos = new LongAdder();
  private final LongAdder decorateColumnInvokeNanos = new LongAdder();
  private final LongAdder decorateCompleteNanos = new LongAdder();
  private final LongAdder decoratePersistRelationNanos = new LongAdder();
  private final LongAdder decoratePersistColumnsNanos = new LongAdder();
  private final LongAdder decorateColumnWarmHits = new LongAdder();
  private final LongAdder resolveNanos = new LongAdder();
  private final LongAdder normalizeNanos = new LongAdder();
  private final LongAdder defaultCatalogNanos = new LongAdder();
  private final LongAdder baseInjectNanos = new LongAdder();
  private final LongAdder pinCollectNanos = new LongAdder();
  private final LongAdder pinCommitNanos = new LongAdder();
  private final LongAdder buildFanoutNanos = new LongAdder();
  private final LongAdder relationBuildNanos = new LongAdder();
  private final LongAdder decorationNanos = new LongAdder();
  private final LongAdder statsWarmNanos = new LongAdder();
  private final LongAdder selectRelationNanos = new LongAdder();
  private final LongAdder nameResolveNanos = new LongAdder();
  private final LongAdder nodeResolveNanos = new LongAdder();
  private final LongAdder foundCount = new LongAdder();
  private final LongAdder notFoundCount = new LongAdder();
  private final LongAdder defaultCatalogLookups = new LongAdder();
  private final LongAdder nameResolutionCacheHits = new LongAdder();
  private final LongAdder nameResolutionCacheMisses = new LongAdder();
  private final LongAdder nodeResolutionCacheHits = new LongAdder();
  private final LongAdder nodeResolutionCacheMisses = new LongAdder();

  void addStatsLookupNanos(long nanos) {
    statsLookupNanos.add(nanos);
  }

  void addStatsWarmNanos(long nanos) {
    statsWarmNanos.add(nanos);
  }

  long statsLookupNanos() {
    return statsLookupNanos.sum();
  }

  void addDecorateRelationNanos(long nanos) {
    decorateRelationNanos.add(nanos);
  }

  void addDecorateViewNanos(long nanos) {
    decorateViewNanos.add(nanos);
  }

  void addDecorateColumnsNanos(long nanos) {
    decorateColumnsNanos.add(nanos);
  }

  void addDecorateColumnInvokeNanos(long nanos) {
    decorateColumnInvokeNanos.add(nanos);
  }

  void addDecorateCompleteNanos(long nanos) {
    decorateCompleteNanos.add(nanos);
  }

  void addDecoratePersistRelationNanos(long nanos) {
    decoratePersistRelationNanos.add(nanos);
  }

  void addDecoratePersistColumnsNanos(long nanos) {
    decoratePersistColumnsNanos.add(nanos);
  }

  void addDecorateColumnWarmHits(long warmHits) {
    decorateColumnWarmHits.add(warmHits);
  }

  long decorationTotalNanos() {
    return decorateRelationNanos.sum()
        + decorateViewNanos.sum()
        + decorateColumnsNanos.sum()
        + decorateCompleteNanos.sum();
  }

  void addResolveNanos(long nanos) {
    resolveNanos.add(nanos);
  }

  void addNormalizeNanos(long nanos) {
    normalizeNanos.add(nanos);
  }

  void addDefaultCatalogNanos(long nanos) {
    defaultCatalogNanos.add(nanos);
  }

  void addBaseInjectNanos(long nanos) {
    baseInjectNanos.add(nanos);
  }

  void addPinCollectNanos(long nanos) {
    pinCollectNanos.add(nanos);
  }

  void addBuildFanoutNanos(long nanos) {
    buildFanoutNanos.add(nanos);
  }

  void addPinCommitNanos(long nanos) {
    pinCommitNanos.add(nanos);
  }

  void addRelationBuildNanos(long nanos) {
    relationBuildNanos.add(nanos);
  }

  void addDecorationNanos(long nanos) {
    decorationNanos.add(nanos);
  }

  void addSelectRelationNanos(long nanos) {
    selectRelationNanos.add(nanos);
  }

  void addNameResolveNanos(long nanos) {
    nameResolveNanos.add(nanos);
  }

  void addNodeResolveNanos(long nanos) {
    nodeResolveNanos.add(nanos);
  }

  void recordFound() {
    foundCount.increment();
  }

  /** Undo a FOUND count: a relation counted FOUND at selection later built into an ERROR. */
  void unrecordFound() {
    foundCount.decrement();
  }

  int found() {
    return (int) foundCount.sum();
  }

  void recordNotFound() {
    notFoundCount.increment();
  }

  int notFound() {
    return (int) notFoundCount.sum();
  }

  void recordDefaultCatalogLookup() {
    defaultCatalogLookups.increment();
  }

  void recordNameCacheHit() {
    nameResolutionCacheHits.increment();
  }

  void recordNameCacheMiss() {
    nameResolutionCacheMisses.increment();
  }

  void recordNodeCacheHit() {
    nodeResolutionCacheHits.increment();
  }

  void recordNodeCacheMiss() {
    nodeResolutionCacheMisses.increment();
  }

  long resolveNanos() {
    return resolveNanos.sum();
  }

  long baseInjectNanos() {
    return baseInjectNanos.sum();
  }

  long relationBuildNanos() {
    return relationBuildNanos.sum();
  }

  long decorationNanos() {
    return decorationNanos.sum();
  }

  /** Sum of the two pin sub-phases; the {@code pin_ms} derived metric. */
  long pinNanos() {
    return pinCollectNanos.sum() + pinCommitNanos.sum();
  }

  /** Scheduling time left after every measured wall-clock phase is subtracted. Never negative. */
  long schedulingNanos(long totalNanos) {
    return Math.max(
        0L,
        totalNanos
            - resolveNanos.sum()
            - baseInjectNanos.sum()
            - pinCollectNanos.sum()
            - pinCommitNanos.sum()
            - buildFanoutNanos.sum()
            - statsWarmNanos.sum());
  }

  /** Fold a joined task's complete tally into the request tally. */
  void mergeFrom(TimingAccumulator other) {
    statsLookupNanos.add(other.statsLookupNanos.sum());
    decorateRelationNanos.add(other.decorateRelationNanos.sum());
    decorateViewNanos.add(other.decorateViewNanos.sum());
    decorateColumnsNanos.add(other.decorateColumnsNanos.sum());
    decorateColumnInvokeNanos.add(other.decorateColumnInvokeNanos.sum());
    decorateCompleteNanos.add(other.decorateCompleteNanos.sum());
    decoratePersistRelationNanos.add(other.decoratePersistRelationNanos.sum());
    decoratePersistColumnsNanos.add(other.decoratePersistColumnsNanos.sum());
    decorateColumnWarmHits.add(other.decorateColumnWarmHits.sum());
    resolveNanos.add(other.resolveNanos.sum());
    normalizeNanos.add(other.normalizeNanos.sum());
    defaultCatalogNanos.add(other.defaultCatalogNanos.sum());
    baseInjectNanos.add(other.baseInjectNanos.sum());
    pinCollectNanos.add(other.pinCollectNanos.sum());
    pinCommitNanos.add(other.pinCommitNanos.sum());
    relationBuildNanos.add(other.relationBuildNanos.sum());
    decorationNanos.add(other.decorationNanos.sum());
    statsWarmNanos.add(other.statsWarmNanos.sum());
    selectRelationNanos.add(other.selectRelationNanos.sum());
    nameResolveNanos.add(other.nameResolveNanos.sum());
    nodeResolveNanos.add(other.nodeResolveNanos.sum());
    foundCount.add(other.foundCount.sum());
    notFoundCount.add(other.notFoundCount.sum());
    defaultCatalogLookups.add(other.defaultCatalogLookups.sum());
    nameResolutionCacheHits.add(other.nameResolutionCacheHits.sum());
    nameResolutionCacheMisses.add(other.nameResolutionCacheMisses.sum());
    nodeResolutionCacheHits.add(other.nodeResolutionCacheHits.sum());
    nodeResolutionCacheMisses.add(other.nodeResolutionCacheMisses.sum());
  }

  /** Write the request summary using the diagnostics contract documented for GetUserObjects. */
  void flushInto(PhaseDiagnostics diagnostics, SummaryContext ctx) {
    diagnostics.put("query_id", ctx.queryId());
    diagnostics.put("correlation_id", ctx.correlationId());
    diagnostics.put("candidates", ctx.candidates());
    diagnostics.put("chunks", ctx.chunks());
    diagnostics.put("found", found());
    diagnostics.put("not_found", notFound());
    diagnostics.put("total_ms", ctx.totalMs());
    diagnostics.nanos("resolve", resolveNanos.sum());
    diagnostics.nanos("normalize", normalizeNanos.sum());
    diagnostics.nanos("select_relation", selectRelationNanos.sum());
    diagnostics.nanos("default_catalog", defaultCatalogNanos.sum());
    diagnostics.nanos("name_resolve", nameResolveNanos.sum());
    diagnostics.nanos("node_resolve", nodeResolveNanos.sum());
    diagnostics.nanos("base_inject", baseInjectNanos.sum());
    diagnostics.nanos("pin_collect", pinCollectNanos.sum());
    diagnostics.nanos("pin_commit", pinCommitNanos.sum());
    diagnostics.put("pin_ms", ctx.pinMs());
    diagnostics.nanos("relation_build", relationBuildNanos.sum());
    diagnostics.nanos("decoration", decorationNanos.sum());
    diagnostics.nanos("stats_lookup", statsLookupNanos.sum());
    diagnostics.nanos("stats_warm", statsWarmNanos.sum());
    diagnostics.nanos("decorate_relation", decorateRelationNanos.sum());
    diagnostics.nanos("decorate_view", decorateViewNanos.sum());
    diagnostics.nanos("decorate_columns", decorateColumnsNanos.sum());
    diagnostics.nanos("decorate_column_invoke", decorateColumnInvokeNanos.sum());
    diagnostics.nanos("decorate_complete", decorateCompleteNanos.sum());
    diagnostics.put("scheduling_ms", ctx.schedulingMs());
    diagnostics.put("decorator_warm_hits", decorateColumnWarmHits.sum());
    diagnostics.nanos(
        "hint_persist", decoratePersistRelationNanos.sum() + decoratePersistColumnsNanos.sum());
    diagnostics.put("default_catalog_lookups", defaultCatalogLookups.sum());
    diagnostics.put("name_cache_hits", nameResolutionCacheHits.sum());
    diagnostics.put("name_cache_misses", nameResolutionCacheMisses.sum());
    diagnostics.put("node_cache_hits", nodeResolutionCacheHits.sum());
    diagnostics.put("node_cache_misses", nodeResolutionCacheMisses.sum());
    diagnostics.put("name_cache_entries", ctx.nameCacheEntries());
    diagnostics.put("node_cache_entries", ctx.nodeCacheEntries());
    diagnostics.put("relation_cache_entries", ctx.relationCacheEntries());
    diagnostics.put("outcome", ctx.outcome());
    diagnostics.emit("floecat.get_user_objects.summary");
  }
}

/** Non-tally request values needed when the GetUserObjects timing summary is emitted. */
record SummaryContext(
    String queryId,
    String correlationId,
    int candidates,
    int chunks,
    double totalMs,
    double pinMs,
    double schedulingMs,
    int nameCacheEntries,
    int nodeCacheEntries,
    int relationCacheEntries,
    String outcome) {}
