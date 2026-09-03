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

package ai.floedb.floecat.service.statistics;

import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsResolutionResult;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsSyncOutcome;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.jboss.logging.Logger;

/**
 * Planner-facing STORE resolution for stats: generation ordering, per-target completeness matching,
 * the pinned → newest → stale fallback ladder and the per-batch lookup diagnostics.
 *
 * <p>Invariants this class maintains:
 *
 * <ul>
 *   <li>the pinned generation is the primary source (query-consistent); when the pin froze no
 *       generation the live/newest generation is primary;
 *   <li>the newest (live active) generation is consulted only as same-snapshot gap-fill — richer
 *       stats for identical data, never weakened snapshot consistency;
 *   <li>a partial record is still served (the planner degrades per stat) and never falls through to
 *       stale or capture;
 *   <li>serialized stats residency belongs to the repository's disk blob cache, keyed by immutable
 *       bundle identity or pointer version; this resolver owns no competing decoded cache.
 * </ul>
 *
 * <p>Capture policy (bounded sync capture, async enqueue) stays in {@link StatsOrchestrator}, which
 * owns this resolver and runs it as the store rungs of the planner ladder.
 */
final class PlannerStatsResolver {

  private static final Logger LOG = Logger.getLogger(PlannerStatsResolver.class);

  private final StatsStore statsStore;
  private final Function<StatsCaptureRequest, Optional<TargetStatsRecord>> storeReader;
  private final MetricCounter counter;
  private final Consumer<StatsSyncOutcome> hitObserver;

  /**
   * Counter seam into the orchestrator's telemetry: the orchestrator stays the single owner of the
   * component/operation tags and the null-observability no-op, so the extraction defines every
   * dashboard series exactly once.
   */
  @FunctionalInterface
  interface MetricCounter {
    void increment(MetricId metric, double amount, Tag... tags);
  }

  /**
   * @param statsStore the persisted stats store all rungs read from
   * @param storeReader the orchestrator's counted live-generation read (STORE_HITS/MISSES stay
   *     defined once, there), used by the single-target rungs
   * @param counter counter seam; the orchestrator merges in its component/operation tags
   * @param hitObserver invoked once per store/cache hit so the orchestrator's sync-outcome
   *     telemetry stays the single owner of that counter
   */
  PlannerStatsResolver(
      StatsStore statsStore,
      Function<StatsCaptureRequest, Optional<TargetStatsRecord>> storeReader,
      MetricCounter counter,
      Consumer<StatsSyncOutcome> hitObserver) {
    this.statsStore = statsStore;
    this.storeReader = storeReader;
    this.counter = counter;
    this.hitObserver = hitObserver;
  }

  /**
   * Outcome of the store rungs for one planner batch: the results served so far ({@code resolved},
   * in ladder-insertion order), the targets no store rung could serve ({@code stillMissing}, ready
   * for the orchestrator's capture rung), the batch diagnostics the capture rung records onto and
   * the orchestrator emits exactly once, and the normalized pinned-generation token ({@code ""}
   * when the pin froze none).
   */
  record Resolution(
      Map<String, StatsResolutionResult> resolved,
      List<StatsCaptureRequest> stillMissing,
      PlannerLookupDiagnostics diagnostics,
      String pinnedGeneration) {}

  /**
   * Store rungs of the planner batch ladder — pinned/primary generation, then newest gap-fill —
   * leaving capture to the caller. Callers guarantee a non-empty batch sharing one table/snapshot;
   * the full ladder contract is documented on {@link
   * StatsOrchestrator#resolvePlannerBatchInGeneration(java.util.List, Optional, java.util.Map,
   * long)}.
   */
  Resolution resolveFromStore(
      List<StatsCaptureRequest> requests,
      Optional<String> pinnedGenerationToken,
      Map<String, Predicate<TargetStatsRecord>> completenessByStorageId) {
    // All requests must share the same tableId and snapshotId (grouped upstream by TableWork).
    StatsCaptureRequest first = requests.get(0);
    String pinnedGeneration = pinnedGenerationToken.filter(token -> !token.isBlank()).orElse("");
    java.util.function.Function<String, java.util.function.Predicate<TargetStatsRecord>>
        completenessFor = key -> completenessByStorageId.getOrDefault(key, record -> true);
    PlannerLookupDiagnostics diagnostics = new PlannerLookupDiagnostics();
    // Primary reader: the pinned generation when the pin froze one, else the live/newest
    // generation.
    TargetBatchReader primaryBatch =
        pinnedGeneration.isBlank()
            ? statsStore::getTargetStatsBatch
            : (tableId, snapshotId, targets) ->
                statsStore.getTargetStatsBatchInGeneration(
                    tableId, snapshotId, pinnedGeneration, targets);
    TargetReader primaryTarget =
        pinnedGeneration.isBlank()
            ? statsStore::getTargetStats
            : (tableId, snapshotId, target) ->
                statsStore.getTargetStatsInGeneration(
                    tableId, snapshotId, pinnedGeneration, target);

    java.util.Map<String, StatsResolutionResult> out =
        new java.util.LinkedHashMap<>(requests.size());
    java.util.List<StatsCaptureRequest> primaryRequests = new java.util.ArrayList<>(requests);

    // 1. Primary store read: the pinned generation (query-consistent), or live/newest if no pin.
    java.util.Map<String, StatsResolutionResult> primaryHits =
        readPlannerBatchIsolated(
            first.tableId(),
            first.snapshotId(),
            primaryRequests,
            primaryBatch,
            primaryTarget,
            StatsResolutionResult::hit);

    // Pinned records that exist but fail their completeness predicate: candidates the newest
    // gap-fill may replace, and the answer of last resort if it cannot — a partial record is
    // still a hit (the planner degrades per stat), never a stale/capture trigger.
    java.util.Map<String, TargetStatsRecord> partialPinned = new java.util.LinkedHashMap<>();
    java.util.List<StatsCaptureRequest> misses = new java.util.ArrayList<>();
    for (StatsCaptureRequest req : primaryRequests) {
      String key = storageId(req);
      StatsResolutionResult hit = primaryHits.get(key);
      if (hit != null && hit.hasStats()) {
        TargetStatsRecord record = hit.stats().get();
        boolean satisfies = completenessFor.apply(key).test(record);
        if (satisfies || pinnedGeneration.isBlank()) {
          // Complete — or partial with no pin, where the primary IS the newest generation and no
          // richer same-snapshot source exists: serve as-is, the planner degrades per stat.
          servePlannerHit(
              out,
              diagnostics,
              key,
              record,
              satisfies ? PlannerLookupOutcome.PRIMARY_HIT : PlannerLookupOutcome.PARTIAL);
        } else {
          partialPinned.put(key, record);
          misses.add(req);
        }
      } else if (hit != null && hit.outcome() == StatsSyncOutcome.FAILED) {
        if (pinnedGeneration.isBlank()) {
          diagnostics.record(PlannerLookupOutcome.FAILED);
          out.put(key, hit);
        } else {
          // A pinned-generation read failure (e.g. an unreadable frozen manifest) must not zero
          // planning quality for the batch: the newest generation of the same snapshot is an
          // independent read path with no frozen manifest involved, so fall through to the
          // gap-fill. If that fails too, ITS failure is the terminal one.
          misses.add(req);
        }
      } else {
        misses.add(req);
      }
    }

    // 2. Newest gap-fill — only when a specific generation was pinned. Serves targets the pinned
    // generation lacks (prevents NOT_FOUND) or holds only partially (prevents a needless
    // downgrade), from the newest generation of the SAME snapshot.
    java.util.List<StatsCaptureRequest> afterFill = new java.util.ArrayList<>();
    if (!pinnedGeneration.isBlank() && !misses.isEmpty()) {
      java.util.Map<String, StatsResolutionResult> fillHits =
          readPlannerBatchIsolated(
              first.tableId(),
              first.snapshotId(),
              misses,
              statsStore::getTargetStatsBatch,
              statsStore::getTargetStats,
              StatsResolutionResult::hit);
      for (StatsCaptureRequest req : misses) {
        String key = storageId(req);
        StatsResolutionResult hit = fillHits.get(key);
        TargetStatsRecord partial = partialPinned.get(key);
        boolean newestSatisfies =
            hit != null && hit.hasStats() && completenessFor.apply(key).test(hit.stats().get());
        if (newestSatisfies) {
          servePlannerHit(
              out, diagnostics, key, hit.stats().get(), PlannerLookupOutcome.NEWEST_FILL);
        } else if (partial != null) {
          // Newest is no more complete than the pin: between equally incomplete records,
          // consistency prefers the pinned generation the scan reads.
          servePlannerHit(out, diagnostics, key, partial, PlannerLookupOutcome.PARTIAL);
        } else if (hit != null && hit.hasStats()) {
          // Pin has nothing at all; a partial newest record beats stale or capture.
          servePlannerHit(out, diagnostics, key, hit.stats().get(), PlannerLookupOutcome.PARTIAL);
        } else if (hit != null && hit.outcome() == StatsSyncOutcome.FAILED) {
          diagnostics.record(PlannerLookupOutcome.FAILED);
          out.put(key, hit);
        } else {
          afterFill.add(req);
        }
      }
    } else {
      afterFill.addAll(misses);
    }

    return new Resolution(out, List.copyOf(afterFill), diagnostics, pinnedGeneration);
  }

  /** Reads only the pinned generation, treating an unreadable frozen manifest as a miss. */
  Optional<TargetStatsRecord> resolvePinnedFromStore(
      StatsCaptureRequest request, String pinnedGeneration) {
    try {
      return statsStore.getTargetStatsInGeneration(
          request.tableId(), request.snapshotId(), pinnedGeneration, request.target());
    } catch (BaseResourceRepository.AbortRetryableException | StorageAbortRetryableException e) {
      throw e;
    } catch (RuntimeException e) {
      // A frozen manifest may be temporarily unreadable even though the live generation is still
      // available. Treat that generation as a miss; callers can then use the normal newest ladder.
      LOG.debugf(
          e, "pinned-generation read failed for %s; falling through to newest", storageId(request));
      return Optional.empty();
    }
  }

  /**
   * Store rungs of the single-target planner lookup: the pinned generation for the pinned snapshot
   * (query-consistent; a pinned read failure falls through rather than failing the lookup), then
   * the newest (live active) generation only to fill a target the pinned generation lacks. Empty
   * means no store rung could serve; the caller decides whether to capture.
   */
  Optional<TargetStatsRecord> resolveSingleFromStore(
      StatsCaptureRequest request, Optional<String> pinnedGenerationToken) {
    String pinnedGeneration = pinnedGenerationToken.filter(token -> !token.isBlank()).orElse("");

    // Primary: the pinned generation (query-consistent), or live/newest when the pin froze none.
    Optional<TargetStatsRecord> primary;
    if (pinnedGeneration.isBlank()) {
      primary = storeReader.apply(request);
    } else {
      primary = resolvePinnedFromStore(request, pinnedGeneration);
    }
    if (primary.isPresent()) {
      return primary;
    }

    // Newest gap-fill — only when a specific generation was pinned; the pinned generation lacks
    // this target, so the newest generation backstops it before capture.
    if (!pinnedGeneration.isBlank()) {
      return storeReader.apply(request);
    }
    return Optional.empty();
  }

  /**
   * Serve one resolved planner target, count the ladder rung that produced it, and emit the hit.
   */
  private void servePlannerHit(
      java.util.Map<String, StatsResolutionResult> out,
      PlannerLookupDiagnostics diagnostics,
      String key,
      TargetStatsRecord record,
      PlannerLookupOutcome outcome) {
    hitObserver.accept(StatsSyncOutcome.HIT);
    diagnostics.record(outcome);
    out.put(key, StatsResolutionResult.hit(record));
  }

  private Map<String, StatsResolutionResult> readPlannerBatchIsolated(
      ResourceId tableId,
      long snapshotId,
      List<StatsCaptureRequest> requests,
      TargetBatchReader batchReader,
      TargetReader targetReader,
      Function<TargetStatsRecord, StatsResolutionResult> hitMapper) {
    if (requests == null || requests.isEmpty()) {
      return Map.of();
    }

    List<StatsTarget> targets = targetsOf(requests);
    try {
      Map<String, Optional<TargetStatsRecord>> batchHits =
          batchReader.read(tableId, snapshotId, targets);
      Map<String, StatsResolutionResult> out = new LinkedHashMap<>();
      for (StatsCaptureRequest request : requests) {
        String key = storageId(request);
        Optional<TargetStatsRecord> hit = batchHits == null ? Optional.empty() : batchHits.get(key);
        out.put(
            key,
            hit != null && hit.isPresent()
                ? hitMapper.apply(hit.get())
                : StatsResolutionResult.skipped("batch_store_miss"));
      }
      return java.util.Collections.unmodifiableMap(out);
    } catch (BaseResourceRepository.AbortRetryableException | StorageAbortRetryableException e) {
      throw e;
    } catch (StatsStore.GenerationUnavailableException generationError) {
      LOG.debugf(
          generationError,
          "planner stats generation unavailable table=%s snapshot=%s; skipping target isolation",
          tableId,
          snapshotId);
      Map<String, StatsResolutionResult> out = new LinkedHashMap<>();
      String message =
          generationError.getMessage() == null
              ? "stats generation unavailable"
              : generationError.getMessage();
      for (StatsCaptureRequest request : requests) {
        out.put(storageId(request), StatsResolutionResult.failed(message));
      }
      return java.util.Collections.unmodifiableMap(out);
    } catch (RuntimeException batchError) {
      if (requests.size() == 1) {
        return readPlannerTargetIsolated(
            tableId, snapshotId, requests.get(0), targetReader, hitMapper, batchError);
      }
      int mid = requests.size() / 2;
      Map<String, StatsResolutionResult> out = new LinkedHashMap<>();
      out.putAll(
          readPlannerBatchIsolated(
              tableId, snapshotId, requests.subList(0, mid), batchReader, targetReader, hitMapper));
      out.putAll(
          readPlannerBatchIsolated(
              tableId,
              snapshotId,
              requests.subList(mid, requests.size()),
              batchReader,
              targetReader,
              hitMapper));
      return java.util.Collections.unmodifiableMap(out);
    }
  }

  private Map<String, StatsResolutionResult> readPlannerTargetIsolated(
      ResourceId tableId,
      long snapshotId,
      StatsCaptureRequest request,
      TargetReader targetReader,
      Function<TargetStatsRecord, StatsResolutionResult> hitMapper,
      RuntimeException batchError) {
    String key = storageId(request);
    try {
      Optional<TargetStatsRecord> record = targetReader.read(tableId, snapshotId, request.target());
      return Map.of(
          key,
          record.map(hitMapper).orElseGet(() -> StatsResolutionResult.skipped("batch_store_miss")));
    } catch (BaseResourceRepository.AbortRetryableException | StorageAbortRetryableException e) {
      throw e;
    } catch (RuntimeException targetError) {
      LOG.debugf(
          targetError,
          "planner stats target read failed after batch isolation table=%s target=%s",
          tableId,
          key);
      String message =
          targetError.getMessage() == null ? batchError.getMessage() : targetError.getMessage();
      return Map.of(key, StatsResolutionResult.failed(message));
    }
  }

  private static List<StatsTarget> targetsOf(List<StatsCaptureRequest> requests) {
    return requests.stream().map(StatsCaptureRequest::target).toList();
  }

  static String storageId(StatsCaptureRequest request) {
    return StatsTargetIdentity.storageId(request.target());
  }

  @FunctionalInterface
  private interface TargetBatchReader {
    Map<String, Optional<TargetStatsRecord>> read(
        ResourceId tableId, long snapshotId, List<StatsTarget> targets);
  }

  @FunctionalInterface
  private interface TargetReader {
    Optional<TargetStatsRecord> read(ResourceId tableId, long snapshotId, StatsTarget target);
  }

  /**
   * The ladder rung that resolved (or failed) one planner target — the diagnostics vocabulary for
   * {@link StatsOrchestrator#resolvePlannerBatchInGeneration}. Emitted per target as a {@code
   * result}-tagged count of {@code PLANNER_LOOKUP_OUTCOMES_TOTAL} and summarized in one DEBUG line
   * per table batch, so "which rung served this query's stats" is a metric query or a log grep, not
   * archaeology.
   */
  enum PlannerLookupOutcome {
    /** The primary generation (pinned, or live when unpinned) satisfied the need. */
    PRIMARY_HIT,
    /** The pinned generation lacked the target or capability; the newest generation satisfied. */
    NEWEST_FILL,
    /** Served a record that does not satisfy the full need (planner degrades per stat). */
    PARTIAL,
    /** Missing everywhere; a bounded sync capture produced the record. */
    CAPTURED,
    /** Missing everywhere; capture is pending (async mode, budget exhausted, or in flight). */
    CAPTURE_PENDING,
    /** A rung failed outright (store error surfaced as FAILED). */
    FAILED
  }

  /**
   * Per-batch outcome accumulator for the planner ladder: counts each target's outcome, then emits
   * them once — as {@code result}-tagged counter increments and a single DEBUG summary line. One
   * instance per {@link #resolveFromStore} call; never shared across threads.
   */
  final class PlannerLookupDiagnostics {
    private final java.util.EnumMap<PlannerLookupOutcome, Integer> counts =
        new java.util.EnumMap<>(PlannerLookupOutcome.class);

    void record(PlannerLookupOutcome outcome) {
      counts.merge(outcome, 1, Integer::sum);
    }

    void emit(ResourceId tableId, long snapshotId, String pinnedGeneration, int requested) {
      for (Map.Entry<PlannerLookupOutcome, Integer> entry : counts.entrySet()) {
        counter.increment(
            ServiceMetrics.Stats.PLANNER_LOOKUP_OUTCOMES_TOTAL,
            entry.getValue(),
            Tag.of(TagKey.RESULT, entry.getKey().name().toLowerCase(java.util.Locale.ROOT)));
      }
      // The greppable per-batch summary: which generation the batch read and how each target
      // resolved. "generation=live" means the pin froze none and the live generation was primary.
      LOG.debugf(
          "planner_stats lookup table=%s snapshot=%d generation=%s requested=%d outcomes=%s",
          tableId.getId(),
          snapshotId,
          pinnedGeneration.isBlank() ? "live" : pinnedGeneration,
          requested,
          counts);
    }
  }
}
