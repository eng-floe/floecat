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

package ai.floedb.floecat.service.statistics.scheduler;

import ai.floedb.floecat.reconciler.jobs.CoverageLevel;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * In-process signal index for scheduler scoring factors.
 *
 * <p>Stores the three scoring signals ({@link #lastSuccessfulCaptureMs}, {@link #coverageLevel},
 * {@link #snapshotDeltaRows}) and two demand counters ({@link #recentTableDemand}, {@link
 * #recentColumnDemand}) required by {@link DefaultSchedulerProfile} and {@link SchedulerContext}.
 * All reads are O(1) and non-blocking; writes are lock-free on the hot path.
 *
 * <h2>Key convention</h2>
 *
 * <p>All signal keys include the account ID to prevent cross-account collisions. Per-table signals
 * use a composite key {@code accountId:tableId}. Per-snapshot signals use {@code
 * accountId:tableId:snapshotId}. Per-column demand keys use {@code accountId:tableId:} followed by
 * the normalized (lowercased, trimmed) column selector.
 *
 * <h2>Coverage closed-flag invariant</h2>
 *
 * <p>Once {@link #recordFinalizedSnapshot} marks a snapshot as {@link CoverageLevel#FULL} and
 * closes it, subsequent {@link #recordPartialCoverage} calls for the same key are silently ignored.
 * This prevents a late write (e.g. a delayed retry) from downgrading a completed snapshot.
 *
 * <h2>Demand window rotation</h2>
 *
 * <p>Demand counters use a two-bucket rolling window. The current and previous buckets are swapped
 * atomically when {@code windowMs} has elapsed since the last rotation. Reads sum both buckets so
 * that demand accumulated in the just-expired window still contributes while it remains recent. If
 * more than {@code 2 × windowMs} has elapsed without any write or read, both buckets are cleared so
 * that stale demand from a long-idle period does not persist.
 *
 * <h2>Memory note</h2>
 *
 * <p>{@code coverageByKey} and {@code deltaByKey} are bounded Caffeine caches with configurable max
 * entries and idle TTL. This prevents unbounded growth at large snapshot cardinality while keeping
 * O(1) lookups and writes on the enqueue hot path.
 */
@ApplicationScoped
public class SchedulerSignalIndex {

  private static final long DEFAULT_SNAPSHOT_SIGNAL_MAX_ENTRIES = 100_000L;
  private static final long DEFAULT_SNAPSHOT_SIGNAL_TTL_MS = 86_400_000L; // 24h

  // ── Per-table last successful capture time ────────────────────────
  // key: accountId + ":" + tableId
  private final ConcurrentHashMap<String, Long> lastSuccessfulMs = new ConcurrentHashMap<>();

  // ── Per-snapshot coverage state ───────────────────────────────────
  // key: accountId + ":" + tableId + ":" + snapshotId
  private final Cache<String, CoverageEntry> coverageByKey;

  // ── Per-snapshot delta row counts ─────────────────────────────────
  // key: accountId + ":" + tableId + ":" + snapshotId
  private final Cache<String, Long> deltaByKey;

  // ── Rolling demand window ─────────────────────────────────────────
  private final AtomicReference<DemandWindow> window;
  private final long windowMs;

  // ── Unknown-rate tracking (read via accessor methods) ─────────────
  private final LongAdder lastCaptureKnown = new LongAdder();
  private final LongAdder lastCaptureUnknown = new LongAdder();
  private final LongAdder coverageKnown = new LongAdder();
  private final LongAdder coverageUnknown = new LongAdder();
  private final LongAdder deltaKnown = new LongAdder();
  private final LongAdder deltaUnknown = new LongAdder();

  @Inject
  SchedulerSignalIndex(
      @ConfigProperty(
              name = "floecat.stats.scheduler.scoring.demand.window-ms",
              defaultValue = "3600000")
          long windowMs,
      @ConfigProperty(
              name = "floecat.stats.scheduler.signals.snapshot.max-entries",
              defaultValue = "100000")
          long snapshotSignalMaxEntries,
      @ConfigProperty(
              name = "floecat.stats.scheduler.signals.snapshot.ttl-ms",
              defaultValue = "86400000")
          long snapshotSignalTtlMs) {
    this.windowMs = windowMs > 0L ? windowMs : 3_600_000L;
    long maxEntries =
        snapshotSignalMaxEntries > 0L
            ? snapshotSignalMaxEntries
            : DEFAULT_SNAPSHOT_SIGNAL_MAX_ENTRIES;
    long ttlMs = snapshotSignalTtlMs > 0L ? snapshotSignalTtlMs : DEFAULT_SNAPSHOT_SIGNAL_TTL_MS;
    this.coverageByKey =
        Caffeine.newBuilder()
            .maximumSize(maxEntries)
            .expireAfterAccess(Duration.ofMillis(ttlMs))
            .build();
    this.deltaByKey =
        Caffeine.newBuilder()
            .maximumSize(maxEntries)
            .expireAfterAccess(Duration.ofMillis(ttlMs))
            .build();
    this.window = new AtomicReference<>(new DemandWindow(System.currentTimeMillis()));
  }

  public SchedulerSignalIndex(long windowMs) {
    this(windowMs, DEFAULT_SNAPSHOT_SIGNAL_MAX_ENTRIES, DEFAULT_SNAPSHOT_SIGNAL_TTL_MS);
  }

  // ── Write methods ─────────────────────────────────────────────────

  /**
   * Records a fully-finalized snapshot: sets {@link CoverageLevel#FULL} (closed) and updates the
   * per-table last-successful-capture timestamp.
   *
   * <p>Must be called only from paths that successfully write stats outputs (e.g. the {@code
   * FINALIZE_SNAPSHOT_CAPTURE} success path after persisting aggregate stats). Paths that skip
   * stats outputs (e.g. no-stats-outputs early-return) must NOT call this method.
   *
   * @param accountId account that owns the table
   * @param tableId table identifier
   * @param snapshotId snapshot that was finalized
   * @param nowMs current epoch-ms timestamp (for testability)
   */
  public void recordFinalizedSnapshot(
      String accountId, String tableId, long snapshotId, long nowMs) {
    String tableKey = tableKey(accountId, tableId);
    lastSuccessfulMs.put(tableKey, nowMs);
    coverageByKey.put(snapshotKey(accountId, tableId, snapshotId), CoverageEntry.FULL_CLOSED);
  }

  /**
   * Records interim partial coverage for a snapshot (e.g. when enqueuing a P2_REPAIR follow-up).
   * Ignored if the snapshot has already been marked {@link CoverageLevel#FULL} and closed.
   */
  public void recordPartialCoverage(String accountId, String tableId, long snapshotId) {
    coverageByKey
        .asMap()
        .compute(
            snapshotKey(accountId, tableId, snapshotId),
            (k, existing) -> {
              if (existing != null && existing.closed) return existing; // closed — never downgrade
              return CoverageEntry.PARTIAL_OPEN;
            });
  }

  /**
   * Records the estimated delta-row count for a snapshot.
   *
   * <p>Must be called <em>before</em> any {@code enqueueFileGroupExecution} calls for the same
   * snapshot so that the signal is visible when the enqueue path reads the context.
   *
   * @param deltaRows empty if the delta cannot be determined (e.g. no {@code total-records} in the
   *     snapshot summary, or no parent snapshot found)
   */
  public void recordSnapshotDelta(
      String accountId, String tableId, long snapshotId, OptionalLong deltaRows) {
    if (deltaRows.isPresent()) {
      deltaByKey.put(snapshotKey(accountId, tableId, snapshotId), deltaRows.getAsLong());
    }
  }

  /**
   * Records a planner demand hit for a table. Called at every stats resolution or planner bundle
   * request for this table.
   */
  public void recordTableDemand(String accountId, String tableId) {
    maybeRotate();
    window
        .get()
        .tableCurrent
        .computeIfAbsent(tableKey(accountId, tableId), k -> new LongAdder())
        .increment();
  }

  /**
   * Records a per-column demand hit. The column selector is normalized (trimmed, lowercased) to
   * prevent alias-drift from inflating the signal map.
   */
  public void recordColumnDemand(String accountId, String tableId, String rawColumnSelector) {
    if (rawColumnSelector == null || rawColumnSelector.isBlank()) return;
    String normalized = rawColumnSelector.trim().toLowerCase(java.util.Locale.ROOT);
    maybeRotate();
    window
        .get()
        .columnCurrent
        .computeIfAbsent(columnKey(accountId, tableId, normalized), k -> new LongAdder())
        .increment();
  }

  // ── Read methods (non-blocking, O(1)) ─────────────────────────────

  /**
   * Returns the last successful stats capture epoch-ms for the table, or empty if not yet recorded.
   *
   * @param tableKey compound {@code accountId:tableId} key
   */
  public OptionalLong lastSuccessfulCaptureMs(String tableKey) {
    Long ts = lastSuccessfulMs.get(tableKey);
    if (ts == null) {
      lastCaptureUnknown.increment();
      return OptionalLong.empty();
    }
    lastCaptureKnown.increment();
    return OptionalLong.of(ts);
  }

  /**
   * Returns the coverage level for the given (table, snapshot) pair. Defaults to {@link
   * CoverageLevel#NONE} when no record exists (conservative — biases toward capturing).
   *
   * @param tableKey compound {@code accountId:tableId} key
   * @param snapshotId snapshot identifier
   */
  public CoverageLevel coverageLevel(String tableKey, long snapshotId) {
    // Reconstruct the full snapshot key from the compound tableKey.
    // tableKey is already "accountId:tableId"; snapshotKey storage uses "accountId:tableId:sid".
    CoverageEntry entry = coverageByKey.getIfPresent(tableKey + ':' + snapshotId);
    if (entry == null) {
      coverageUnknown.increment();
      return CoverageLevel.NONE;
    }
    coverageKnown.increment();
    return entry.level;
  }

  /**
   * Returns the estimated delta row count for the given (table, snapshot) pair, or empty if not
   * recorded.
   *
   * @param tableKey compound {@code accountId:tableId} key
   * @param snapshotId snapshot identifier
   */
  public OptionalLong snapshotDeltaRows(String tableKey, long snapshotId) {
    Long rows = deltaByKey.getIfPresent(tableKey + ':' + snapshotId);
    if (rows == null) {
      deltaUnknown.increment();
      return OptionalLong.empty();
    }
    deltaKnown.increment();
    return OptionalLong.of(rows);
  }

  /**
   * Returns the number of planner demand hits for the table in the current + previous window.
   *
   * @param tableKey compound {@code accountId:tableId} key
   */
  public long recentTableDemand(String tableKey) {
    maybeRotate();
    DemandWindow w = window.get();
    return count(w.tableCurrent, tableKey) + count(w.tablePrevious, tableKey);
  }

  /**
   * Returns the number of planner demand hits for the column in the current + previous window. The
   * {@code normalizedColumnSelector} must already be trimmed/lowercased (use {@link
   * #recordColumnDemand} to write, which normalizes automatically).
   *
   * @param tableKey compound {@code accountId:tableId} key
   * @param normalizedColumnSelector column selector (normalized)
   */
  public long recentColumnDemand(String tableKey, String normalizedColumnSelector) {
    maybeRotate();
    DemandWindow w = window.get();
    String key = tableKey + ':' + normalizedColumnSelector;
    return count(w.columnCurrent, key) + count(w.columnPrevious, key);
  }

  // ── Unknown-rate accessors (for ReconcileQueueMetrics) ─────────────

  /** Drains and returns the count of signal reads that found a real value for last-capture. */
  public long drainLastCaptureKnown() {
    return lastCaptureKnown.sumThenReset();
  }

  /** Drains and returns the count of signal reads that found no last-capture record. */
  public long drainLastCaptureUnknown() {
    return lastCaptureUnknown.sumThenReset();
  }

  /** Drains and returns the count of coverage reads that returned PARTIAL or FULL. */
  public long drainCoverageKnown() {
    return coverageKnown.sumThenReset();
  }

  /** Drains and returns the count of coverage reads that defaulted to NONE. */
  public long drainCoverageUnknown() {
    return coverageUnknown.sumThenReset();
  }

  /** Drains and returns the count of delta reads that found a real value. */
  public long drainDeltaKnown() {
    return deltaKnown.sumThenReset();
  }

  /** Drains and returns the count of delta reads that found no record. */
  public long drainDeltaUnknown() {
    return deltaUnknown.sumThenReset();
  }

  // Test-only observability helpers.
  long coverageEntryCount() {
    coverageByKey.cleanUp();
    return coverageByKey.estimatedSize();
  }

  long deltaEntryCount() {
    deltaByKey.cleanUp();
    return deltaByKey.estimatedSize();
  }

  // ── Key builders ──────────────────────────────────────────────────

  /** Builds the compound per-table key: {@code accountId:tableId}. */
  public static String tableKey(String accountId, String tableId) {
    return accountId + ':' + tableId;
  }

  private static String snapshotKey(String accountId, String tableId, long snapshotId) {
    return accountId + ':' + tableId + ':' + snapshotId;
  }

  private static String columnKey(String accountId, String tableId, String normalizedSelector) {
    return accountId + ':' + tableId + ':' + normalizedSelector;
  }

  // ── Window rotation ────────────────────────────────────────────────

  private void maybeRotate() {
    DemandWindow current = window.get();
    long now = System.currentTimeMillis();
    long elapsed = now - current.startMs;
    if (elapsed < windowMs) return;

    DemandWindow next;
    if (elapsed >= 2L * windowMs) {
      // Long idle gap — clear both buckets to avoid stale demand persisting.
      next = new DemandWindow(now);
    } else {
      // Normal rotation — move current to previous, open a fresh current bucket.
      next =
          new DemandWindow(
              new ConcurrentHashMap<>(),
              current.tableCurrent,
              new ConcurrentHashMap<>(),
              current.columnCurrent,
              now);
    }
    window.compareAndSet(current, next); // only one thread wins; others see the new window
  }

  private static long count(ConcurrentHashMap<String, LongAdder> map, String key) {
    LongAdder adder = map.get(key);
    return adder == null ? 0L : adder.sum();
  }

  // ── Inner types ───────────────────────────────────────────────────

  /** Coverage state for a single (account, table, snapshot) triple. */
  static final class CoverageEntry {
    final CoverageLevel level;

    /** When true, no further writes will downgrade this entry (FULL is terminal). */
    final boolean closed;

    static final CoverageEntry PARTIAL_OPEN = new CoverageEntry(CoverageLevel.PARTIAL, false);
    static final CoverageEntry FULL_CLOSED = new CoverageEntry(CoverageLevel.FULL, true);

    private CoverageEntry(CoverageLevel level, boolean closed) {
      this.level = level;
      this.closed = closed;
    }
  }

  /** Immutable snapshot of the two-bucket demand window. */
  static final class DemandWindow {
    final ConcurrentHashMap<String, LongAdder> tableCurrent;
    final ConcurrentHashMap<String, LongAdder> tablePrevious;
    final ConcurrentHashMap<String, LongAdder> columnCurrent;
    final ConcurrentHashMap<String, LongAdder> columnPrevious;
    final long startMs;

    /** Creates a fully empty window (used for long-idle-gap rotation). */
    DemandWindow(long startMs) {
      this(
          new ConcurrentHashMap<>(),
          new ConcurrentHashMap<>(),
          new ConcurrentHashMap<>(),
          new ConcurrentHashMap<>(),
          startMs);
    }

    DemandWindow(
        ConcurrentHashMap<String, LongAdder> tableCurrent,
        ConcurrentHashMap<String, LongAdder> tablePrevious,
        ConcurrentHashMap<String, LongAdder> columnCurrent,
        ConcurrentHashMap<String, LongAdder> columnPrevious,
        long startMs) {
      this.tableCurrent = tableCurrent;
      this.tablePrevious = tablePrevious;
      this.columnCurrent = columnCurrent;
      this.columnPrevious = columnPrevious;
      this.startMs = startMs;
    }
  }
}
