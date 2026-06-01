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

import ai.floedb.floecat.stats.spi.CoverageLevel;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * In-process cache for three scoring signals and two rolling demand counters.
 *
 * <p>All operations are O(1) and non-blocking — no I/O is performed. The index is updated by
 * write-side hooks (finalize executor, stats orchestrator) and read by the scheduler priority
 * policy during enqueue scoring.
 *
 * <p>The demand window uses a two-bucket rolling approach: current bucket accumulates live counts,
 * previous bucket retains the prior window's counts. Reads sum both buckets so demand estimates
 * decay gracefully rather than dropping to zero at window boundaries.
 */
@ApplicationScoped
public class SchedulerSignalIndex {

  // ---- per-table last capture timestamp -----------------------------------------------
  /** Key: {@code accountId:tableId} → epoch-ms of most recent finalized snapshot. */
  private final ConcurrentHashMap<String, Long> lastSuccessfulMs = new ConcurrentHashMap<>();

  // ---- per-(table,snapshot) coverage --------------------------------------------------
  /**
   * Key: {@code accountId:tableId:snapshotId} → coverage entry. Bounded with LRU eviction to
   * prevent unbounded growth at high snapshot cardinality (e.g. 500k tables × 10 snapshots). Size
   * is configurable via {@code floecat.stats.scheduler.signals.snapshot.max-entries}.
   */
  private final Cache<String, CoverageEntry> coverageBySnapshot;

  // ---- per-(table,snapshot) delta rows ------------------------------------------------
  /** Key: {@code accountId:tableId:snapshotId} → delta row count. Bounded same as coverage. */
  private final Cache<String, Long> deltaBySnapshot;

  // ---- rolling two-bucket demand window -----------------------------------------------
  private final AtomicReference<DemandWindow> window;

  // ---- drain counters (reset-and-return for metrics export) ---------------------------
  private final LongAdder lastCaptureKnown = new LongAdder();
  private final LongAdder lastCaptureUnknown = new LongAdder();
  private final LongAdder coverageKnown = new LongAdder();
  private final LongAdder coverageUnknown = new LongAdder();
  private final LongAdder deltaKnown = new LongAdder();
  private final LongAdder deltaUnknown = new LongAdder();

  private final long windowMs;

  @Inject
  public SchedulerSignalIndex(
      @ConfigProperty(
              name = "floecat.stats.scheduler.scoring.demand.window-ms",
              defaultValue = "3600000")
          long windowMs,
      @ConfigProperty(
              name = "floecat.stats.scheduler.signals.snapshot.max-entries",
              defaultValue = "100000")
          long maxSnapshotEntries) {
    this.windowMs = windowMs;
    this.window = new AtomicReference<>(new DemandWindow(System.currentTimeMillis()));
    long effectiveMax = maxSnapshotEntries > 0L ? maxSnapshotEntries : 100_000L;
    this.coverageBySnapshot = Caffeine.newBuilder().maximumSize(effectiveMax).build();
    this.deltaBySnapshot = Caffeine.newBuilder().maximumSize(effectiveMax).build();
  }

  // ---- static key builders ------------------------------------------------------------

  /** Builds the table-level composite key used in read/write calls. */
  public static String tableKey(String accountId, String tableId) {
    return accountId + ':' + tableId;
  }

  private static String snapshotKey(String accountId, String tableId, long snapshotId) {
    return accountId + ':' + tableId + ':' + snapshotId;
  }

  // ---- write methods ------------------------------------------------------------------

  /**
   * Records a successfully finalized snapshot: updates last-capture timestamp and marks coverage as
   * FULL/CLOSED for the snapshot.
   */
  public void recordFinalizedSnapshot(
      String accountId, String tableId, long snapshotId, long nowMs) {
    String tKey = tableKey(accountId, tableId);
    String sKey = snapshotKey(accountId, tableId, snapshotId);
    lastSuccessfulMs.put(tKey, nowMs);
    coverageBySnapshot.put(sKey, CoverageEntry.FULL_CLOSED);
    lastCaptureKnown.increment();
    coverageKnown.increment();
  }

  /**
   * Records partial coverage for a snapshot. Ignored if coverage is already FULL/CLOSED (closed
   * state never downgrades).
   */
  public void recordPartialCoverage(String accountId, String tableId, long snapshotId) {
    String sKey = snapshotKey(accountId, tableId, snapshotId);
    coverageBySnapshot
        .asMap()
        .compute(
            sKey,
            (k, existing) -> {
              if (existing != null && existing.closed) {
                return existing; // never downgrade FULL → PARTIAL
              }
              return CoverageEntry.PARTIAL_OPEN;
            });
    coverageKnown.increment();
  }

  /**
   * Records the delta row count for a snapshot. If {@code deltaRows} is empty the unknown counter
   * is incremented instead.
   */
  public void recordSnapshotDelta(
      String accountId, String tableId, long snapshotId, OptionalLong deltaRows) {
    if (deltaRows.isPresent()) {
      String sKey = snapshotKey(accountId, tableId, snapshotId);
      deltaBySnapshot.put(sKey, deltaRows.getAsLong());
      deltaKnown.increment();
    } else {
      deltaUnknown.increment();
    }
  }

  /** Records one table-level demand event (e.g. a resolve() call for this table). */
  public void recordTableDemand(String accountId, String tableId) {
    String tKey = tableKey(accountId, tableId);
    DemandWindow w = maybeRotate(System.currentTimeMillis());
    w.currentTableDemand().computeIfAbsent(tKey, k -> new LongAdder()).increment();
  }

  /**
   * Records one column-level demand event. Key is the table key plus the normalized column
   * selector.
   */
  public void recordColumnDemand(String accountId, String tableId, String columnSelector) {
    String colKey = tableKey(accountId, tableId) + ':' + normalize(columnSelector);
    DemandWindow w = maybeRotate(System.currentTimeMillis());
    w.currentColumnDemand().computeIfAbsent(colKey, k -> new LongAdder()).increment();
  }

  // ---- read methods -------------------------------------------------------------------

  /** Returns the epoch-ms of the last successful capture, or empty if never captured. */
  public OptionalLong lastSuccessfulCaptureMs(String tableKey) {
    Long val = lastSuccessfulMs.get(tableKey);
    return val == null ? OptionalLong.empty() : OptionalLong.of(val);
  }

  /**
   * Returns the coverage level for a (table, snapshot) pair. Returns {@link CoverageLevel#NONE} if
   * no coverage has been recorded.
   */
  public CoverageLevel coverageLevel(String tableKey, long snapshotId) {
    // tableKey is already accountId:tableId; snapshotId is appended directly.
    String sKey = tableKey + ':' + snapshotId;
    CoverageEntry entry = coverageBySnapshot.getIfPresent(sKey);
    if (entry == null) {
      return CoverageLevel.NONE;
    }
    return entry.level;
  }

  /** Returns the recorded delta row count for a (table, snapshot) pair, or empty if unknown. */
  public OptionalLong snapshotDeltaRows(String tableKey, long snapshotId) {
    String sKey = tableKey + ':' + snapshotId;
    Long val = deltaBySnapshot.getIfPresent(sKey);
    return val == null ? OptionalLong.empty() : OptionalLong.of(val);
  }

  /** Returns the sum of current + previous window demand for this table key. */
  public long recentTableDemand(String tableKey) {
    DemandWindow w = window.get();
    return w.recentTableCount(tableKey);
  }

  /** Returns the sum of current + previous window demand for this column selector. */
  public long recentColumnDemand(String tableKey, String normalizedSelector) {
    String colKey = tableKey + ':' + normalizedSelector;
    DemandWindow w = window.get();
    return w.recentColumnCount(colKey);
  }

  // ---- drain methods ------------------------------------------------------------------

  /** Resets and returns the last-capture-known counter (for metrics export). */
  public long drainLastCaptureKnown() {
    return getAndReset(lastCaptureKnown);
  }

  /** Resets and returns the last-capture-unknown counter. */
  public long drainLastCaptureUnknown() {
    return getAndReset(lastCaptureUnknown);
  }

  /** Resets and returns the coverage-known counter. */
  public long drainCoverageKnown() {
    return getAndReset(coverageKnown);
  }

  /** Resets and returns the coverage-unknown counter. */
  public long drainCoverageUnknown() {
    return getAndReset(coverageUnknown);
  }

  /** Resets and returns the delta-known counter. */
  public long drainDeltaKnown() {
    return getAndReset(deltaKnown);
  }

  /** Resets and returns the delta-unknown counter. */
  public long drainDeltaUnknown() {
    return getAndReset(deltaUnknown);
  }

  // ---- window rotation ----------------------------------------------------------------

  /**
   * Rotates the demand window if the current window has expired. Uses CAS to avoid duplicate
   * rotation under concurrent calls; exactly one caller wins and performs the rotation.
   *
   * @return the (potentially new) current window snapshot
   */
  private DemandWindow maybeRotate(long nowMs) {
    DemandWindow current = window.get();
    long elapsed = nowMs - current.startedAtMs();
    if (elapsed < windowMs) {
      return current;
    }
    DemandWindow next;
    if (elapsed < 2L * windowMs) {
      // Normal rotation: previous ← current, new current bucket
      next =
          new DemandWindow(
              current.currentTableDemand(),
              new ConcurrentHashMap<>(),
              current.currentColumnDemand(),
              new ConcurrentHashMap<>(),
              nowMs);
    } else {
      // Long idle gap — discard both buckets entirely
      next = new DemandWindow(nowMs);
    }
    // Only one thread wins the CAS; losers re-read and use the winner's window.
    if (window.compareAndSet(current, next)) {
      return next;
    }
    return window.get();
  }

  // ---- helpers ------------------------------------------------------------------------

  private static long getAndReset(LongAdder adder) {
    return adder.sumThenReset();
  }

  private static String normalize(String selector) {
    return selector == null ? "" : selector.trim().toLowerCase(java.util.Locale.ROOT);
  }

  // ---- inner types --------------------------------------------------------------------

  /** Immutable coverage entry. Once closed (FULL), never downgrades to PARTIAL. */
  private static final class CoverageEntry {
    static final CoverageEntry PARTIAL_OPEN = new CoverageEntry(CoverageLevel.PARTIAL, false);
    static final CoverageEntry FULL_CLOSED = new CoverageEntry(CoverageLevel.FULL, true);

    final CoverageLevel level;

    /** When {@code true} this entry is in its terminal state and must not be replaced. */
    final boolean closed;

    private CoverageEntry(CoverageLevel level, boolean closed) {
      this.level = level;
      this.closed = closed;
    }
  }

  /**
   * Two-bucket rolling demand window.
   *
   * <p>Demand counts accumulate in the current buckets. On rotation the current maps become the
   * previous maps, and fresh empty maps become current. Reads sum both buckets so signal values
   * decay over two window widths rather than dropping sharply.
   */
  private record DemandWindow(
      ConcurrentHashMap<String, LongAdder> currentTableDemand,
      ConcurrentHashMap<String, LongAdder> previousTableDemand,
      ConcurrentHashMap<String, LongAdder> currentColumnDemand,
      ConcurrentHashMap<String, LongAdder> previousColumnDemand,
      long startedAtMs) {

    /**
     * Convenience constructor for a brand-new empty window (used at startup or after long idle).
     */
    DemandWindow(long startedAtMs) {
      this(
          new ConcurrentHashMap<>(),
          new ConcurrentHashMap<>(),
          new ConcurrentHashMap<>(),
          new ConcurrentHashMap<>(),
          startedAtMs);
    }

    /** Returns the sum of current + previous table demand for the given key. */
    long recentTableCount(String key) {
      return adderSum(currentTableDemand, key) + adderSum(previousTableDemand, key);
    }

    /** Returns the sum of current + previous column demand for the given key. */
    long recentColumnCount(String key) {
      return adderSum(currentColumnDemand, key) + adderSum(previousColumnDemand, key);
    }

    private static long adderSum(ConcurrentHashMap<String, LongAdder> map, String key) {
      LongAdder adder = map.get(key);
      return adder == null ? 0L : adder.sum();
    }
  }
}
