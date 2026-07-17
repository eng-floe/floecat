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

import ai.floedb.floecat.common.rpc.ResourceId;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.jboss.logging.Logger;

/**
 * Query-path reporting of broken table-root state into the {@link RootResyncQueue}, so the periodic
 * re-drive converges tables whose roots a read found missing or internally inconsistent — instead
 * of every query failing the same way forever with nothing scheduled to repair it.
 *
 * <p>The queue's marker was originally only written by the transactional resync path; a root broken
 * by anything else (a swept blob under a live pointer, a migration that landed on legacy data, a
 * manifest missing its currency) had no writer to converge it. Reads reporting here extend the same
 * durable re-drive to any observed breakage: {@code resyncFromCommittedState} re-derives the root —
 * definition ref, snapshot manifest, currency — from committed state, so it heals every shape of
 * breakage whose underlying data still exists, and converges to a deleted root when it does not.
 *
 * <p>Two properties keep this safe on the query path:
 *
 * <ul>
 *   <li><b>Rate-limited per table.</b> A broken table fails every query against it (incidents show
 *       dozens of identical failures per second), while one durable marker is all the re-drive
 *       needs. The first observer in each window enqueues; the rest are suppressed.
 *   <li><b>Best-effort, never throws.</b> Repair bookkeeping must not mask or replace the query's
 *       own diagnostic error; an enqueue failure is logged and the suppression released so a later
 *       query retries the report.
 * </ul>
 *
 * <p>The suppression map keeps one {@code (account/table) -> timestamp} entry per table ever
 * reported broken — a handful of tiny entries in practice, never per-query growth.
 */
@ApplicationScoped
public class RootRepairRequests {

  private static final Logger LOG = Logger.getLogger(RootRepairRequests.class);

  /**
   * One report per table per window. Well above any query rate, well below the re-drive cadence: a
   * marker the GC pass could not clear stays durable regardless, so re-reporting sooner buys
   * nothing.
   */
  private static final long SUPPRESSION_WINDOW_NANOS = TimeUnit.MINUTES.toNanos(5);

  /** Null when disabled: reports become no-ops (graphs wired without a pointer store, in tests). */
  private final RootResyncQueue resyncQueue;

  private final LongSupplier nanoClock;
  private final ConcurrentHashMap<String, Long> lastReported = new ConcurrentHashMap<>();

  @Inject
  public RootRepairRequests(RootResyncQueue resyncQueue) {
    this(resyncQueue, System::nanoTime);
  }

  RootRepairRequests(RootResyncQueue resyncQueue, LongSupplier nanoClock) {
    this.resyncQueue = resyncQueue;
    this.nanoClock = nanoClock;
  }

  /** A no-op instance for object graphs wired without a pointer store (test-only constructors). */
  public static RootRepairRequests disabled() {
    return new RootRepairRequests(null, System::nanoTime);
  }

  /**
   * Reports the table's root as observed-broken, durably enqueueing it for the periodic resync
   * re-drive. Fire-and-forget: rate-limited per table and swallowing enqueue failures, so callers
   * on the query path invoke it right before raising their own error without changing what the
   * client sees.
   */
  public void request(ResourceId tableId) {
    if (resyncQueue == null) {
      return;
    }
    String key = tableId.getAccountId() + "/" + tableId.getId();
    long now = nanoClock.getAsLong();
    boolean[] firstInWindow = {false};
    // Atomic claim of the window so concurrent failing queries elect exactly one reporter.
    // Comparison by difference: nanoTime is only meaningful subtractively (it may be negative).
    lastReported.compute(
        key,
        (k, prev) -> {
          if (prev != null && now - prev < SUPPRESSION_WINDOW_NANOS) {
            return prev;
          }
          firstInWindow[0] = true;
          return now;
        });
    if (!firstInWindow[0]) {
      return;
    }
    try {
      resyncQueue.enqueue(tableId);
    } catch (RuntimeException e) {
      // Release the claimed window (only if still ours) so the next failing query retries the
      // report instead of the table sitting unrepaired for the full window.
      lastReported.remove(key, now);
      LOG.warnf(e, "failed to enqueue root repair for table %s", tableId.getId());
    }
  }
}
