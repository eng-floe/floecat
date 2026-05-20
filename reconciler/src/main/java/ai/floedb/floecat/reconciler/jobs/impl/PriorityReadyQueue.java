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

package ai.floedb.floecat.reconciler.jobs.impl;

import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Priority-aware ready-job queue for the reconcile scheduler.
 *
 * <p>Jobs are organized into four buckets — one per {@link StatsPriorityClass}. Within each bucket
 * jobs are ordered by score: higher score is dispatched first. Ties are broken by insertion order
 * (FIFO within the same score value).
 *
 * <h2>Data structure</h2>
 *
 * Each bucket is a {@link ConcurrentSkipListMap}{@code <Long, String>}. The map key is a composite
 * long that encodes both priority and insertion order:
 *
 * <pre>
 *   key = (Integer.MAX_VALUE - clampedScore) &lt;&lt; 32  |  (seqNum &amp; 0xFFFFFFFFL)
 * </pre>
 *
 * <ul>
 *   <li>Upper 32 bits: {@code Integer.MAX_VALUE - clampedScore}. A higher score produces a
 *       <em>smaller</em> upper value, placing it at the map head.
 *   <li>Lower 32 bits: a monotonically increasing global sequence number. Earlier-enqueued jobs
 *       with the same score appear before later-enqueued ones (FIFO tie-break).
 * </ul>
 *
 * <h2>Dispatch operation</h2>
 *
 * {@link ConcurrentSkipListMap#pollFirstEntry()} atomically removes the entry with the smallest key
 * (= highest priority). There are no shared mutable deques, so there is no window between "get
 * deque reference" and "add item" where a concurrent consumer could remove the deque and silently
 * drop the job.
 *
 * <h2>Score clamping</h2>
 *
 * Scores are clamped to {@code [0, Integer.MAX_VALUE]} before key construction. In practice the
 * 3-factor scoring model produces values in the range {@code [0, 600]}, so no clamping occurs under
 * normal operation.
 *
 * <h2>Concurrency</h2>
 *
 * All methods are safe to call concurrently from many threads without external synchronisation. The
 * {@link #sizeByClass} counters are approximate (may lag by at most one in-flight operation) and
 * are intended for metrics only.
 *
 * <h2>Dispatch contract</h2>
 *
 * The caller ({@code InMemoryReconcileJobStore.leaseNext()}) is responsible for iterating over
 * priority classes in urgency order (P0→P1→P2→P3) and calling {@link #pollHighest} for each class
 * until a dispatchable job is found. This class does not enforce ordering across classes.
 */
public final class PriorityReadyQueue {

  /**
   * Global sequence counter. Shared across all classes so that sequence numbers are globally
   * unique, which guarantees a total order even for jobs that are re-queued across classes (e.g.
   * during starvation aging).
   */
  private final AtomicLong seqGen = new AtomicLong(0);

  /**
   * One skip-list map per priority class. Each entry: key = composite priority long (see class
   * Javadoc), value = job identifier.
   */
  private final EnumMap<StatsPriorityClass, ConcurrentSkipListMap<Long, String>> buckets;

  /** Approximate size counter per class. Used for metrics only. */
  private final EnumMap<StatsPriorityClass, AtomicLong> sizes;

  public PriorityReadyQueue() {
    buckets = new EnumMap<>(StatsPriorityClass.class);
    sizes = new EnumMap<>(StatsPriorityClass.class);
    for (StatsPriorityClass cls : StatsPriorityClass.values()) {
      buckets.put(cls, new ConcurrentSkipListMap<>());
      sizes.put(cls, new AtomicLong(0));
    }
  }

  /**
   * Adds {@code jobId} to the bucket for {@code cls} at the given {@code score}.
   *
   * <p>Jobs with higher scores are dispatched before jobs with lower scores within the same class.
   * If {@code score} is negative it is treated as zero.
   *
   * @param jobId non-null job identifier
   * @param cls priority class; must not be null
   * @param score non-negative urgency score; higher = dispatched first
   */
  public void enqueue(String jobId, StatsPriorityClass cls, long score) {
    buckets.get(cls).put(compositeKey(score), jobId);
    sizes.get(cls).incrementAndGet();
  }

  /**
   * Returns and removes the highest-scored job in {@code cls}, or {@code null} if that class is
   * empty.
   *
   * <p>Among jobs with the same score, the one enqueued first is returned (FIFO tie-break). The
   * removal is atomic: no concurrent enqueue or poll can observe an intermediate state.
   *
   * @param cls the class to poll from
   * @return a job identifier, or {@code null} if the class bucket is empty
   */
  public String pollHighest(StatsPriorityClass cls) {
    Map.Entry<Long, String> entry = buckets.get(cls).pollFirstEntry();
    if (entry != null) {
      sizes.get(cls).decrementAndGet();
      return entry.getValue();
    }
    return null;
  }

  /**
   * Re-enqueues a job that was polled but could not be dispatched (e.g. lane-blocked or failed
   * lease check). Semantically equivalent to {@link #enqueue}; provided for clarity at call sites.
   *
   * @param jobId job to put back
   * @param cls the class it was originally polled from
   * @param score the same score used at original enqueue
   */
  public void requeue(String jobId, StatsPriorityClass cls, long score) {
    enqueue(jobId, cls, score);
  }

  /**
   * Returns the approximate number of jobs in {@code cls}.
   *
   * <p>The value may lag by at most one in-flight {@link #enqueue}/{@link #pollHighest} operation.
   * It is intended for metrics and health-band evaluation only — do not use it for correctness
   * decisions.
   */
  public long sizeByClass(StatsPriorityClass cls) {
    return sizes.get(cls).get();
  }

  /**
   * Returns a snapshot of approximate sizes for all priority classes.
   *
   * <p>The returned map is unmodifiable. Each value is subject to the same approximation caveat as
   * {@link #sizeByClass}.
   */
  public Map<StatsPriorityClass, Long> sizeByAllClasses() {
    EnumMap<StatsPriorityClass, Long> snapshot = new EnumMap<>(StatsPriorityClass.class);
    for (StatsPriorityClass cls : StatsPriorityClass.values()) {
      snapshot.put(cls, sizes.get(cls).get());
    }
    return Collections.unmodifiableMap(snapshot);
  }

  /**
   * Returns the total number of jobs across all classes.
   *
   * <p>Subject to the same approximation caveat as {@link #sizeByClass}.
   */
  public long totalSize() {
    long total = 0;
    for (StatsPriorityClass cls : StatsPriorityClass.values()) {
      total += sizes.get(cls).get();
    }
    return total;
  }

  // ---------------------------------------------------------------------------
  // Key construction
  // ---------------------------------------------------------------------------

  /**
   * Builds the composite skip-list key for a given score.
   *
   * <pre>
   *   upper 32 bits = Integer.MAX_VALUE - clamp(score, 0, Integer.MAX_VALUE)
   *   lower 32 bits = next sequence number (monotonically increasing)
   * </pre>
   *
   * <p>A higher {@code score} produces a smaller upper value, so the resulting key is smaller and
   * {@link ConcurrentSkipListMap#pollFirstEntry()} returns it first. The sequence number breaks
   * ties in insertion order (FIFO).
   */
  private long compositeKey(long score) {
    long clamped = Math.max(0L, Math.min(score, Integer.MAX_VALUE));
    long upper = (long) Integer.MAX_VALUE - clamped;
    long lower = seqGen.getAndIncrement() & 0xFFFFFFFFL;
    return (upper << 32) | lower;
  }
}
