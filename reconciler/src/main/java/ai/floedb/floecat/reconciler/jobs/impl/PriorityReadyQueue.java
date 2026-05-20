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
import java.util.Map.Entry;
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
 * Each bucket is a {@link ConcurrentSkipListMap}{@code <QKey, String>}. {@link QKey} is a two-field
 * comparable record:
 *
 * <pre>
 *   QKey(invertedScore = Integer.MAX_VALUE - clamp(score, 0, Integer.MAX_VALUE),
 *        seq          = seqGen.getAndIncrement())
 * </pre>
 *
 * <ul>
 *   <li>{@code invertedScore}: a higher job score produces a <em>smaller</em> inverted value,
 *       placing the entry at the map head.
 *   <li>{@code seq}: a monotonically increasing 64-bit global sequence. Because {@code long} holds
 *       2<sup>63</sup> positive values, this counter cannot realistically overflow in any
 *       deployment (1 billion enqueues per second would take 292 years). Sequence is never masked
 *       or truncated.
 * </ul>
 *
 * <h2>Why QKey instead of a bit-packed Long</h2>
 *
 * An earlier design packed {@code (invertedScore << 32 | seq & 0xFFFFFFFFL)} into a single {@code
 * long}. That approach silently overwrites existing entries after 2<sup>32</sup> enqueues for the
 * same score bucket, because {@link ConcurrentSkipListMap#put} replaces the value on key collision.
 * At Floescan scale (10M+ EXEC_FILE_GROUP jobs per large snapshot) this is a realistic correctness
 * risk. {@link QKey} eliminates it: the 64-bit {@code seq} field guarantees a unique key for every
 * enqueue regardless of score.
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
   * Collision-safe ordering key for a single entry in a priority bucket.
   *
   * <p>Ordering: smaller {@code invertedScore} = higher job score = dispatched first. Ties broken
   * by {@code seq} (earlier enqueue = smaller seq = dispatched first).
   *
   * <p>Java records generate correct {@code equals}/{@code hashCode} from both fields, though
   * {@link ConcurrentSkipListMap} uses only {@link Comparable#compareTo} for ordering and does not
   * call {@code equals}.
   */
  record QKey(long invertedScore, long seq) implements Comparable<QKey> {
    @Override
    public int compareTo(QKey o) {
      int c = Long.compare(invertedScore, o.invertedScore);
      return c != 0 ? c : Long.compare(seq, o.seq);
    }
  }

  /**
   * One skip-list map per priority class. Each entry: key = {@link QKey} (see class Javadoc), value
   * = job identifier.
   */
  private final EnumMap<StatsPriorityClass, ConcurrentSkipListMap<QKey, String>> buckets;

  /** Approximate size counter per class. Used for metrics only. */
  private final EnumMap<StatsPriorityClass, AtomicLong> sizes;

  public PriorityReadyQueue() {
    buckets = new EnumMap<>(StatsPriorityClass.class);
    sizes = new EnumMap<>(StatsPriorityClass.class);
    for (StatsPriorityClass cls : StatsPriorityClass.values()) {
      buckets.put(cls, new ConcurrentSkipListMap<QKey, String>());
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
    buckets.get(cls).put(qKey(score), jobId);
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
    Entry<QKey, String> entry = buckets.get(cls).pollFirstEntry();
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
   * Builds a collision-safe {@link QKey} for the given score.
   *
   * <p>A higher {@code score} produces a smaller {@code invertedScore}, placing the entry at the
   * skip-list head so that {@link ConcurrentSkipListMap#pollFirstEntry()} returns it first. The
   * {@code seq} field is the raw 64-bit value from {@link #seqGen} — it is never masked or
   * truncated, so keys are globally unique for the lifetime of this queue instance.
   */
  private QKey qKey(long score) {
    long clamped = Math.max(0L, Math.min(score, Integer.MAX_VALUE));
    return new QKey((long) Integer.MAX_VALUE - clamped, seqGen.getAndIncrement());
  }
}
