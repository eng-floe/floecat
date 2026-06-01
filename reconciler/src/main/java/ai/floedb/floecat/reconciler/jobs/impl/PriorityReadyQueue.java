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
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Priority-aware in-memory ready queue for the {@link
 * ai.floedb.floecat.reconciler.jobs.impl.InMemoryReconcileJobStore}.
 *
 * <p>Maintains four buckets — one per {@link StatsPriorityClass} — each backed by a {@link
 * ConcurrentSkipListMap} keyed by inverted score ({@code Long.MAX_VALUE - score}). This ensures
 * that within a class the job with the highest score appears at the map head and is dispatched
 * first.
 *
 * <p>The public API is thread-safe. Individual operations are not atomically composed, so callers
 * must not rely on consistent reads across multiple method calls.
 */
public final class PriorityReadyQueue {

  /** Per-class bucket: invertedScore → FIFO deque of jobIds at that score. */
  private final Map<StatsPriorityClass, ConcurrentSkipListMap<Long, ConcurrentLinkedDeque<String>>>
      buckets;

  /**
   * Tracks each enqueued job's location — {@code [classOrdinal, invertedScore]} — so that {@link
   * #remove(String)} can locate the right bucket without a full scan.
   */
  private final ConcurrentHashMap<String, long[]> jobMeta = new ConcurrentHashMap<>();

  public PriorityReadyQueue() {
    buckets = new EnumMap<>(StatsPriorityClass.class);
    for (StatsPriorityClass cls : StatsPriorityClass.values()) {
      buckets.put(cls, new ConcurrentSkipListMap<>());
    }
  }

  /**
   * Adds {@code jobId} to the bucket for {@code cls} at {@code score}. If the job was previously
   * enqueued in a different class or at a different score, that stale entry is left in place until
   * it is polled; callers should only enqueue a job once (or call {@link #remove} first if
   * re-enqueueing after a class change).
   */
  public void enqueue(String jobId, StatsPriorityClass cls, long score) {
    StatsPriorityClass effectiveCls = cls == null ? StatsPriorityClass.P3_BACKGROUND : cls;
    long invertedScore = Long.MAX_VALUE - Math.max(0L, score);
    buckets
        .get(effectiveCls)
        .computeIfAbsent(invertedScore, k -> new ConcurrentLinkedDeque<>())
        .add(jobId);
    jobMeta.put(jobId, new long[] {effectiveCls.ordinal(), invertedScore});
  }

  /**
   * Removes and returns the highest-score job from the bucket for {@code cls}, or {@code null} if
   * the bucket is empty. Empty deques at a given score are cleaned up lazily.
   */
  public String pollHighest(StatsPriorityClass cls) {
    var bucket = buckets.get(cls);
    while (true) {
      Map.Entry<Long, ConcurrentLinkedDeque<String>> entry = bucket.firstEntry();
      if (entry == null) {
        return null;
      }
      ConcurrentLinkedDeque<String> deque = entry.getValue();
      String jobId = deque.poll();
      if (jobId != null) {
        jobMeta.remove(jobId);
        return jobId;
      }
      // Deque is empty — remove it and try the next score level.
      bucket.remove(entry.getKey(), deque);
    }
  }

  /**
   * Removes a specific job from its bucket. No-op if the job is not currently enqueued. This is an
   * O(n) deque scan for the specific score level but is only called on cancel/completion paths.
   */
  public void remove(String jobId) {
    long[] meta = jobMeta.remove(jobId);
    if (meta == null) {
      return;
    }
    StatsPriorityClass cls = StatsPriorityClass.values()[(int) meta[0]];
    long invertedScore = meta[1];
    ConcurrentLinkedDeque<String> deque = buckets.get(cls).get(invertedScore);
    if (deque != null) {
      deque.remove(jobId);
    }
  }

  /** Total number of jobs across all classes. */
  public int size() {
    int total = 0;
    for (ConcurrentSkipListMap<Long, ConcurrentLinkedDeque<String>> bucket : buckets.values()) {
      for (ConcurrentLinkedDeque<String> deque : bucket.values()) {
        total += deque.size();
      }
    }
    return total;
  }

  /** Jobs ready in the given class. */
  public long sizeByClass(StatsPriorityClass cls) {
    long total = 0L;
    for (ConcurrentLinkedDeque<String> deque : buckets.get(cls).values()) {
      total += deque.size();
    }
    return total;
  }

  /** Per-class counts as an {@link EnumMap}. */
  public Map<StatsPriorityClass, Long> sizeByAllClasses() {
    EnumMap<StatsPriorityClass, Long> result = new EnumMap<>(StatsPriorityClass.class);
    for (StatsPriorityClass cls : StatsPriorityClass.values()) {
      result.put(cls, sizeByClass(cls));
    }
    return result;
  }
}
