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

package ai.floedb.floecat.service.gc;

/**
 * Bounded membership index used by CAS GC mark epochs.
 *
 * <p>The interface deliberately exposes no iterator: callers may ask whether a key was marked, but
 * an implementation does not have to retain the keys. A future disk-backed exact index can
 * implement the same contract.
 */
public interface ReferenceIndex extends AutoCloseable {

  /**
   * Marks {@code key}. Implementations must throw rather than silently lose an insertion when they
   * cannot safely accept it.
   */
  void add(String key) throws CapacityExceededException;

  /** Returns false only when {@code key} was definitely not marked. */
  boolean mightContain(String key);

  /** Number of distinct-looking insertions accepted by this epoch. */
  long insertions();

  /** Configured maximum number of distinct-looking insertions. */
  long capacity();

  /** Fraction of the bounded backing store currently occupied, in {@code [0, 1]}. */
  double saturation();

  /** Current estimated false-positive probability. */
  double estimatedFalsePositiveProbability();

  /** Fixed backing-memory size, excluding the small index object itself. */
  long memoryBytes();

  @Override
  default void close() {}

  final class CapacityExceededException extends RuntimeException {
    public CapacityExceededException(String message) {
      super(message);
    }
  }
}
