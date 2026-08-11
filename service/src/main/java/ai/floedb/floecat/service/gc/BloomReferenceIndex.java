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

import java.util.Objects;

/** A fixed-size, seed-rotated Bloom-filter implementation of {@link ReferenceIndex}. */
public final class BloomReferenceIndex implements ReferenceIndex {
  private static final double LN_2 = Math.log(2.0d);
  private static final long HASH_STEP = 0x9e3779b97f4a7c15L;

  private final long capacity;
  private final long bitSize;
  private final int hashFunctions;
  private final long seed;
  private final long[] words;
  private long insertions;
  private long setBits;

  public BloomReferenceIndex(long expectedCapacity, double falsePositiveRate, long seed) {
    if (expectedCapacity <= 0L) {
      throw new IllegalArgumentException("expectedCapacity must be positive");
    }
    if (!(falsePositiveRate > 0.0d && falsePositiveRate < 1.0d)) {
      throw new IllegalArgumentException("falsePositiveRate must be between 0 and 1");
    }
    double requestedBits =
        Math.ceil(-expectedCapacity * Math.log(falsePositiveRate) / (LN_2 * LN_2));
    if (!Double.isFinite(requestedBits)
        || requestedBits <= 0.0d
        || requestedBits > (long) Integer.MAX_VALUE * Long.SIZE) {
      throw new IllegalArgumentException("configured Bloom filter is too large");
    }
    this.capacity = expectedCapacity;
    this.bitSize = Math.max(Long.SIZE, (long) requestedBits);
    this.hashFunctions =
        Math.max(1, Math.min(64, (int) Math.round((bitSize / (double) capacity) * LN_2)));
    this.seed = seed;
    this.words = new long[(int) ((bitSize + Long.SIZE - 1L) / Long.SIZE)];
  }

  @Override
  public void add(String key) {
    Objects.requireNonNull(key, "key");
    long h1 = hash(key, seed);
    long h2 = hash(key, seed ^ HASH_STEP) | 1L;
    boolean alreadyPresent = true;
    for (int i = 0; i < hashFunctions; i++) {
      long bit = positiveMod(h1 + (long) i * h2, bitSize);
      long mask = 1L << (bit & 63L);
      if ((words[(int) (bit >>> 6)] & mask) == 0L) {
        alreadyPresent = false;
      }
    }
    if (alreadyPresent) {
      return;
    }
    if (insertions >= capacity) {
      throw new CapacityExceededException(
          "reference index capacity exceeded: capacity=" + capacity);
    }
    for (int i = 0; i < hashFunctions; i++) {
      long bit = positiveMod(h1 + (long) i * h2, bitSize);
      int word = (int) (bit >>> 6);
      long mask = 1L << (bit & 63L);
      if ((words[word] & mask) == 0L) {
        words[word] |= mask;
        setBits++;
      }
    }
    insertions++;
  }

  @Override
  public boolean mightContain(String key) {
    if (key == null) {
      return false;
    }
    long h1 = hash(key, seed);
    long h2 = hash(key, seed ^ HASH_STEP) | 1L;
    for (int i = 0; i < hashFunctions; i++) {
      long bit = positiveMod(h1 + (long) i * h2, bitSize);
      long mask = 1L << (bit & 63L);
      if ((words[(int) (bit >>> 6)] & mask) == 0L) {
        return false;
      }
    }
    return true;
  }

  @Override
  public long insertions() {
    return insertions;
  }

  @Override
  public long capacity() {
    return capacity;
  }

  @Override
  public double saturation() {
    return setBits / (double) bitSize;
  }

  @Override
  public double estimatedFalsePositiveProbability() {
    return Math.pow(saturation(), hashFunctions);
  }

  @Override
  public long memoryBytes() {
    return (long) words.length * Long.BYTES;
  }

  int hashFunctions() {
    return hashFunctions;
  }

  private static long positiveMod(long value, long modulus) {
    long result = value % modulus;
    return result < 0L ? result + modulus : result;
  }

  /** Allocation-free 64-bit hash over the String's UTF-16 code units. */
  private static long hash(String value, long seed) {
    long h = seed ^ ((long) value.length() * HASH_STEP);
    for (int i = 0; i < value.length(); i++) {
      h ^= (long) value.charAt(i) * 0x100000001b3L;
      h = Long.rotateLeft(h, 27) * HASH_STEP + 0x52dce729L;
    }
    h ^= h >>> 33;
    h *= 0xff51afd7ed558ccdL;
    h ^= h >>> 33;
    h *= 0xc4ceb9fe1a85ec53L;
    return h ^ (h >>> 33);
  }
}
