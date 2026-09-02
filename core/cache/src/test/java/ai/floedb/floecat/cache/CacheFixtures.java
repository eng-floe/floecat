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

package ai.floedb.floecat.cache;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** The value, key weight, budgets and event recorder the cache tests share. */
final class CacheFixtures {

  private CacheFixtures() {}

  /** Comfortably more than any test puts in, so nothing evicts unless a test means it to. */
  static final long AMPLE_BUDGET = 1024L * 1024L;

  /** Small enough that a few hundred entries overrun it, which is what the eviction tests need. */
  static final long TIGHT_BUDGET = 4_000L;

  /**
   * Two keys that share a fence stripe, so a write to the first moves the fence a load of the
   * second is watching. That lets a single-threaded test land a write "during" a load without
   * depending on eviction, which is not deterministic.
   *
   * <p>Coupled to {@code CaffeineMemoryCache.stripeFor} and {@code FENCE_STRIPES}: change either
   * and these stop colliding. {@code theSharedStripeFixtureStillCollides} is what says so, rather
   * than leaving the tests that rely on it to fail as though the fence were broken.
   */
  static final String STRIPE_SHARER_A = "acct/1/table/a";

  /** The other half of {@link #STRIPE_SHARER_A}. */
  static final String STRIPE_SHARER_B = "acct/1/table/38";

  /** A cached value that carries a version and knows its own size, like a real one. */
  record Versioned(String value, long version) implements WeightedValue {
    @Override
    public long estimatedWeightBytes() {
      return 2L * value.length() + Long.BYTES;
    }
  }

  /** Two bytes a character, which is what a String key costs. */
  static long keyWeight(String key) {
    return 2L * key.length();
  }

  static MemoryCache<String, Versioned> cache(long maxBytes) {
    return cache(maxBytes, CacheEvents.none());
  }

  /**
   * A cache over any value type, for tests about weighing, the fence, or a value with another
   * shape.
   */
  static <V> CaffeineMemoryCache<String, V> cacheForAnyValue(CacheEvents events) {
    return new CaffeineMemoryCache<>(
        CacheFamily.POINTER, AMPLE_BUDGET, CacheFixtures::keyWeight, events);
  }

  static MemoryCache<String, Versioned> cache(long maxBytes, CacheEvents events) {
    return new CaffeineMemoryCache<>(
        CacheFamily.POINTER, maxBytes, CacheFixtures::keyWeight, events);
  }

  /**
   * Every event, counted. No empty overrides: the interface defaults each method to a no-op, so a
   * test asserts on the fields it cares about and ignores the rest.
   */
  static final class RecordingEvents implements CacheEvents {
    int hits;
    int misses;
    int failures;
    int evictions;
    int loadsDiscarded;
    long evictedBytes;
    final List<Duration> loadTimes = new ArrayList<>();

    final List<Duration> hitTimes = new ArrayList<>();

    @Override
    public void hit(Duration served) {
      hits++;
      hitTimes.add(served);
    }

    @Override
    public void miss() {
      misses++;
    }

    @Override
    public void loadTime(Duration elapsed) {
      loadTimes.add(elapsed);
    }

    @Override
    public void loadFailed(Duration elapsed, RuntimeException error) {
      failures++;
    }

    @Override
    public void loadDiscarded() {
      loadsDiscarded++;
    }

    @Override
    public void evicted(long weightBytes) {
      evictions++;
      evictedBytes += weightBytes;
    }

    void reset() {
      hits = 0;
      misses = 0;
      failures = 0;
      evictions = 0;
      loadsDiscarded = 0;
      evictedBytes = 0L;
      loadTimes.clear();
      hitTimes.clear();
    }
  }

  /**
   * A protobuf message that reports its size the way one above 2 GB does: negative, because
   * protobuf reports it as an int. Only {@code getSerializedSize} is reachable from the weigher.
   */
  static final class OversizedMessage implements com.google.protobuf.MessageLite {

    @Override
    public int getSerializedSize() {
      return -1;
    }

    @Override
    public com.google.protobuf.Parser<? extends com.google.protobuf.MessageLite>
        getParserForType() {
      throw new UnsupportedOperationException();
    }

    @Override
    public com.google.protobuf.ByteString toByteString() {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] toByteArray() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(com.google.protobuf.CodedOutputStream output) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(java.io.OutputStream output) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writeDelimitedTo(java.io.OutputStream output) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder newBuilderForType() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder toBuilder() {
      throw new UnsupportedOperationException();
    }

    @Override
    public com.google.protobuf.MessageLite getDefaultInstanceForType() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isInitialized() {
      return true;
    }
  }
}
