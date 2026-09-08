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

import com.google.protobuf.MessageLite;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;

/** Retained-heap estimates for caches built on {@link MemoryCache}. */
public final class CacheWeights {

  // Caffeine's per-entry machinery before any payload: the node, the key and value references, the
  // hash, and the per-entry eviction bookkeeping. Counting it makes a byte budget also an entry
  // budget -- ten million entries is 1.25 GB before any metadata. Excludes the key's own bytes,
  // which arrive as keyBytes.
  private static final long ENTRY_OVERHEAD_BYTES = 125L;
  // A slot in a map or a list plus the node around it: the reference, and for a HashMap the Node
  // with its hash and next pointer. Charged per element so that a collection of ten thousand tiny
  // values weighs its structure rather than only its contents.
  private static final long CONTAINER_ENTRY_OVERHEAD_BYTES = 32L;

  // Decoded protobuf against its serialized size, from the blob cache's own measurements: object
  // headers, boxed fields and the String/ByteString copies a parse allocates come to roughly 3x.
  // The direction matters more than the digit -- charging the serialized size alone would let a
  // cache hold three times the heap its budget says it does.
  private static final long RETAINED_PROTO_FACTOR = 3L;

  // A boxed Long, Double or Boolean, or an enum reference: the box plus the reference to it.
  private static final long BOXED_SCALAR_BYTES = 32L;

  private CacheWeights() {}

  public static long entry(Object value, long keyBytes) {
    if (keyBytes < 0) {
      throw new UnweighableValueException(
          "key weight is negative: "
              + keyBytes
              + "; a key cannot claim space back from the budget");
    }
    long size = plus(ENTRY_OVERHEAD_BYTES, keyBytes);
    // No visited set yet. Only a container can cycle, and the shapes this contract steers callers
    // to -- WeightedValue, protobuf -- never descend, so allocating one per weigh would put half a
    // kilobyte on every insert and on every eviction, the latter under the cache's eviction lock.
    return plus(size, value(value, null));
  }

  /**
   * Adds without wrapping. A wrapped sum reads negative and weighs an enormous entry as one byte,
   * letting it escape eviction; an implausible size must read as maximally expensive, never free.
   */
  private static long plus(long left, long right) {
    long sum = left + right;
    return ((left ^ sum) & (right ^ sum)) < 0 ? Long.MAX_VALUE : sum;
  }

  /**
   * Records {@code container} as walked, creating the guard on the first one. Returns {@code null}
   * if it was already walked, which is the cycle.
   */
  private static IdentityHashMap<Object, Boolean> seen(
      IdentityHashMap<Object, Boolean> visited, Object container) {
    var walked = visited == null ? new IdentityHashMap<Object, Boolean>() : visited;
    return walked.put(container, Boolean.TRUE) == null ? walked : null;
  }

  private static long value(Object value, IdentityHashMap<Object, Boolean> visited) {
    if (value == null) {
      return 0L;
    }
    if (value instanceof WeightedValue weighted) {
      long declared = weighted.estimatedWeightBytes();
      if (declared < 0) {
        // Refused for the same reason an unwalkable shape is: a value that under-reports its size
        // makes the budget wrong in the direction that exhausts the heap. Flooring to zero would
        // charge it entry machinery alone -- the flat default this class exists not to apply.
        throw new UnweighableValueException(
            value.getClass().getName() + " declares a negative weight: " + declared);
      }
      return declared;
    }
    if (value instanceof MessageLite message) {
      int serialized = message.getSerializedSize();
      if (serialized < 0) {
        // Protobuf reports its size as an int, so a message above 2 GB reports it negative. Left
        // alone it makes the whole entry weigh negative, and Caffeine's weigher then throws a bare
        // IllegalArgumentException that reads as a failed store read rather than a value defect.
        throw new UnweighableValueException(
            message.getClass().getName() + " reports a negative serialized size: " + serialized);
      }
      return RETAINED_PROTO_FACTOR * serialized;
    }
    if (value instanceof CharSequence text) {
      return 2L * text.length();
    }
    if (value instanceof byte[] bytes) {
      return bytes.length;
    }
    if (value instanceof Map<?, ?> map) {
      // Only a container can cycle, so only a container is recorded; marking leaves too would grow
      // this to one entry per element, on the insert path and again under the eviction lock. The
      // cost is that a leaf referenced twice in one container is charged twice -- the safe
      // direction here.
      visited = seen(visited, value);
      if (visited == null) {
        return 0L;
      }
      long size = 0L;
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        size = plus(size, CONTAINER_ENTRY_OVERHEAD_BYTES);
        size = plus(size, value(entry.getKey(), visited));
        size = plus(size, value(entry.getValue(), visited));
      }
      return size;
    }
    if (value instanceof Collection<?> collection) {
      visited = seen(visited, value);
      if (visited == null) {
        return 0L;
      }
      long size = 0L;
      for (Object item : collection) {
        size = plus(size, CONTAINER_ENTRY_OVERHEAD_BYTES);
        size = plus(size, value(item, visited));
      }
      return size;
    }
    // Before the boxed-scalar branch: these are Numbers of unbounded size, and the flat scalar
    // figure would weigh a megabyte of magnitude as 32 bytes.
    if (value instanceof java.math.BigInteger big) {
      return BOXED_SCALAR_BYTES + big.bitLength() / Byte.SIZE;
    }
    if (value instanceof java.math.BigDecimal big) {
      return BOXED_SCALAR_BYTES + big.unscaledValue().bitLength() / Byte.SIZE;
    }
    if (value instanceof Number || value instanceof Boolean || value instanceof Enum<?>) {
      return BOXED_SCALAR_BYTES;
    }
    // Not a flat default: a record, a POJO or any array but byte[] would be charged it with none of
    // its fields walked, so a value retaining megabytes weighs a kilobyte -- the budget wrong in
    // the direction that exhausts the heap. A type this cannot weigh has to say what it costs.
    throw new UnweighableValueException(
        "cannot weigh "
            + value.getClass().getName()
            + ": implement WeightedValue, or use a shape CacheWeights walks (protobuf, CharSequence,"
            + " byte[], Map, Collection, boxed scalar)");
  }
}
