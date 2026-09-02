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

/**
 * A cached value that knows its own retained size.
 *
 * <p>This is the seam that keeps {@link CacheWeights} free of domain types: anything whose cost
 * lives in private aggregate structure reports its own size rather than being introspected by the
 * weigher. Values that do not implement it are weighed generically.
 */
public interface WeightedValue {

  /** Retained bytes for this value, excluding the cache's own per-entry overhead. */
  long estimatedWeightBytes();
}
