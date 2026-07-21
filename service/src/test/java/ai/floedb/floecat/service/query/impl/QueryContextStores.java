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
package ai.floedb.floecat.service.query.impl;

/** Test factory for a real, initialized {@link QueryContextStoreImpl} outside this package. */
public final class QueryContextStores {

  private QueryContextStores() {}

  /** A store with sane test defaults (60s TTL, 100 entries, 60s resolving-pin grace). */
  public static QueryContextStoreImpl forTesting() {
    QueryContextStoreImpl store = new QueryContextStoreImpl();
    store.defaultTtlMs = 60_000L;
    store.endedGraceMs = 15_000L;
    store.maxSize = 100L;
    store.safetyExpiryMinutes = 10L;
    store.resolvingPinGraceMs = 60_000L;
    store.init();
    return store;
  }

  /** As {@link #forTesting()} but with an explicit size cap (to exercise eviction weighting). */
  public static QueryContextStoreImpl forTesting(long maxSize) {
    QueryContextStoreImpl store = new QueryContextStoreImpl();
    store.defaultTtlMs = 60_000L;
    store.endedGraceMs = 15_000L;
    store.maxSize = maxSize;
    store.safetyExpiryMinutes = 10L;
    store.resolvingPinGraceMs = 60_000L;
    store.init();
    return store;
  }
}
