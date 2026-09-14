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

package ai.floedb.floecat.service.repo.cache;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The durable, always-consistent pointer view account assignment fences against. A type rather than
 * a per-call choice, which is what the store-view rule asks for.
 */
public final class DurablePointerReads {
  private final PointerStore store;

  public DurablePointerReads(PointerStore store) {
    this.store = Objects.requireNonNull(store, "store");
  }

  /** Reads one committed pointer, bypassing the planner index. */
  public Optional<Pointer> read(String key) {
    return store.getConsistent(key);
  }

  /** Reads committed pointers for a batch of keys. */
  public Map<String, Pointer> readBatch(List<String> keys) {
    return store.getBatchConsistent(keys);
  }

  /** Commits an assignment record — the fence take, or the member index — unfenced by design. */
  public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
    return store.compareAndSet(key, expectedVersion, next);
  }
}
