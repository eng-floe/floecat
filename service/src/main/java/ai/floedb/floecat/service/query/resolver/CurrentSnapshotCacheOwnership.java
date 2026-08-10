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
package ai.floedb.floecat.service.query.resolver;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.TablePin;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

/**
 * Owns the single-flight cache entries inserted by one input-resolution attempt. Failure evicts
 * only this attempt's entries, while terminal cleanup prevents late workers from publishing an
 * unrooted pin after the attempt has ended.
 */
final class CurrentSnapshotCacheOwnership {
  private final ConcurrentMap<ResourceId, CompletableFuture<TablePin>> cache;
  private final Map<ResourceId, CompletableFuture<TablePin>> owned = new LinkedHashMap<>();
  private boolean terminal;

  CurrentSnapshotCacheOwnership(ConcurrentMap<ResourceId, CompletableFuture<TablePin>> cache) {
    this.cache = cache;
  }

  /** Claim a newly inserted holder, or discard it when terminal cleanup already won. */
  synchronized boolean claim(ResourceId tableId, CompletableFuture<TablePin> holder) {
    if (terminal) {
      synchronized (holder) {
        cache.remove(tableId, holder);
        holder.completeExceptionally(
            new CancellationException("input resolution no longer active"));
      }
      return false;
    }
    owned.put(tableId, holder);
    return true;
  }

  /** Forget a holder that its lookup failure already evicted from the shared cache. */
  synchronized void forget(ResourceId tableId, CompletableFuture<TablePin> holder) {
    owned.remove(tableId, holder);
  }

  /**
   * Replace a compatible losing CURRENT entry with the retained first-touch pin before the losing
   * pin relinquishes transient-root ownership. The holder lock makes replacement atomic with a
   * waiter's published-entry check.
   */
  synchronized void replaceCompatiblePin(TablePin losingPin, TablePin retainedPin) {
    ResourceId tableId = losingPin.getTableId();
    CompletableFuture<TablePin> holder = cache.get(tableId);
    if (holder == null
        || !holder.isDone()
        || holder.isCompletedExceptionally()
        || holder.isCancelled()) {
      return;
    }
    synchronized (holder) {
      if (cache.get(tableId) == holder && holder.getNow(null) == losingPin) {
        CompletableFuture<TablePin> replacement = CompletableFuture.completedFuture(retainedPin);
        if (cache.replace(tableId, holder, replacement)) {
          owned.remove(tableId, holder);
          owned.put(tableId, replacement);
        }
      }
    }
  }

  /** Evict and fail every holder still owned by this failed resolution attempt. */
  synchronized void closeAndEvict() {
    terminal = true;
    owned.forEach(
        (tableId, holder) -> {
          synchronized (holder) {
            cache.remove(tableId, holder);
            holder.completeExceptionally(new CancellationException("input resolution failed"));
          }
        });
    owned.clear();
  }
}
