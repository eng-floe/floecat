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
import ai.floedb.floecat.service.concurrent.Futures;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Adapts the legacy completed-pin map contract to the resolver's single-flight cache. */
final class LegacyCurrentSnapshotPinCaches {

  private LegacyCurrentSnapshotPinCaches() {}

  /** Snapshot a caller-owned completed-pin cache without exposing it to resolution workers. */
  static Map<ResourceId, TablePin> snapshot(Map<ResourceId, TablePin> completedPins) {
    synchronized (completedPins) {
      return new LinkedHashMap<>(completedPins);
    }
  }

  /** Convert a completed-pin snapshot into the resolver's single-flight representation. */
  static QueryInputResolver.CurrentSnapshotPinCache singleFlight(
      Map<ResourceId, TablePin> completedPins) {
    QueryInputResolver.CurrentSnapshotPinCache cache =
        new QueryInputResolver.CurrentSnapshotPinCache();
    completedPins.forEach(
        (tableId, pin) -> cache.entries().put(tableId, CompletableFuture.completedFuture(pin)));
    return cache;
  }

  /** Restore a caller cache after a failed copy, recording rollback failures on the cause. */
  static boolean restore(
      Map<ResourceId, TablePin> completedPins,
      Map<ResourceId, TablePin> initialPins,
      Throwable copyFailure) {
    synchronized (completedPins) {
      if (completedPins.equals(initialPins)) {
        return true;
      }
      try {
        completedPins.clear();
        completedPins.putAll(initialPins);
      } catch (RuntimeException | Error rollbackFailure) {
        copyFailure.addSuppressed(rollbackFailure);
      }
      return completedPins.equals(initialPins);
    }
  }

  /** Copy successful single-flight entries into a caller-owned completed-pin map. */
  static void copySuccessful(
      QueryInputResolver.CurrentSnapshotPinCache cache, Map<ResourceId, TablePin> completedPins) {
    synchronized (completedPins) {
      successful(cache).forEach(completedPins::put);
    }
  }

  private static Map<ResourceId, TablePin> successful(
      QueryInputResolver.CurrentSnapshotPinCache cache) {
    Map<ResourceId, TablePin> completed = new LinkedHashMap<>();
    cache
        .entries()
        .forEach(
            (tableId, pinFuture) -> {
              if (pinFuture.isDone()
                  && !pinFuture.isCompletedExceptionally()
                  && !pinFuture.isCancelled()) {
                completed.put(tableId, Futures.join(pinFuture));
              }
            });
    return completed;
  }
}
