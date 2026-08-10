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

import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.QueryPins;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Owns transient GC-root registrations created during one input-resolution attempt. Retained pins
 * stay registered until the caller commits them; discarded pins and failed attempts release only
 * the registrations created by this owner.
 */
final class ResolvingPinRoots {
  private final QueryContextStore queryStore;
  private final String queryId;
  private final Map<TablePin, List<String>> rootsByPin = new IdentityHashMap<>();
  private boolean terminal;

  ResolvingPinRoots(QueryContextStore queryStore, String queryId) {
    this.queryStore = queryStore;
    this.queryId = queryId;
  }

  /** Register a pin once; a store failure leaves it untracked for normal propagation. */
  synchronized void register(TablePin pin) {
    if (terminal || queryStore == null || queryId == null || queryId.isEmpty() || pin == null) {
      return;
    }
    if (rootsByPin.containsKey(pin)) {
      return;
    }
    List<String> roots = QueryPins.gcRootUris(pin);
    if (roots.isEmpty()) {
      return;
    }
    queryStore.registerResolvingPinBlobs(queryId, roots);
    rootsByPin.put(pin, roots);
  }

  /** Release the registration for a compatible pin that lost ordered first-touch. */
  synchronized void discard(TablePin pin) {
    if (terminal) {
      return;
    }
    List<String> roots = rootsByPin.get(pin);
    if (roots != null) {
      queryStore.releaseResolvingPinBlobs(queryId, roots);
      rootsByPin.remove(pin);
    }
  }

  /**
   * Close ownership and release all remaining registrations. Closing first prevents a late worker
   * from adding roots after the cleanup sweep.
   */
  synchronized void releaseAll() {
    terminal = true;
    var iterator = rootsByPin.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<TablePin, List<String>> entry = iterator.next();
      queryStore.releaseResolvingPinBlobs(queryId, entry.getValue());
      iterator.remove();
    }
  }
}
