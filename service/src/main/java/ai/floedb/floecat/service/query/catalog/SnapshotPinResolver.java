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
 * distributed under the License is distributed on an "AS-IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.query.catalog;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.impl.QueryContext;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.LongFunction;

final class SnapshotPinResolver {

  private final QueryContextStore queryStore;
  private final QueryContext initialCtx;
  private final String queryId;
  private final String correlationId;

  SnapshotPinResolver(QueryContextStore queryStore, QueryContext ctx, String correlationId) {
    this.queryStore = queryStore;
    this.initialCtx = ctx;
    this.queryId = ctx.getQueryId();
    this.correlationId = correlationId;
  }

  OptionalLong pinnedSnapshotId(ResourceId tableId) {
    Optional<SnapshotPin> pin = liveContext().findSnapshotPin(tableId, correlationId);
    return pin.isPresent() ? OptionalLong.of(pin.get().getSnapshotId()) : OptionalLong.empty();
  }

  <T> Optional<T> withPinnedSnapshot(
      ResourceId tableId, LongFunction<Optional<T>> snapshotScopedLookup) {
    return liveContext()
        .findSnapshotPin(tableId, correlationId)
        .flatMap(pin -> snapshotScopedLookup.apply(pin.getSnapshotId()));
  }

  private QueryContext liveContext() {
    return queryStore.get(queryId).orElse(initialCtx);
  }
}
