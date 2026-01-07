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

package ai.floedb.floecat.gateway.iceberg.rest.services.staging;

import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record StagedTableEntry(
    StagedTableKey key,
    ResourceId catalogId,
    ResourceId namespaceId,
    TableRequests.Create request,
    TableSpec spec,
    List<Map<String, Object>> requirements,
    StageState state,
    Instant createdAt,
    Instant updatedAt,
    String idempotencyKey) {

  public StagedTableEntry {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(catalogId, "catalogId");
    Objects.requireNonNull(namespaceId, "namespaceId");
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(spec, "spec");
    requirements = requirements == null ? List.of() : List.copyOf(requirements);
    state = state == null ? StageState.STAGED : state;
  }

  public StagedTableEntry initializeTimestamps(Instant now) {
    Instant created = createdAt == null ? now : createdAt;
    return new StagedTableEntry(
        key,
        catalogId,
        namespaceId,
        request,
        spec,
        requirements,
        state,
        created,
        now,
        idempotencyKey);
  }

  public StagedTableEntry withState(StageState newState) {
    return new StagedTableEntry(
        key,
        catalogId,
        namespaceId,
        request,
        spec,
        requirements,
        newState,
        createdAt,
        Instant.now(),
        idempotencyKey);
  }
}
