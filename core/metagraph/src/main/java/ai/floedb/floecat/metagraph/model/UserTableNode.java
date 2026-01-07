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

package ai.floedb.floecat.metagraph.model;

import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Immutable table node capturing logical schema and snapshot metadata.
 *
 * <p>The node purposefully limits heavyweight fields (statistics, resolved snapshot sets) to
 * optional references so the cache can keep large catalogs hot without unnecessary churn.
 */
public record UserTableNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    ResourceId catalogId,
    ResourceId namespaceId,
    String displayName,
    TableFormat format,
    String schemaJson,
    Map<String, String> properties,
    List<String> partitionKeys,
    Map<String, Integer> fieldIdByPath,
    Optional<SnapshotRef> currentSnapshot,
    Optional<SnapshotRef> previousSnapshot,
    Optional<ResolvedSnapshotInfo> resolvedSnapshots,
    Optional<TableStatsSummary> statsSummary,
    List<ResourceId> dependentViews,
    Map<EngineKey, EngineHint> engineHints)
    implements TableNode {

  public UserTableNode {
    properties = Map.copyOf(properties);
    partitionKeys = List.copyOf(partitionKeys);
    fieldIdByPath = Map.copyOf(fieldIdByPath);
    currentSnapshot = currentSnapshot == null ? Optional.empty() : currentSnapshot;
    previousSnapshot = previousSnapshot == null ? Optional.empty() : previousSnapshot;
    resolvedSnapshots = resolvedSnapshots == null ? Optional.empty() : resolvedSnapshots;
    statsSummary = statsSummary == null ? Optional.empty() : statsSummary;
    dependentViews = List.copyOf(dependentViews);
    engineHints = Map.copyOf(engineHints);
  }

  @Override
  public GraphNodeOrigin origin() {
    return GraphNodeOrigin.USER;
  }
}
