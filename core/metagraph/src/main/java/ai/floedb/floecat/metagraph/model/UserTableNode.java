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

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import java.time.Instant;
import java.util.LinkedHashMap;
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
    ColumnIdAlgorithm columnIdAlgorithm,
    String schemaJson,
    Map<String, String> properties,
    List<String> partitionKeys,
    Optional<SnapshotRef> currentSnapshot,
    Optional<SnapshotRef> previousSnapshot,
    Optional<ResolvedSnapshotInfo> resolvedSnapshots,
    Optional<TableStatsSummary> statsSummary,
    List<ResourceId> dependentViews,
    Map<EngineHintKey, EngineHint> engineHints,
    Map<Long, Map<EngineHintKey, EngineHint>> columnHints)
    implements TableNode {

  public UserTableNode {
    properties = Map.copyOf(properties);
    partitionKeys = List.copyOf(partitionKeys);
    currentSnapshot = currentSnapshot == null ? Optional.empty() : currentSnapshot;
    previousSnapshot = previousSnapshot == null ? Optional.empty() : previousSnapshot;
    resolvedSnapshots = resolvedSnapshots == null ? Optional.empty() : resolvedSnapshots;
    statsSummary = statsSummary == null ? Optional.empty() : statsSummary;
    dependentViews = List.copyOf(dependentViews);
    engineHints = Map.copyOf(engineHints == null ? Map.of() : engineHints);
    columnHints = normalizeColumnHints(columnHints);
  }

  @Override
  public Map<Long, Map<EngineHintKey, EngineHint>> columnHints() {
    return columnHints;
  }

  @Override
  public GraphNodeOrigin origin() {
    return GraphNodeOrigin.USER;
  }

  private static Map<Long, Map<EngineHintKey, EngineHint>> normalizeColumnHints(
      Map<Long, Map<EngineHintKey, EngineHint>> hints) {
    if (hints == null || hints.isEmpty()) {
      return Map.of();
    }
    Map<Long, Map<EngineHintKey, EngineHint>> normalized = new LinkedHashMap<>();
    for (Map.Entry<Long, Map<EngineHintKey, EngineHint>> entry : hints.entrySet()) {
      Map<EngineHintKey, EngineHint> value =
          entry.getValue() == null ? Map.of() : Map.copyOf(entry.getValue());
      normalized.put(entry.getKey(), value);
    }
    return Map.copyOf(normalized);
  }
}
