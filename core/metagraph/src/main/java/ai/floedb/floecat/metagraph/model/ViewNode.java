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

import ai.floedb.floecat.catalog.rpc.ViewSqlDefinition;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Immutable view node encapsulating SQL definition and dependency references.
 *
 * <p>{@code baseRelations} holds the fully-qualified names of relations this view directly depends
 * on, expressed as {@link NameRef} objects so that resolution can be performed directly via {@code
 * CatalogOverlay.resolveName()} without any string parsing. The list is an optional performance
 * hint: when non-empty, {@code UserObjectBundleService} eagerly resolves base-table metadata in the
 * same {@code GetUserObjects} response, saving the planner a round-trip.
 */
public record ViewNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    ResourceId catalogId,
    ResourceId namespaceId,
    String displayName,
    List<ViewSqlDefinition> sqlDefinitions,
    List<SchemaColumn> outputColumns,
    List<NameRef> baseRelations,
    List<String> creationSearchPath,
    GraphNodeOrigin origin,
    Map<String, String> properties,
    Optional<String> owner,
    Map<Long, Map<EngineHintKey, EngineHint>> columnHints,
    Map<EngineHintKey, EngineHint> engineHints)
    implements RelationNode {

  public ViewNode {
    sqlDefinitions = List.copyOf(sqlDefinitions == null ? List.of() : sqlDefinitions);
    outputColumns = List.copyOf(outputColumns);
    baseRelations = List.copyOf(baseRelations);
    creationSearchPath = List.copyOf(creationSearchPath);
    properties = Map.copyOf(properties);
    owner = owner == null ? Optional.empty() : owner;
    columnHints = RelationNode.normalizeColumnHints(columnHints);
    engineHints = Map.copyOf(engineHints == null ? Map.of() : engineHints);
  }

  public ViewNode(
      ResourceId id,
      long version,
      Instant metadataUpdatedAt,
      ResourceId catalogId,
      ResourceId namespaceId,
      String displayName,
      String sql,
      String dialect,
      List<SchemaColumn> outputColumns,
      List<NameRef> baseRelations,
      List<String> creationSearchPath,
      GraphNodeOrigin origin,
      Map<String, String> properties,
      Optional<String> owner,
      Map<Long, Map<EngineHintKey, EngineHint>> columnHints,
      Map<EngineHintKey, EngineHint> engineHints) {
    this(
        id,
        version,
        metadataUpdatedAt,
        catalogId,
        namespaceId,
        displayName,
        (sql == null || sql.isBlank())
            ? List.of()
            : List.of(
                ViewSqlDefinition.newBuilder()
                    .setSql(sql)
                    .setDialect(dialect == null ? "" : dialect)
                    .build()),
        outputColumns,
        baseRelations,
        creationSearchPath,
        origin,
        properties,
        owner,
        columnHints,
        engineHints);
  }

  public String sql() {
    return preferredSqlDefinition().map(ViewSqlDefinition::getSql).orElse("");
  }

  public String dialect() {
    return preferredSqlDefinition().map(ViewSqlDefinition::getDialect).orElse("");
  }

  public Optional<ViewSqlDefinition> preferredSqlDefinition() {
    return sqlDefinitions.stream()
        .filter(def -> def != null && !def.getSql().isBlank())
        .sorted(
            (left, right) ->
                Integer.compare(
                    definitionPriority(left.getDialect()), definitionPriority(right.getDialect())))
        .findFirst();
  }

  private static int definitionPriority(String dialect) {
    if (dialect == null) {
      return 3;
    }
    return switch (dialect.trim().toLowerCase()) {
      case "floe" -> 0;
      case "ansi" -> 1;
      case "spark" -> 2;
      default -> 3;
    };
  }

  @Override
  public GraphNodeKind kind() {
    return GraphNodeKind.VIEW;
  }

  @Override
  public GraphNodeOrigin origin() {
    return origin;
  }

  @Override
  public Map<Long, Map<EngineHintKey, EngineHint>> columnHints() {
    return columnHints;
  }
}
