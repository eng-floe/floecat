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
package ai.floedb.floecat.service.catalog.impl.surface;

import ai.floedb.floecat.catalog.rpc.Queryability;
import ai.floedb.floecat.catalog.rpc.Relation;
import ai.floedb.floecat.catalog.rpc.RelationStatus;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableDetails;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.catalog.rpc.ViewDetails;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.common.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.query.rpc.Origin;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.scanner.utils.CatalogContext;
import ai.floedb.floecat.systemcatalog.graph.SystemResourceIdGenerator;
import java.util.Map;
import java.util.Optional;

/** Builds the kind-neutral {@link Relation} read model from a stored table or view. */
final class RelationMapper {

  private final CatalogGraphView graphView;
  private final CatalogContext context;
  private final CurrentSnapshotView currentSnapshot;
  private final LogicalSchemaMapper schemaMapper = new LogicalSchemaMapper();

  RelationMapper(
      CatalogGraphView graphView, CatalogContext context, CurrentSnapshotView currentSnapshot) {
    this.graphView = graphView;
    this.context = context;
    this.currentSnapshot = currentSnapshot;
  }

  Relation fromTable(Table table, boolean includeSchema, boolean includeStatus) {
    Origin origin = originOf(table.getResourceId());
    var builder =
        common(
                table.getResourceId(),
                table.getDisplayName(),
                table.getPropertiesMap(),
                origin,
                nameOf(table))
            .setTable(tableDetails(table));
    if (includeSchema) {
      builder.setSchema(tableSchema(table));
    }
    if (includeStatus) {
      builder.setStatus(tableStatus(table.getResourceId(), origin));
    }
    return builder.build();
  }

  NameRef nameOf(Table table) {
    return canonicalName(table.getResourceId(), table.getDisplayName());
  }

  Relation fromView(View view, boolean includeSchema, boolean includeStatus) {
    var builder =
        common(
                view.getResourceId(),
                view.getDisplayName(),
                view.getPropertiesMap(),
                originOf(view.getResourceId()),
                nameOf(view))
            .setView(viewDetails(view));
    if (includeSchema) {
      builder.setSchema(SchemaDescriptor.newBuilder().addAllColumns(view.getOutputColumnsList()));
    }
    if (includeStatus) {
      builder.setStatus(queryable());
    }
    return builder.build();
  }

  NameRef nameOf(View view) {
    return canonicalName(view.getResourceId(), view.getDisplayName());
  }

  /** Builds the identity/status-only form used by pointer-backed generic reads. */
  Relation fromRef(
      CatalogGraphView.RelationRef ref,
      CatalogGraphView.NamespaceRef namespace,
      boolean includeStatus) {
    return fromRef(ref, namespaceName(namespace, ref.name()), includeStatus);
  }

  /** Builds the identity/status-only form while preserving the caller's resolved name. */
  Relation fromRef(CatalogGraphView.RelationRef ref, NameRef name, boolean includeStatus) {
    Origin origin = originOf(ref.id());
    var builder = common(ref.id(), ref.name(), Map.of(), origin, name);
    if (ref.kind() == ResourceKind.RK_VIEW) {
      builder.setView(ViewDetails.getDefaultInstance());
    } else {
      builder.setTable(TableDetails.getDefaultInstance());
    }
    if (includeStatus) {
      builder.setStatus(
          ref.kind() == ResourceKind.RK_VIEW ? queryable() : tableStatus(ref.id(), origin));
    }
    return builder.build();
  }

  NameRef namespaceName(CatalogGraphView.NamespaceRef namespace, String relationName) {
    var builder = NameRef.newBuilder().addAllPath(namespace.pathSegments()).setName(relationName);
    if (namespace.name() != null && !namespace.name().isBlank()) {
      builder.addPath(namespace.name());
    }
    return builder.build();
  }

  /** Everything a relation carries regardless of kind. */
  private Relation.Builder common(
      ResourceId id,
      String displayName,
      Map<String, String> properties,
      Origin origin,
      NameRef name) {
    return Relation.newBuilder()
        .setResourceId(id)
        .setName(name)
        .setDisplayName(displayName)
        .setOrigin(origin)
        .putAllProperties(properties);
  }

  private static TableDetails tableDetails(Table table) {
    var builder = TableDetails.newBuilder().setSchemaJson(table.getSchemaJson());
    if (table.hasUpstream()) {
      builder.setUpstream(table.getUpstream());
    }
    return builder.build();
  }

  private static ViewDetails viewDetails(View view) {
    return ViewDetails.newBuilder()
        .addAllSqlDefinitions(view.getSqlDefinitionsList())
        .addAllBaseRelations(view.getBaseRelationsList())
        .addAllCreationSearchPath(view.getCreationSearchPathList())
        .build();
  }

  private SchemaDescriptor tableSchema(Table table) {
    var graphSchema = graphView.tableSchema(table.getResourceId(), context);
    if (!graphSchema.isEmpty()) {
      return SchemaDescriptor.newBuilder().addAllColumns(graphSchema).build();
    }
    return schemaMapper.map(table, table.getSchemaJson());
  }

  private NameRef canonicalName(ResourceId id, String fallbackDisplayName) {
    Optional<NameRef> resolved =
        id.getKind() == ResourceKind.RK_VIEW
            ? graphView.viewName(id, context)
            : graphView.tableName(id, context);
    return resolved.orElseGet(() -> NameRef.newBuilder().setName(fallbackDisplayName).build());
  }

  private static Origin originOf(ResourceId id) {
    return SystemResourceIdGenerator.isSystemId(id) ? Origin.ORIGIN_BUILTIN : Origin.ORIGIN_USER;
  }

  /**
   * Builtin tables are materialized per request, so they read without a snapshot. A user table is
   * queryable once it has a committed current snapshot; that pointer read reports the committed
   * selection and does not pin.
   */
  private RelationStatus tableStatus(ResourceId tableId, Origin origin) {
    if (origin == Origin.ORIGIN_BUILTIN) {
      return queryable();
    }
    return currentSnapshot
        .currentSnapshotId(tableId)
        .map(id -> queryable().toBuilder().setCurrentSnapshotId(id).build())
        .orElseGet(
            () ->
                RelationStatus.newBuilder()
                    .setQueryability(Queryability.Q_NOT_QUERYABLE_NO_SNAPSHOT)
                    .setReason("table has no committed current snapshot")
                    .build());
  }

  private static RelationStatus queryable() {
    return RelationStatus.newBuilder().setQueryability(Queryability.Q_QUERYABLE).build();
  }
}
