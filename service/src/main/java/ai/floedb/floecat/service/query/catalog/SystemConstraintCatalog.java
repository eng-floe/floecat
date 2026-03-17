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

package ai.floedb.floecat.service.query.catalog;

import ai.floedb.floecat.catalog.rpc.ConstraintColumnRef;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintEnforcement;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.scanner.spi.ConstraintProvider;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.def.SystemColumnDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.graph.SystemCatalogTranslator;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Immutable, engine/version-scoped cache of system relation constraints.
 *
 * <p>System constraints come from builtin system table definitions (pbtxt-backed), not from
 * information_schema scans, and are not snapshot-scoped.
 */
@ApplicationScoped
final class SystemConstraintCatalog {

  private final SystemNodeRegistry systemNodeRegistry;
  private final ConcurrentMap<VersionKey, Map<ResourceId, ConstraintProvider.ConstraintSetView>>
      byVersion = new ConcurrentHashMap<>();

  @Inject
  SystemConstraintCatalog(SystemNodeRegistry systemNodeRegistry) {
    this.systemNodeRegistry = systemNodeRegistry;
  }

  Optional<ConstraintProvider.ConstraintSetView> constraints(
      EngineContext engineContext, ResourceId relationId) {
    ResourceId normalized = SystemCatalogTranslator.normalizeSystemId(relationId);
    if (normalized == null) {
      normalized = relationId;
    }
    return Optional.ofNullable(catalogFor(engineContext).get(normalized));
  }

  private Map<ResourceId, ConstraintProvider.ConstraintSetView> catalogFor(EngineContext ctx) {
    EngineContext effective = ctx == null ? EngineContext.empty() : ctx;
    VersionKey key = new VersionKey(effective.effectiveEngineKind(), effective.normalizedVersion());
    return byVersion.computeIfAbsent(key, ignored -> buildCatalog(effective));
  }

  private Map<ResourceId, ConstraintProvider.ConstraintSetView> buildCatalog(EngineContext ctx) {
    var nodes = systemNodeRegistry.nodesFor(ctx);
    Map<ResourceId, ConstraintProvider.ConstraintSetView> out = new LinkedHashMap<>();
    for (var tableDef : nodes.toCatalogData().tables()) {
      ResourceId tableId = nodes.tableNames().get(NameRefUtil.canonical(tableDef.name()));
      if (tableId == null) {
        continue;
      }
      List<ConstraintDefinition> constraints = effectiveConstraints(tableDef);
      if (constraints.isEmpty()) {
        continue;
      }
      out.put(
          tableId,
          new SystemConstraintSetView(
              tableId,
              constraints,
              Map.of(
                  "source", "system_catalog",
                  "engine_kind", ctx.effectiveEngineKind(),
                  "engine_version", ctx.normalizedVersion())));
    }
    return Map.copyOf(out);
  }

  private static List<ConstraintDefinition> effectiveConstraints(SystemTableDef tableDef) {
    List<ConstraintDefinition> explicit = tableDef.constraints();
    if ((explicit == null || explicit.isEmpty()) && tableDef.columns().isEmpty()) {
      return List.of();
    }

    List<ConstraintDefinition> out = new java.util.ArrayList<>();
    LinkedHashSet<String> emittedNotNullKeys = new LinkedHashSet<>();

    for (ConstraintDefinition constraint : explicit) {
      if (constraint.getType() != ConstraintType.CT_NOT_NULL) {
        out.add(constraint);
        continue;
      }
      String key = notNullKey(constraint);
      if (key == null) {
        out.add(constraint);
        continue;
      }
      if (emittedNotNullKeys.add(key)) {
        out.add(constraint);
      }
    }

    for (SystemColumnDef column : tableDef.columns()) {
      if (column.nullable()) {
        continue;
      }
      String key = notNullKey(column);
      if (!emittedNotNullKeys.contains(key)) {
        out.add(implicitNotNull(column));
        emittedNotNullKeys.add(key);
      }
    }

    return List.copyOf(out);
  }

  private static ConstraintDefinition implicitNotNull(SystemColumnDef column) {
    ConstraintColumnRef.Builder ref =
        ConstraintColumnRef.newBuilder().setColumnName(column.name()).setOrdinal(1);
    String constraintName = "nn_" + column.name();
    if (column.hasId() && column.id() > 0) {
      ref.setColumnId(column.id());
      constraintName = "nn_" + column.id();
    }
    return ConstraintDefinition.newBuilder()
        .setName(constraintName)
        .setType(ConstraintType.CT_NOT_NULL)
        .setEnforcement(ConstraintEnforcement.CE_ENFORCED)
        .addColumns(ref.build())
        .build();
  }

  private static String notNullKey(ConstraintDefinition constraint) {
    if (constraint.getColumnsCount() != 1) {
      return null;
    }
    ConstraintColumnRef ref = constraint.getColumns(0);
    if (ref.getColumnId() > 0) {
      return "id:" + ref.getColumnId();
    }
    if (!ref.getColumnName().isBlank()) {
      return "name:" + ref.getColumnName();
    }
    return null;
  }

  private static String notNullKey(SystemColumnDef column) {
    if (column.hasId() && column.id() > 0) {
      return "id:" + column.id();
    }
    return "name:" + column.name();
  }

  private record VersionKey(String engineKind, String engineVersion) {}

  private record SystemConstraintSetView(
      ResourceId relationId, List<ConstraintDefinition> constraints, Map<String, String> properties)
      implements ConstraintProvider.ConstraintSetView {}
}
