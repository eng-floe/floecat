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

package ai.floedb.floecat.systemcatalog.informationschema;

import ai.floedb.floecat.catalog.rpc.ConstraintColumnRef;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintEnforcement;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.catalog.rpc.ForeignKeyActionRule;
import ai.floedb.floecat.catalog.rpc.ForeignKeyMatchOption;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Builds a compact per-scan index of visible constraints for information_schema scanners. */
final class ConstraintScanIndex {
  private static final Object CACHE_KEY = new Object();
  private static final String RESOLVE_CORRELATION_ID = "information_schema.constraints";

  private final List<ConstraintEntry> entries;

  private ConstraintScanIndex(List<ConstraintEntry> entries) {
    this.entries = entries;
  }

  List<ConstraintEntry> entries() {
    return entries;
  }

  static ConstraintScanIndex build(SystemObjectScanContext ctx) {
    return ctx.memoized(CACHE_KEY, () -> buildUncached(ctx));
  }

  private static ConstraintScanIndex buildUncached(SystemObjectScanContext ctx) {
    Map<ResourceId, TableRef> tableRefs = new LinkedHashMap<>();
    Map<ResourceId, TableRef> tablesWithConstraints = new LinkedHashMap<>();
    Map<ResourceId, String> catalogNames = new HashMap<>();
    Map<ResourceId, Map<Long, String>> columnsById = new HashMap<>();
    Map<ResourceId, List<ConstraintDefinition>> constraintsByTable = new HashMap<>();
    Map<ResourceId, Map<List<ColumnRef>, String>> referencedConstraintNameByColumnsByTable =
        new HashMap<>();
    Map<String, Optional<TableRef>> referencedTablesByName = new HashMap<>();

    for (NamespaceNode namespace : ctx.listNamespaces()) {
      String catalogName =
          catalogNames.computeIfAbsent(
              namespace.catalogId(), id -> ((CatalogNode) ctx.resolve(id)).displayName());
      String schemaName = schemaName(namespace);
      for (RelationNode relation : ctx.listRelations(namespace.id())) {
        if (!(relation instanceof TableNode table)) {
          continue;
        }
        TableRef tableRef =
            new TableRef(table.id(), catalogName, schemaName, relation.displayName());
        tableRefs.put(table.id(), tableRef);
        var viewOpt = ctx.constraintProvider().latestConstraints(table.id());
        if (viewOpt.isEmpty() || viewOpt.get().constraints().isEmpty()) {
          continue;
        }
        tablesWithConstraints.put(table.id(), tableRef);
        constraintsByTable.put(table.id(), viewOpt.get().constraints());
      }
    }

    List<ConstraintEntry> out = new ArrayList<>();
    for (TableRef table : tablesWithConstraints.values()) {
      Map<Long, String> localById =
          columnsById.computeIfAbsent(table.tableId(), id -> columnsById(ctx, id));
      for (ConstraintDefinition constraint :
          constraintsByTable.getOrDefault(table.tableId(), List.of())) {
        String constraintName = constraint.getName().trim();
        if (constraintName.isEmpty()) {
          continue;
        }

        ConstraintType type = constraint.getType();
        TableRef referenced =
            resolveReferencedTable(
                ctx,
                tableRefs,
                referencedTablesByName,
                constraint.hasReferencedTable() ? constraint.getReferencedTable() : null,
                constraint.hasReferencedTableId() ? constraint.getReferencedTableId() : null);
        Map<Long, String> referencedById =
            referenced == null
                    || referenced.tableId() == null
                    || referenced.tableId().getId().isBlank()
                ? Map.of()
                : columnsById.computeIfAbsent(referenced.tableId(), id -> columnsById(ctx, id));

        List<ColumnRef> localColumns = normalizeColumns(constraint.getColumnsList(), localById);
        List<ColumnRef> referencedColumns =
            normalizeColumns(constraint.getReferencedColumnsList(), referencedById);
        String referencedConstraintName = resolveReferencedConstraintName(constraint);
        if (referencedConstraintName == null
            && referenced != null
            && referenced.tableId() != null
            && !referenced.tableId().getId().isBlank()
            && !referencedColumns.isEmpty()) {
          referencedConstraintName =
              resolveReferencedConstraintNameFromLookup(
                  referenced.tableId(),
                  referencedColumns,
                  referencedById,
                  constraintsByTable,
                  referencedConstraintNameByColumnsByTable);
        }

        out.add(
            new ConstraintEntry(
                table,
                constraintName,
                typeName(type),
                type,
                enforced(constraint.getEnforcement()),
                constraint.getCheckExpression(),
                localColumns,
                referenced,
                referencedColumns,
                referencedConstraintName,
                fkMatchOption(constraint.getMatchOption()),
                fkActionRule(constraint.getUpdateRule()),
                fkActionRule(constraint.getDeleteRule())));
      }
    }

    return new ConstraintScanIndex(List.copyOf(out));
  }

  private static TableRef resolveReferencedTable(
      SystemObjectScanContext ctx,
      Map<ResourceId, TableRef> byId,
      Map<String, Optional<TableRef>> byName,
      NameRef referencedTable,
      ResourceId referencedTableId) {
    if (referencedTableId != null && !referencedTableId.getId().isBlank()) {
      TableRef fromId = byId.get(referencedTableId);
      if (fromId != null) {
        return fromId;
      }
    }
    if (referencedTable == null) {
      return null;
    }
    if (referencedTable.hasResourceId()) {
      TableRef fromResource = byId.get(referencedTable.getResourceId());
      if (fromResource != null) {
        return fromResource;
      }
    }
    if (referencedTable.getName().isBlank()) {
      return null;
    }
    String key = referencedTableCacheKey(referencedTable);
    return byName
        .computeIfAbsent(
            key,
            ignored ->
                ctx.graph().resolveTable(RESOLVE_CORRELATION_ID, referencedTable).map(byId::get))
        .orElse(null);
  }

  private static String referencedTableCacheKey(NameRef referencedTable) {
    String catalog = referencedTable.getCatalog().trim().toLowerCase(java.util.Locale.ROOT);
    String canonical = NameRefUtil.canonical(referencedTable);
    return catalog + "|" + canonical;
  }

  private static String resolveReferencedConstraintName(ConstraintDefinition constraint) {
    String explicit = constraint.getReferencedConstraintName().trim();
    if (!explicit.isEmpty()) {
      return explicit;
    }
    return null;
  }

  private static String resolveReferencedConstraintNameFromLookup(
      ResourceId referencedTableId,
      List<ColumnRef> referencedColumns,
      Map<Long, String> referencedById,
      Map<ResourceId, List<ConstraintDefinition>> constraintsByTable,
      Map<ResourceId, Map<List<ColumnRef>, String>> lookupCache) {
    Map<List<ColumnRef>, String> byColumns =
        lookupCache.computeIfAbsent(
            referencedTableId,
            id ->
                buildReferencedConstraintLookup(
                    constraintsByTable.getOrDefault(id, List.of()), referencedById));
    return byColumns.get(referencedColumns);
  }

  private static Map<List<ColumnRef>, String> buildReferencedConstraintLookup(
      List<ConstraintDefinition> constraints, Map<Long, String> referencedById) {
    Map<List<ColumnRef>, String> out = new HashMap<>();
    Set<List<ColumnRef>> ambiguous = new HashSet<>();
    for (ConstraintDefinition candidate : constraints) {
      if (candidate.getType() != ConstraintType.CT_PRIMARY_KEY
          && candidate.getType() != ConstraintType.CT_UNIQUE) {
        continue;
      }
      String name = candidate.getName().trim();
      if (name.isEmpty()) {
        continue;
      }
      List<ColumnRef> normalized = normalizeColumns(candidate.getColumnsList(), referencedById);
      if (normalized.isEmpty() || ambiguous.contains(normalized)) {
        continue;
      }
      String existing = out.putIfAbsent(normalized, name);
      if (existing != null && !existing.equals(name)) {
        out.remove(normalized);
        ambiguous.add(normalized);
      }
    }
    return out;
  }

  private static List<ColumnRef> normalizeColumns(
      List<ConstraintColumnRef> refs, Map<Long, String> namesById) {
    if (refs.isEmpty()) {
      return List.of();
    }
    List<ColumnRef> out = new ArrayList<>(refs.size());
    for (int i = 0; i < refs.size(); i++) {
      ConstraintColumnRef ref = refs.get(i);
      int ordinal = ref.getOrdinal() > 0 ? ref.getOrdinal() : i + 1;
      String name = ref.getColumnName().trim();
      if (name.isEmpty() && ref.getColumnId() > 0) {
        name = namesById.getOrDefault(ref.getColumnId(), "");
      }
      if (name.isEmpty()) {
        continue;
      }
      out.add(new ColumnRef(name, ordinal));
    }
    out.sort(Comparator.comparingInt(ColumnRef::ordinal));
    return List.copyOf(out);
  }

  private static Map<Long, String> columnsById(SystemObjectScanContext ctx, ResourceId tableId) {
    List<SchemaColumn> columns = ctx.graph().tableSchema(tableId);
    if (columns.isEmpty()) {
      return Map.of();
    }
    Map<Long, String> out = new HashMap<>();
    for (SchemaColumn column : columns) {
      if (column.getFieldId() > 0) {
        out.put((long) column.getFieldId(), column.getName());
      }
    }
    return out;
  }

  private static String typeName(ConstraintType type) {
    return switch (type) {
      case CT_PRIMARY_KEY -> "PRIMARY KEY";
      case CT_UNIQUE -> "UNIQUE";
      case CT_FOREIGN_KEY -> "FOREIGN KEY";
      case CT_CHECK -> "CHECK";
      case CT_NOT_NULL -> "NOT NULL";
      default -> "UNKNOWN";
    };
  }

  private static String enforced(ConstraintEnforcement enforcement) {
    return switch (enforcement) {
      case CE_ENFORCED -> "YES";
      case CE_NOT_ENFORCED -> "NO";
      default -> null;
    };
  }

  private static String fkMatchOption(ForeignKeyMatchOption option) {
    // Keep defaults in sync with service/repo ConstraintNormalizer defaults:
    // unspecified -> NONE.
    return switch (option) {
      case FK_MATCH_OPTION_FULL -> "FULL";
      case FK_MATCH_OPTION_PARTIAL -> "PARTIAL";
      case FK_MATCH_OPTION_NONE, FK_MATCH_OPTION_UNSPECIFIED -> "NONE";
      case UNRECOGNIZED -> "NONE";
    };
  }

  private static String fkActionRule(ForeignKeyActionRule rule) {
    // Keep defaults in sync with service/repo ConstraintNormalizer defaults:
    // unspecified -> NO ACTION.
    return switch (rule) {
      case FK_ACTION_RULE_RESTRICT -> "RESTRICT";
      case FK_ACTION_RULE_CASCADE -> "CASCADE";
      case FK_ACTION_RULE_SET_NULL -> "SET NULL";
      case FK_ACTION_RULE_SET_DEFAULT -> "SET DEFAULT";
      case FK_ACTION_RULE_NO_ACTION, FK_ACTION_RULE_UNSPECIFIED -> "NO ACTION";
      case UNRECOGNIZED -> "NO ACTION";
    };
  }

  private static String schemaName(NamespaceNode namespace) {
    return NameRefUtil.namespaceName(namespace.pathSegments(), namespace.displayName());
  }

  record TableRef(ResourceId tableId, String catalog, String schema, String name) {}

  record ColumnRef(String name, int ordinal) {}

  record ConstraintEntry(
      TableRef table,
      String name,
      String typeName,
      ConstraintType type,
      String enforced,
      String checkClause,
      List<ColumnRef> localColumns,
      TableRef referencedTable,
      List<ColumnRef> referencedColumns,
      String referencedConstraintName,
      String matchOption,
      String updateRule,
      String deleteRule) {}
}
