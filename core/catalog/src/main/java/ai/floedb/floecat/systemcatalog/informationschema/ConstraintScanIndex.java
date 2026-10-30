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
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.spi.SystemScanRequest;
import ai.floedb.floecat.systemcatalog.informationschema.NamespaceScanSupport.NamespaceEntry;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import ai.floedb.floecat.systemcatalog.util.SchemaColumns;
import ai.floedb.floecat.types.RelationFailures;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Builds a compact per-scan index of visible constraints for metadata scanners. */
public final class ConstraintScanIndex {
  private static final Object CACHE_KEY = new Object();
  private static final String RESOLVE_CORRELATION_ID = "information_schema.constraints";

  private final List<ConstraintEntry> entries;

  private ConstraintScanIndex(List<ConstraintEntry> entries) {
    this.entries = entries;
  }

  public List<ConstraintEntry> entries() {
    return entries;
  }

  public static ConstraintScanIndex build(SystemObjectScanContext ctx) {
    return ctx.memoized(CACHE_KEY, () -> buildUncached(ctx));
  }

  private static ConstraintScanIndex buildUncached(SystemObjectScanContext ctx) {
    return buildUncached(ctx, SystemScanRequest.empty(), null, null);
  }

  public static ConstraintScanIndex build(
      SystemObjectScanContext ctx,
      SystemScanRequest request,
      String schemaColumnName,
      String tableNameColumn) {
    return buildUncached(ctx, request, schemaColumnName, tableNameColumn);
  }

  private static ConstraintScanIndex buildUncached(
      SystemObjectScanContext ctx,
      SystemScanRequest request,
      String schemaColumnName,
      String tableNameColumn) {
    Map<ResourceId, TableRef> tableRefs = new LinkedHashMap<>();
    Map<ResourceId, TableRef> tablesWithConstraints = new LinkedHashMap<>();
    Map<ResourceId, String> catalogNames = new HashMap<>();
    Map<ResourceId, Optional<Map<Long, String>>> columnsById = new HashMap<>();
    Map<ResourceId, List<ConstraintDefinition>> constraintsByTable = new HashMap<>();
    Map<ResourceId, Map<List<ColumnRef>, String>> referencedConstraintNameByColumnsByTable =
        new HashMap<>();
    Map<String, Optional<Referenced>> referencedTablesByName = new HashMap<>();

    for (NamespaceEntry namespace : NamespaceScanSupport.entries(ctx, request, schemaColumnName)) {
      String catalogName =
          catalogNames.computeIfAbsent(
              namespace.catalogId(), id -> ((CatalogNode) ctx.resolve(id)).displayName());
      String schemaName = namespace.schemaName();
      for (RelationNode relation :
          NamespaceScanSupport.relations(ctx, namespace.id(), request, tableNameColumn)) {
        if (!(relation instanceof TableNode table)) {
          continue;
        }
        TableRef tableRef =
            new TableRef(table.id(), catalogName, schemaName, relation.displayName());
        tableRefs.put(table.id(), tableRef);
        var viewOpt =
            ctx.forRelation(
                RelationFailures.label(table.id()),
                () -> ctx.constraintProvider().latestConstraints(table.id()));
        if (viewOpt.isEmpty() || viewOpt.get().constraints().isEmpty()) {
          continue;
        }
        tablesWithConstraints.put(table.id(), tableRef);
        constraintsByTable.put(table.id(), viewOpt.get().constraints());
      }
    }

    List<ConstraintEntry> out = new ArrayList<>();
    for (TableRef table : tablesWithConstraints.values()) {
      Optional<Map<Long, String>> local =
          columnsById.computeIfAbsent(table.tableId(), id -> columnsById(ctx, table));
      if (local.isEmpty()) {
        continue;
      }
      Map<Long, String> localById = local.get();
      for (ConstraintDefinition constraint :
          constraintsByTable.getOrDefault(table.tableId(), List.of())) {
        String constraintName = constraint.getName().trim();
        if (constraintName.isEmpty()) {
          continue;
        }

        ConstraintType type = constraint.getType();
        Optional<Referenced> resolved =
            referenced(ctx, constraint, tableRefs, referencedTablesByName, columnsById);
        if (resolved.isEmpty()) {
          continue;
        }
        TableRef referenced = resolved.get().ref();
        Map<Long, String> referencedById = resolved.get().columnsById();

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

  /** A constraint's referenced table, {@code null} when it has none, with its column names. */
  private record Referenced(TableRef ref, Map<Long, String> columnsById) {}

  /**
   * The table {@code constraint} references and its column names; empty when resolving that table
   * or reading its schema fails.
   */
  private static Optional<Referenced> referenced(
      SystemObjectScanContext ctx,
      ConstraintDefinition constraint,
      Map<ResourceId, TableRef> byId,
      Map<String, Optional<Referenced>> byName,
      Map<ResourceId, Optional<Map<Long, String>>> columnsById) {
    if (constraint.hasReferencedTableId() && !constraint.getReferencedTableId().getId().isBlank()) {
      TableRef fromId = byId.get(constraint.getReferencedTableId());
      if (fromId != null) {
        return withColumns(ctx, fromId, columnsById);
      }
    }
    if (!constraint.hasReferencedTable()) {
      return withColumns(ctx, null, columnsById);
    }
    NameRef referencedTable = constraint.getReferencedTable();
    if (referencedTable.hasResourceId()) {
      TableRef fromResource = byId.get(referencedTable.getResourceId());
      if (fromResource != null) {
        return withColumns(ctx, fromResource, columnsById);
      }
    }
    if (referencedTable.getName().isBlank()) {
      return withColumns(ctx, null, columnsById);
    }
    return byName.computeIfAbsent(
        referencedTableCacheKey(referencedTable),
        ignored ->
            ctx.forRelation(
                    RelationFailures.label(
                        ctx.queryDefaultCatalogId().getAccountId(), referencedTable),
                    () ->
                        Optional.of(
                            new Referenced(
                                ctx.graph()
                                    .resolveTable(
                                        RESOLVE_CORRELATION_ID,
                                        referencedTable,
                                        ctx.catalogContext())
                                    .map(byId::get)
                                    .orElse(null),
                                Map.of())))
                .flatMap(found -> withColumns(ctx, found.ref(), columnsById)));
  }

  private static Optional<Referenced> withColumns(
      SystemObjectScanContext ctx,
      TableRef ref,
      Map<ResourceId, Optional<Map<Long, String>>> columnsById) {
    if (ref == null || ref.tableId() == null || ref.tableId().getId().isBlank()) {
      return Optional.of(new Referenced(ref, Map.of()));
    }
    return columnsById
        .computeIfAbsent(ref.tableId(), id -> columnsById(ctx, ref))
        .map(columns -> new Referenced(ref, columns));
  }

  private static String referencedTableCacheKey(NameRef referencedTable) {
    // The value cached under this key comes from an exact resolution, so the key preserves case
    // too. Folding it would let two spellings share one entry and inherit each other's result.
    String catalog = referencedTable.getCatalog().trim();
    return catalog + "|" + NameRefUtil.matchKey(referencedTable);
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

  /** A table's column names by field id; empty when reading its schema fails. */
  private static Optional<Map<Long, String>> columnsById(
      SystemObjectScanContext ctx, TableRef table) {
    return ctx.forRelation(
        RelationFailures.label(table.tableId()),
        () ->
            Optional.of(
                columnsById(ctx.graph().tableSchema(table.tableId(), ctx.catalogContext()))));
  }

  private static Map<Long, String> columnsById(List<SchemaColumn> schema) {
    List<SchemaColumn> columns = SchemaColumns.withoutSyntheticNodes(schema);
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

  public record TableRef(ResourceId tableId, String catalog, String schema, String name) {}

  public record ColumnRef(String name, int ordinal) {}

  public record ConstraintEntry(
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
