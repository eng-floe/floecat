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
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interim conservative pruning policy based on request-scoped relation/column projection.
 *
 * <p>This is intentionally a scaffold for future visibility-aware pruning.
 *
 * <p>Current policy preserves PK/UNIQUE and single-column nullability constraints for requested
 * relations, treating requested column IDs as stats projection hints rather than strict visibility
 * boundaries. FK/CHECK are pruned or masked using request-scoped visibility.
 *
 * <p>CHECK masking assumes CHECK constraints populate {@code ConstraintDefinition.columns} with
 * referenced local columns. If connectors omit those column refs, this pruner cannot detect hidden
 * column usage and will not mask the check expression.
 */
final class RequestScopeConstraintPruner implements ConstraintPruner {

  private final Set<String> requestedRelationKeys;
  private final Map<String, Set<Long>> requestedColumnsByRelationKey;
  private final Set<String> allColumnsVisibleRelationKeys;

  RequestScopeConstraintPruner(
      Set<String> requestedRelationKeys, Map<String, Set<Long>> requestedColumnsByRelationKey) {
    this(requestedRelationKeys, requestedColumnsByRelationKey, Set.of());
  }

  private RequestScopeConstraintPruner(
      Set<String> requestedRelationKeys,
      Map<String, Set<Long>> requestedColumnsByRelationKey,
      Set<String> allColumnsVisibleRelationKeys) {
    this.requestedRelationKeys = requestedRelationKeys;
    this.requestedColumnsByRelationKey = requestedColumnsByRelationKey;
    this.allColumnsVisibleRelationKeys = allColumnsVisibleRelationKeys;
  }

  static RequestScopeConstraintPruner forRequestedTablesOnly(Set<String> requestedRelationKeys) {
    return new RequestScopeConstraintPruner(requestedRelationKeys, Map.of(), requestedRelationKeys);
  }

  @Override
  public List<ConstraintDefinition> prune(
      ResourceId tableId, List<ConstraintDefinition> constraints) {
    String localTableKey = relationKey(tableId);
    Set<Long> requestedLocalColumns = requestedColumnsFor(localTableKey);
    boolean allLocalColumnsVisible = allColumnsVisibleRelationKeys.contains(localTableKey);
    List<ConstraintDefinition> out = new ArrayList<>(constraints.size());
    for (ConstraintDefinition constraint : constraints) {
      // Preserve PK/UNIQUE and column nullability when the relation is requested.
      // Requested column IDs are treated as stats projection hints for these constraints.
      if (isRelationScopedConstraint(constraint.getType())) {
        out.add(constraint);
        continue;
      }

      // Conservative FK policy:
      // 1) hide FK if referenced table is outside request scope,
      // 2) filter local/referenced columns to requested sets,
      // 3) drop FK if either side becomes empty after filtering.
      if (constraint.getType() == ConstraintType.CT_FOREIGN_KEY
          && constraint.hasReferencedTableId()
          && !requestedRelationKeys.contains(relationKey(constraint.getReferencedTableId()))) {
        continue;
      }

      List<ConstraintColumnRef> localColumns =
          allLocalColumnsVisible
              ? constraint.getColumnsList()
              : filterRequestedColumns(constraint.getColumnsList(), requestedLocalColumns);
      ConstraintDefinition.Builder builder = constraint.toBuilder().clearColumns();
      builder.addAllColumns(localColumns);

      if (constraint.getType() == ConstraintType.CT_FOREIGN_KEY) {
        List<ConstraintColumnRef> referencedColumns = constraint.getReferencedColumnsList();
        if (constraint.hasReferencedTableId()) {
          String referencedTableKey = relationKey(constraint.getReferencedTableId());
          if (!allColumnsVisibleRelationKeys.contains(referencedTableKey)) {
            referencedColumns =
                filterRequestedColumns(referencedColumns, requestedColumnsFor(referencedTableKey));
          }
        }
        builder.clearReferencedColumns().addAllReferencedColumns(referencedColumns);
        if (localColumns.isEmpty() || referencedColumns.isEmpty()) {
          continue;
        }
      } else if (constraint.getType() == ConstraintType.CT_CHECK) {
        // Mask the expression when there are hidden columns. If columns is empty the connector
        // didn't populate column refs; treat conservatively as "may reference hidden columns".
        if (!allLocalColumnsVisible
            && (constraint.getColumnsList().isEmpty()
                || hasUnrequestedColumns(constraint.getColumnsList(), requestedLocalColumns))) {
          builder.setCheckExpression("");
        }
      } else if (!constraint.getColumnsList().isEmpty() && localColumns.isEmpty()) {
        continue;
      }
      out.add(builder.build());
    }
    return out;
  }

  private Set<Long> requestedColumnsFor(String relationKey) {
    return requestedColumnsByRelationKey.getOrDefault(relationKey, Set.of());
  }

  static String relationKey(ResourceId tableId) {
    return tableId.getAccountId() + "/" + tableId.getKindValue() + "/" + tableId.getId();
  }

  private static boolean isRelationScopedConstraint(ConstraintType type) {
    return type == ConstraintType.CT_PRIMARY_KEY
        || type == ConstraintType.CT_UNIQUE
        || type == ConstraintType.CT_NOT_NULL;
  }

  private static boolean hasUnrequestedColumns(
      List<ConstraintColumnRef> columns, Set<Long> requestedColumns) {
    for (ConstraintColumnRef column : columns) {
      if (column.getColumnId() > 0 && !requestedColumns.contains(column.getColumnId())) {
        return true;
      }
    }
    return false;
  }

  private static List<ConstraintColumnRef> filterRequestedColumns(
      List<ConstraintColumnRef> columns, Set<Long> requestedColumns) {
    if (columns.isEmpty()) {
      return List.of();
    }
    List<ConstraintColumnRef> filtered = new ArrayList<>(columns.size());
    for (ConstraintColumnRef column : columns) {
      if (column.getColumnId() <= 0 || requestedColumns.contains(column.getColumnId())) {
        filtered.add(column);
      }
    }
    return filtered;
  }
}
