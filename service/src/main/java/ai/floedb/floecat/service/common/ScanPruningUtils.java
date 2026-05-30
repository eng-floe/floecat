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

package ai.floedb.floecat.service.common;

import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.execution.rpc.ScanBundle;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.types.LogicalCoercions;
import ai.floedb.floecat.types.LogicalComparators;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeProtoAdapter;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Utility functions for pruning {@link ScanBundle} instances.
 *
 * <p>Pruning consists of:
 *
 * <ul>
 *   <li>Column projection: dropping unused column statistics.
 *   <li>Predicate pruning: eliminating files whose min/max statistics make them unable to match a
 *       filter.
 * </ul>
 *
 * The logic is intentionally conservative: files are only removed when they cannot possibly satisfy
 * the provided predicates. This keeps the pruning engine-agnostic and safe for all connectors.
 *
 * <p>All methods in this class are pure functions and do not mutate inputs. New ScanBundle
 * instances are always constructed.
 */
public final class ScanPruningUtils {

  private ScanPruningUtils() {
    // Utility class; do not instantiate.
  }

  /**
   * Applies pruning to a connector-produced bundle.
   *
   * <p>Pruning includes:
   *
   * <ul>
   *   <li>Column projection (dropping unused column statistics)
   *   <li>Predicate pruning (removing files whose min/max values cannot match a predicate)
   * </ul>
   */
  public static ScanBundle pruneBundle(
      ai.floedb.floecat.connector.spi.FloecatConnector.ScanBundle raw,
      List<String> requiredColumns,
      List<Predicate> predicates) {
    return pruneBundle(raw, requiredColumns, List.of(), predicates, Map.of());
  }

  /**
   * Applies pruning to a connector-produced bundle using resolved stable column IDs when
   * available.
   */
  public static ScanBundle pruneBundle(
      ai.floedb.floecat.connector.spi.FloecatConnector.ScanBundle raw,
      List<String> requiredColumns,
      List<Long> requiredColumnIds,
      List<Predicate> predicates,
      Map<String, Long> columnIdsByName) {

    // Convert raw connector bundle → RPC bundle so we operate on the same type everywhere.
    ScanBundle.Builder builder = ScanBundle.newBuilder();
    builder.addAllDataFiles(raw.dataFiles());
    builder.addAllDeleteFiles(raw.deleteFiles());
    ScanBundle initial = builder.build();

    ScanBundle afterProjection =
        pruneColumns(initial, requiredColumns, requiredColumnIds, columnIdsByName);
    return prunePredicates(afterProjection, predicates, columnIdsByName);
  }

  // ----------------------------------------------------------------------
  //  Column pruning
  // ----------------------------------------------------------------------
  private static ScanBundle pruneColumns(
      ScanBundle bundle,
      List<String> requiredColumns,
      List<Long> requiredColumnIds,
      Map<String, Long> columnIdsByName) {
    if ((requiredColumns == null || requiredColumns.isEmpty())
        && (requiredColumnIds == null || requiredColumnIds.isEmpty())) {
      return bundle;
    }

    List<ProjectionSelector> selectors =
        projectionSelectors(requiredColumnIds, requiredColumns, columnIdsByName);

    ScanBundle.Builder out = ScanBundle.newBuilder();

    for (ScanFile f : bundle.getDataFilesList()) {
      out.addDataFiles(filterColumns(f, selectors));
    }
    for (ScanFile f : bundle.getDeleteFilesList()) {
      out.addDeleteFiles(filterColumns(f, selectors));
    }

    return out.build();
  }

  /** Drops column statistics not included in the projection list. */
  private static ScanFile filterColumns(ScanFile file, List<ProjectionSelector> selectors) {
    ScanFile.Builder b = file.toBuilder().clearColumns();
    for (ProjectionSelector selector : selectors) {
      for (var c : file.getColumnsList()) {
        boolean matchById =
            selector.columnId() != null
                && c.getColumnId() > 0
                && selector.columnId().longValue() == c.getColumnId();
        boolean matchByName =
            selector.columnName() != null
                && c.hasScalar()
                && selector.columnName().equals(normalizeName(c.getScalar().getDisplayName()));
        if (matchById || matchByName) {
          b.addColumns(c);
          break;
        }
      }
    }
    return b.build();
  }

  // ----------------------------------------------------------------------
  //  Predicate pruning
  // ----------------------------------------------------------------------

  /**
   * Filters out files that cannot satisfy the predicates based on min/max column statistics. This
   * is conservative: a file is only removed if it is guaranteed to not match.
   */
  private static ScanBundle prunePredicates(
      ScanBundle bundle, List<Predicate> preds, Map<String, Long> columnIdsByName) {
    if (preds == null || preds.isEmpty()) {
      return bundle;
    }

    ScanBundle.Builder out = ScanBundle.newBuilder();
    List<ScanFile> originalDeletes = bundle.getDeleteFilesList();
    Map<String, Integer> deleteIndexByPath = new HashMap<>();

    for (ScanFile f : bundle.getDeleteFilesList()) {
      if (matches(f, preds, columnIdsByName)) {
        deleteIndexByPath.put(f.getFilePath(), out.getDeleteFilesCount());
        out.addDeleteFiles(f);
      }
    }

    for (ScanFile f : bundle.getDataFilesList()) {
      if (matches(f, preds, columnIdsByName)) {
        out.addDataFiles(remapDeleteFileIndices(f, originalDeletes, deleteIndexByPath));
      }
    }

    return out.build();
  }

  private static ScanFile remapDeleteFileIndices(
      ScanFile file, List<ScanFile> originalDeletes, Map<String, Integer> deleteIndexByPath) {
    if (file.getDeleteFileIndicesCount() == 0 || deleteIndexByPath.isEmpty()) {
      return file;
    }
    var builder = file.toBuilder().clearDeleteFileIndices();
    for (int idx : file.getDeleteFileIndicesList()) {
      if (idx < 0 || idx >= originalDeletes.size()) {
        continue;
      }
      String path = originalDeletes.get(idx).getFilePath();
      Integer newIndex = deleteIndexByPath.get(path);
      if (newIndex != null) {
        builder.addDeleteFileIndices(newIndex);
      }
    }
    return builder.build();
  }

  private static boolean matches(
      ScanFile file, List<Predicate> preds, Map<String, Long> columnIdsByName) {
    for (Predicate p : preds) {
      if (!fileMayMatchPredicate(file, p, columnIdsByName)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Determines whether a file may satisfy a predicate based on its column-level min/max statistics.
   * This is a conservative check: the method returns {@code true} unless the file is guaranteed to
   * not match the predicate.
   */
  private static boolean fileMayMatchPredicate(
      ScanFile pf, Predicate p, Map<String, Long> columnIdsByName) {
    String col = p.getColumn();
    if (col == null || col.isBlank()) {
      return true;
    }

    ScalarStats cs = null;
    Long requestedColumnId =
        columnIdsByName == null ? null : columnIdsByName.get(normalizeName(col));
    for (FileColumnStats c : pf.getColumnsList()) {
      boolean matchesId = requestedColumnId != null && requestedColumnId == c.getColumnId();
      boolean matchesName =
          c.hasScalar() && col.equalsIgnoreCase(c.getScalar().getDisplayName());
      if (matchesId || matchesName) {
        cs = c.getScalar();
        break;
      }
    }

    if (cs == null) {
      return true; // no stats: keep file
    }

    LogicalType type = LogicalTypeProtoAdapter.columnLogicalType(cs);
    Object min = LogicalTypeProtoAdapter.columnMin(cs);
    Object max = LogicalTypeProtoAdapter.columnMax(cs);

    if (min == null || max == null) {
      return true;
    }

    if (!LogicalComparators.isStatsOrderable(type)) {
      return true;
    }

    BiFunction<Object, Object, Integer> cmp = (a, b) -> LogicalComparators.compare(type, a, b);

    var op = p.getOp();
    List<String> values = p.getValuesList();

    try {
      switch (op) {
        case OP_EQ -> {
          if (values.isEmpty()) return true;
          Object v = LogicalCoercions.coerceStatValue(type, values.get(0));
          if (cmp.apply(v, min) < 0 || cmp.apply(v, max) > 0) return false;
          return true;
        }

        case OP_IN -> {
          if (values.isEmpty()) return true;
          for (String vstr : values) {
            Object v = LogicalCoercions.coerceStatValue(type, vstr);
            if (cmp.apply(v, min) >= 0 && cmp.apply(v, max) <= 0) return true;
          }
          return false;
        }

        case OP_BETWEEN -> {
          if (values.size() < 2) return true;
          Object a = LogicalCoercions.coerceStatValue(type, values.get(0));
          Object b = LogicalCoercions.coerceStatValue(type, values.get(1));

          if (cmp.apply(a, b) > 0) {
            Object tmp = a;
            a = b;
            b = tmp;
          }

          if (cmp.apply(b, min) < 0 || cmp.apply(a, max) > 0) return false;
          return true;
        }

        case OP_LT -> {
          if (values.isEmpty()) return true;
          Object v = LogicalCoercions.coerceStatValue(type, values.get(0));
          if (cmp.apply(min, v) >= 0) return false;
          return true;
        }

        case OP_LTE -> {
          if (values.isEmpty()) return true;
          Object v = LogicalCoercions.coerceStatValue(type, values.get(0));
          if (cmp.apply(min, v) > 0) return false;
          return true;
        }

        case OP_GT -> {
          if (values.isEmpty()) return true;
          Object v = LogicalCoercions.coerceStatValue(type, values.get(0));
          if (cmp.apply(max, v) <= 0) return false;
          return true;
        }

        case OP_GTE -> {
          if (values.isEmpty()) return true;
          Object v = LogicalCoercions.coerceStatValue(type, values.get(0));
          if (cmp.apply(max, v) < 0) return false;
          return true;
        }

        case OP_NEQ -> {
          if (values.isEmpty()) return true;
          Object v = LogicalCoercions.coerceStatValue(type, values.get(0));
          if (cmp.apply(min, max) == 0 && cmp.apply(min, v) == 0) return false;
          return true;
        }

        case OP_IS_NULL -> {
          return true;
        }

        case OP_IS_NOT_NULL -> {
          return true;
        }

        default -> {
          return true;
        }
      }
    } catch (RuntimeException ignore) {
      // Conservative fallback: if coercion/comparison fails, keep the file.
      return true;
    }
  }

  private static List<ProjectionSelector> projectionSelectors(
      Collection<Long> requiredColumnIds,
      Collection<String> requiredColumns,
      Map<String, Long> columnIdsByName) {
    List<ProjectionSelector> selectors = new java.util.ArrayList<>();
    Set<Long> seenColumnIds = new LinkedHashSet<>();
    Set<String> seenColumnNames = new LinkedHashSet<>();

    if (requiredColumnIds != null) {
      for (Long columnId : requiredColumnIds) {
        if (columnId != null && columnId > 0 && seenColumnIds.add(columnId)) {
          selectors.add(new ProjectionSelector(columnId, null));
        }
      }
    }
    if (requiredColumns != null) {
      for (String column : requiredColumns) {
        String normalized = normalizeName(column);
        if (normalized == null) {
          continue;
        }
        Long columnId = columnIdsByName == null ? null : columnIdsByName.get(normalized);
        if (columnId != null && columnId > 0) {
          if (seenColumnIds.add(columnId)) {
            selectors.add(new ProjectionSelector(columnId, null));
          }
        } else if (seenColumnNames.add(normalized)) {
          selectors.add(new ProjectionSelector(null, normalized));
        }
      }
    }
    return selectors;
  }

  private static String normalizeName(String value) {
    if (value == null) {
      return null;
    }
    String normalized = value.trim().toLowerCase();
    return normalized.isEmpty() ? null : normalized;
  }

  private record ProjectionSelector(Long columnId, String columnName) {}
}
