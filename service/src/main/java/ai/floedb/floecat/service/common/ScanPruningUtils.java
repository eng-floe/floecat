package ai.floedb.floecat.service.common;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.execution.rpc.ScanBundle;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.types.LogicalCoercions;
import ai.floedb.floecat.types.LogicalComparators;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeProtoAdapter;
import java.util.HashSet;
import java.util.List;
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

    // Convert raw connector bundle → RPC bundle so we operate on the same type everywhere.
    ScanBundle.Builder builder = ScanBundle.newBuilder();
    builder.addAllDataFiles(raw.dataFiles());
    builder.addAllDeleteFiles(raw.deleteFiles());
    ScanBundle initial = builder.build();

    ScanBundle afterProjection = pruneColumns(initial, requiredColumns);
    return prunePredicates(afterProjection, predicates);
  }

  // ----------------------------------------------------------------------
  //  Column pruning
  // ----------------------------------------------------------------------
  private static ScanBundle pruneColumns(ScanBundle bundle, List<String> requiredColumns) {
    if (requiredColumns == null || requiredColumns.isEmpty()) {
      return bundle;
    }

    Set<String> cols = new HashSet<>(requiredColumns);

    ScanBundle.Builder out = ScanBundle.newBuilder();

    for (ScanFile f : bundle.getDataFilesList()) {
      out.addDataFiles(filterColumns(f, cols));
    }
    for (ScanFile f : bundle.getDeleteFilesList()) {
      out.addDeleteFiles(filterColumns(f, cols));
    }

    return out.build();
  }

  /** Drops column statistics not included in the projection list. */
  private static ScanFile filterColumns(ScanFile file, Set<String> cols) {
    ScanFile.Builder b = file.toBuilder().clearColumns();
    for (var c : file.getColumnsList()) {
      if (cols.contains(c.getColumnName())) {
        b.addColumns(c);
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
  private static ScanBundle prunePredicates(ScanBundle bundle, List<Predicate> preds) {
    if (preds == null || preds.isEmpty()) {
      return bundle;
    }

    ScanBundle.Builder out = ScanBundle.newBuilder();

    for (ScanFile f : bundle.getDataFilesList()) {
      if (matches(f, preds)) {
        out.addDataFiles(f);
      }
    }
    for (ScanFile f : bundle.getDeleteFilesList()) {
      if (matches(f, preds)) {
        out.addDeleteFiles(f);
      }
    }

    return out.build();
  }

  private static boolean matches(ScanFile file, List<Predicate> preds) {
    for (Predicate p : preds) {
      if (!fileMayMatchPredicate(file, p)) {
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
  private static boolean fileMayMatchPredicate(ScanFile pf, Predicate p) {
    String col = p.getColumn();
    if (col == null || col.isBlank()) {
      return true;
    }

    ColumnStats cs = null;
    for (ColumnStats c : pf.getColumnsList()) {
      if (col.equalsIgnoreCase(c.getColumnName())) {
        cs = c;
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

    BiFunction<Object, Object, Integer> cmp = (a, b) -> LogicalComparators.compare(type, a, b);

    var op = p.getOp();
    List<String> values = p.getValuesList();

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
  }
}
