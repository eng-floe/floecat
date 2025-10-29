package ai.floedb.metacat.connector.common;

import ai.floedb.metacat.types.LogicalType;
import java.util.Map;
import java.util.Optional;

/** Format-agnostic stats engine. No protobufs, no Iceberg/Delta types. */
public interface StatsEngine<K> {

  /** Run the computation once over the planner's file plan. */
  Result<K> compute();

  /** Optional helpers so callers can label columns in results. */
  default Optional<String> columnNameFor(K colKey) {
    return Optional.empty();
  }

  default Optional<LogicalType> logicalTypeFor(K colKey) {
    return Optional.empty();
  }

  /** Immutable result of a stats run. */
  interface Result<K> {
    long totalRowCount();

    long totalSizeBytes();

    long fileCount();

    /** Aggregates per column key (e.g., Iceberg field-id). */
    Map<K, ColumnAgg> columns();
  }

  /** Per-column aggregates kept neutral (no proto types). */
  interface ColumnAgg {
    /** Distinct values: exact if known; else null. */
    Long ndvExact();

    /** NDV as HLL sketch bytes if computed; else null. */
    byte[] ndvHll();

    /** Count of non-null values if available; may be null. */
    Long valueCount();

    /** Count of nulls if available; may be null. */
    Long nullCount();

    /** Count of NaNs if available; may be null (float/double only). */
    Long nanCount();

    /** Typed min/max (use LogicalComparators/ValueEncoders with the LogicalType). */
    Object min();

    Object max();
  }
}
