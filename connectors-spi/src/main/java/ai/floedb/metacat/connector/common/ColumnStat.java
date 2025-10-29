package ai.floedb.metacat.connector.common;

import ai.floedb.metacat.types.LogicalType;

public record ColumnStat(
    int fieldId,
    String name,
    LogicalType type,
    Long valueCount,
    Long nullCount,
    Long nanCount,
    Object min,
    Object max,
    Long ndv) {}
