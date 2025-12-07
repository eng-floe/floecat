package ai.floedb.floecat.connector.common;

import ai.floedb.floecat.types.LogicalType;

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
