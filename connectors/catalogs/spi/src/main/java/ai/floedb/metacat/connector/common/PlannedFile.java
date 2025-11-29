package ai.floedb.metacat.connector.common;

import java.util.Map;

public record PlannedFile<K>(
    String path,
    long rowCount,
    long sizeBytes,
    Map<K, Long> valueCounts,
    Map<K, Long> nullCounts,
    Map<K, Long> nanCounts,
    Map<K, Object> lowerBounds,
    Map<K, Object> upperBounds,
    String partitionDataJson,
    int partitionSpecId) {}
