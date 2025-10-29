package ai.floedb.metacat.connector.common;

import java.util.List;

public record TableStats(
    long fileCount, long totalRecordCount, long totalSizeBytes, List<ColumnStat> columns) {}
