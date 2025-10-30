package ai.floedb.metacat.connector.common;

import ai.floedb.metacat.connector.common.ndv.ColumnNdv;
import ai.floedb.metacat.types.LogicalType;
import java.util.Map;
import java.util.Optional;

public interface StatsEngine<K> {

  Result<K> compute();

  default Optional<String> columnNameFor(K colKey) {
    return Optional.empty();
  }

  default Optional<LogicalType> logicalTypeFor(K colKey) {
    return Optional.empty();
  }

  interface Result<K> {
    long totalRowCount();

    long totalSizeBytes();

    long fileCount();

    Map<K, ColumnAgg> columns();
  }

  interface ColumnAgg {
    Long ndvExact();

    ColumnNdv ndv();

    Long valueCount();

    Long nullCount();

    Long nanCount();

    Object min();

    Object max();
  }
}
