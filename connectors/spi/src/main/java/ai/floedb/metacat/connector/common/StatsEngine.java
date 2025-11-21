package ai.floedb.metacat.connector.common;

import ai.floedb.metacat.connector.common.ndv.ColumnNdv;
import ai.floedb.metacat.types.LogicalType;
import java.util.List;
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

    default List<FileAgg<K>> files() {
      return List.of();
    }
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

  interface FileAgg<K> {
    String path();

    long rowCount();

    long sizeBytes();

    Map<K, ColumnAgg> columns();
  }
}
