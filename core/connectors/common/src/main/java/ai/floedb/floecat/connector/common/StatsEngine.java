package ai.floedb.floecat.connector.common;

import ai.floedb.floecat.connector.common.ndv.ColumnNdv;
import ai.floedb.floecat.types.LogicalType;
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

    String format();

    long rowCount();

    long sizeBytes();

    Map<K, ColumnAgg> columns();

    default String partitionDataJson() {
      return null;
    }

    default int partitionSpecId() {
      return 0;
    }

    default Long sequenceNumber() {
      return null;
    }

    default boolean isDelete() {
      return false;
    }

    default boolean isEqualityDelete() {
      return false;
    }
  }
}
