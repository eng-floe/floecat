package ai.floedb.metacat.connector.common;

import ai.floedb.metacat.connector.common.ndv.NdvProvider;
import ai.floedb.metacat.types.LogicalType;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public interface Planner<K> extends AutoCloseable, Iterable<PlannedFile<K>> {
  Map<K, String> columnNamesByKey();

  Map<K, LogicalType> logicalTypesByKey();

  NdvProvider ndvProvider();

  Set<K> columns();

  default Function<K, String> nameOf() {
    var byKey = columnNamesByKey();
    return byKey::get;
  }

  default Function<K, LogicalType> typeOf() {
    var byKey = logicalTypesByKey();
    return byKey::get;
  }

  @Override
  default void close() {}
}
