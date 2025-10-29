package ai.floedb.metacat.connector.common;

import ai.floedb.metacat.connector.common.ndv.NdvProvider;
import ai.floedb.metacat.types.LogicalType;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;

public interface Planner<K> extends AutoCloseable, Iterable<PlannedFile<K>> {
  @Override
  Iterator<PlannedFile<K>> iterator();

  @Override
  void close();

  Function<K, String> nameOf();

  Function<K, LogicalType> typeOf();

  NdvProvider ndvProvider();

  Set<K> columns();
}
