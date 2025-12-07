package ai.floedb.floecat.storage;

import ai.floedb.floecat.common.rpc.Pointer;
import java.util.List;
import java.util.Optional;

public interface PointerStore {
  Optional<Pointer> get(String key);

  boolean compareAndSet(String key, long expectedVersion, Pointer next);

  boolean delete(String key);

  boolean compareAndDelete(String key, long expectedVersion);

  List<Pointer> listPointersByPrefix(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut);

  int deleteByPrefix(String prefix);

  int countByPrefix(String prefix);
}
