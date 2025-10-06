package ai.floedb.metacat.service.storage;

import java.util.Optional;
import java.util.List;

public interface PointerStore {
  Optional<ai.floedb.metacat.common.rpc.Pointer> get(String key);
  boolean compareAndSet(String key, long expectedVersion, ai.floedb.metacat.common.rpc.Pointer next);
  List<String> listByPrefix(String prefix, int limit, String pageToken, StringBuilder nextTokenOut);
  void put(String key, ai.floedb.metacat.common.rpc.Pointer p);
}