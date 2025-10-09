package ai.floedb.metacat.service.storage;

import java.util.Optional;
import java.util.List;

import ai.floedb.metacat.common.rpc.Pointer;

public interface PointerStore {
  Optional<Pointer> get(String key);
  boolean compareAndSet(String key, long expectedVersion, Pointer next);
  List<String> listByPrefix(String prefix, int limit, String pageToken, StringBuilder nextTokenOut);
  boolean delete(String key);
}