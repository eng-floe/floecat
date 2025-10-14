package ai.floedb.metacat.service.storage;

import java.util.*;

public interface PointerStore {
  record Row(String key, String blobUri, long version) {}

  Optional<ai.floedb.metacat.common.rpc.Pointer> get(String key);
  boolean compareAndSet(String key, long expectedVersion, ai.floedb.metacat.common.rpc.Pointer next);
  boolean delete(String key);
  boolean compareAndDelete(String key, long expectedVersion);
  List<Row> listPointersByPrefix(String prefix, int limit, String pageToken, StringBuilder nextTokenOut);
  int countByPrefix(String prefix);
}
