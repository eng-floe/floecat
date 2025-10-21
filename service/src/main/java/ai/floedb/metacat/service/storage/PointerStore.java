package ai.floedb.metacat.service.storage;

import java.util.*;

import ai.floedb.metacat.common.rpc.Pointer;

public interface PointerStore {
  record Row(String key, String blobUri, long version) {}

  Optional<Pointer> get(String key);
  boolean compareAndSet(String key, long expectedVersion, Pointer next);
  boolean delete(String key);
  boolean compareAndDelete(String key, long expectedVersion);
  List<Row> listPointersByPrefix(String prefix, int limit,
      String pageToken, StringBuilder nextTokenOut);
  int countByPrefix(String prefix);
}
