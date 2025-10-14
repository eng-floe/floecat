package ai.floedb.metacat.service.repo;

import java.util.List;
import java.util.Optional;

public interface Repository<T> {
  Optional<T> get(String key);
  void put(String key, String blobUri, T value);
  boolean update(String key, String blobUri, T value, long expectedVersion);
  List<T> listByPrefix(String prefix, int limit, String pageToken, StringBuilder nextTokenOut);
  int countByPrefix(String prefix);
}
