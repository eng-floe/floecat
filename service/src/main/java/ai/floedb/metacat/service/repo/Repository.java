package ai.floedb.metacat.service.repo;

import java.util.List;
import java.util.Optional;

public interface Repository<T> {
  Optional<T> get(String key);
  void putBlob(String blobUri, T value);
  void advancePointer(String key, String blobUri, long expectedVersion);
  List<T> listByPrefix(String prefix, int limit, String pageToken, StringBuilder nextTokenOut);
  int countByPrefix(String prefix);
}
