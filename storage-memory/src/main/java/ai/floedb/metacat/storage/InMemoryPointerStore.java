package ai.floedb.metacat.storage;

import ai.floedb.metacat.common.rpc.Pointer;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
@IfBuildProperty(name = "metacat.kv", stringValue = "memory")
public class InMemoryPointerStore implements PointerStore {
  private final Map<String, Pointer> map = new ConcurrentHashMap<>();

  @Override
  public Optional<Pointer> get(String key) {
    return Optional.ofNullable(map.get(key));
  }

  @Override
  public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
    final boolean[] updated = {false};
    map.compute(
        key,
        (k, cur) -> {
          if (cur == null) {
            if (expectedVersion == 0L) {
              updated[0] = true;
              return next;
            }
            return null;
          }
          if (cur.getVersion() == expectedVersion) {
            updated[0] = true;
            return next;
          }
          return cur;
        });

    return updated[0];
  }

  @Override
  public List<Row> listPointersByPrefix(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
    final String pfx = prefix == null ? "" : prefix;
    final int lim = Math.max(1, limit);

    List<String> keys = new ArrayList<>();
    for (String k : map.keySet()) {
      if (k.startsWith(pfx)) {
        keys.add(k);
      }
    }
    Collections.sort(keys);

    int start = 0;
    if (pageToken != null && !pageToken.isEmpty()) {
      int idx = Collections.binarySearch(keys, pageToken);
      start = (idx >= 0) ? idx + 1 : (-idx - 1);
    }
    if (start >= keys.size()) {
      if (nextTokenOut != null) {
        nextTokenOut.setLength(0);
      }

      return Collections.emptyList();
    }

    int end = Math.min(keys.size(), start + lim);
    List<Row> page = new ArrayList<>(end - start);
    for (int i = start; i < end; i++) {
      String key = keys.get(i);
      Pointer p = map.get(key);
      if (p != null) {
        page.add(new Row(key, p.getBlobUri(), p.getVersion()));
      }
    }

    if (nextTokenOut != null) {
      nextTokenOut.setLength(0);
      if (end < keys.size()) {
        nextTokenOut.append(keys.get(end - 1));
      }
    }

    return page;
  }

  @Override
  public int countByPrefix(String prefix) {
    final String pfx = prefix == null ? "" : prefix;
    int n = 0;
    for (String k : map.keySet()) {
      if (k.startsWith(pfx)) {
        n++;
      }
    }

    return n;
  }

  @Override
  public boolean delete(String key) {
    return map.remove(key) != null;
  }

  @Override
  public boolean compareAndDelete(String key, long expectedVersion) {
    final boolean[] deleted = {false};
    map.compute(
        key,
        (k, cur) -> {
          if (cur == null) {
            return null;
          }

          if (cur.getVersion() == expectedVersion) {
            deleted[0] = true;
            return null;
          }

          return cur;
        });

    return deleted[0];
  }
}
