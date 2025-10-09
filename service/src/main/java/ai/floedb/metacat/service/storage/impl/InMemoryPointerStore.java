package ai.floedb.metacat.service.storage.impl;

import java.util.Optional;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.ApplicationScoped;
import io.quarkus.arc.properties.IfBuildProperty;

import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.service.storage.PointerStore;

@ApplicationScoped
@IfBuildProperty(name = "metacat.blob", stringValue = "memory")
public class InMemoryPointerStore implements PointerStore {
  private final Map<String, Pointer> map = new ConcurrentHashMap<>();

  @Override public Optional<Pointer> get(String key) { 
    return Optional.ofNullable(map.get(key)); 
  }

  @Override public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
    final boolean[] updated = { false };
    map.compute(key, (k, cur) -> {
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

  @Override public List<String> listByPrefix(String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
    List<String> keys = new ArrayList<>();
    for (String k : map.keySet()) if (k.startsWith(prefix)) keys.add(k);
    Collections.sort(keys);
    int start = 0;
    if (pageToken != null && !pageToken.isEmpty()) {
      int idx = Collections.binarySearch(keys, pageToken);
      start = (idx >= 0) ? idx : -idx - 1;
    }
    int end = Math.min(keys.size(), start + limit);
    List<String> page = keys.subList(start, end);
    if (end < keys.size()) {
      nextTokenOut.append(keys.get(end));
    }
    return new ArrayList<>(page);
  }

  @Override public boolean delete(String key) {
    if (!map.containsKey(key)) {
      return false;
    }
    map.remove(key);
    return true;
  }
}