package ai.floedb.metacat.service.storage.impl;

import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.service.storage.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import io.quarkus.arc.Unremovable;

import java.util.Optional;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
@Unremovable
public class InMemoryPointerStore implements PointerStore {
  private final Map<String, Pointer> map = new ConcurrentHashMap<>();

  @Override public Optional<Pointer> get(String key) { return Optional.ofNullable(map.get(key)); }

  @Override public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
    Pointer cur = map.get(key);
    if (cur == null && expectedVersion == 0L) { map.put(key, next); return true; }
    if (cur != null && cur.getVersion() == expectedVersion) { map.put(key, next); return true; }
    return false;
  }

  @Override public List<String> listByPrefix(String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
    List<String> keys = new ArrayList<>();
    for (String k : map.keySet()) if (k.startsWith(prefix)) keys.add(k);
    Collections.sort(keys);
    int start = 0;
    if (pageToken != null && !pageToken.isEmpty()) start = Math.max(0, Collections.binarySearch(keys, pageToken));
    int end = Math.min(keys.size(), start + limit);
    List<String> page = keys.subList(start, end);
    if (end < keys.size()) nextTokenOut.append(keys.get(end));
    return new ArrayList<>(page);
  }

  @Override public void put(String key, Pointer p) { map.put(key, p); }
}