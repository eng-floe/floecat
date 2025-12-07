package ai.floedb.metacat.gateway.iceberg.rest.services.staging;

import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@ApplicationScoped
public class StagedTableRepository {
  private final ConcurrentMap<StagedTableKey, StagedTableEntry> stages = new ConcurrentHashMap<>();

  public StagedTableEntry save(StagedTableEntry entry) {
    return stages.compute(
        entry.key(),
        (key, existing) -> {
          if (existing != null
              && existing.state() == StageState.STAGED
              && entry.state() == StageState.STAGED) {
            return existing;
          }
          return entry;
        });
  }

  public Optional<StagedTableEntry> get(StagedTableKey key) {
    return Optional.ofNullable(stages.get(key));
  }

  public Optional<StagedTableEntry> findSingle(
      String tenantId, String catalog, List<String> namespace, String tableName) {
    StagedTableEntry match = null;
    for (StagedTableEntry entry : stages.values()) {
      StagedTableKey key = entry.key();
      if (!tenantId.equals(key.tenantId())
          || !catalog.equals(key.catalogName())
          || !key.namespace().equals(namespace)
          || !tableName.equals(key.tableName())) {
        continue;
      }
      if (match != null) {
        return Optional.empty();
      }
      match = entry;
    }
    return Optional.ofNullable(match);
  }

  public void delete(StagedTableKey key) {
    stages.remove(key);
  }

  public int deleteOlderThan(Instant cutoff) {
    int removed = 0;
    for (var entry : stages.entrySet()) {
      Instant updated = entry.getValue().updatedAt();
      if (updated != null && updated.isBefore(cutoff)) {
        if (stages.remove(entry.getKey(), entry.getValue())) {
          removed++;
        }
      }
    }
    return removed;
  }

  public void clear() {
    stages.clear();
  }
}
