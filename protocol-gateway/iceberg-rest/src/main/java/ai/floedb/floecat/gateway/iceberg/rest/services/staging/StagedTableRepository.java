/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.gateway.iceberg.rest.services.staging;

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
      String accountId, String catalog, List<String> namespace, String tableName) {
    StagedTableEntry match = null;
    for (StagedTableEntry entry : stages.values()) {
      StagedTableKey key = entry.key();
      if (!accountId.equals(key.accountId())
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
