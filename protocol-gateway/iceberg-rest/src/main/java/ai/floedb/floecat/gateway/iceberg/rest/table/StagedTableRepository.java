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

package ai.floedb.floecat.gateway.iceberg.rest.table;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.eclipse.microprofile.config.Config;

@ApplicationScoped
public class StagedTableRepository {
  private static final long MIN_TTL_SECONDS = 60L;
  private final ConcurrentMap<StagedTableKey, StagedTableEntry> stages = new ConcurrentHashMap<>();
  private final Duration stageTtl;

  @Inject
  public StagedTableRepository(Config config) {
    long seconds =
        config.getOptionalValue("floecat.gateway.stage-ttl-seconds", Long.class).orElse(900L);
    if (seconds < MIN_TTL_SECONDS) {
      seconds = MIN_TTL_SECONDS;
    }
    this.stageTtl = Duration.ofSeconds(seconds);
  }

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

  public StagedTableEntry saveStage(StagedTableEntry entry) {
    StagedTableEntry stored = save(entry.initializeTimestamps(Instant.now()));
    expireStages();
    return stored;
  }

  public Optional<StagedTableEntry> get(StagedTableKey key) {
    return Optional.ofNullable(stages.get(key));
  }

  public Optional<StagedTableEntry> getStage(StagedTableKey key) {
    expireStages();
    return get(key);
  }

  public Optional<StagedTableEntry> findSingle(
      String accountId, String catalog, List<String> namespace, String tableName) {
    return findSingle(accountId, catalog, namespace, tableName, null);
  }

  private Optional<StagedTableEntry> findSingle(
      String accountId,
      String catalog,
      List<String> namespace,
      String tableName,
      StageState requiredState) {
    StagedTableEntry match = null;
    for (StagedTableEntry entry : stages.values()) {
      StagedTableKey key = entry.key();
      if (!accountId.equals(key.accountId())
          || !catalog.equals(key.catalogName())
          || !key.namespace().equals(namespace)
          || !tableName.equals(key.tableName())) {
        continue;
      }
      if (requiredState != null && entry.state() != requiredState) {
        continue;
      }
      if (match != null) {
        return Optional.empty();
      }
      match = entry;
    }
    return Optional.ofNullable(match);
  }

  public Optional<StagedTableEntry> findSingleStage(
      String accountId, String catalog, List<String> namespace, String tableName) {
    expireStages();
    return findSingle(accountId, catalog, namespace, tableName, StageState.STAGED);
  }

  public void delete(StagedTableKey key) {
    stages.remove(key);
  }

  public void deleteStage(StagedTableKey key) {
    delete(key);
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

  public int expireStages() {
    Instant cutoff = Instant.now().minus(stageTtl);
    return deleteOlderThan(cutoff);
  }

  public void clear() {
    stages.clear();
  }

  Duration stageTtl() {
    return stageTtl;
  }
}
