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
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.config.Config;

@ApplicationScoped
public class StagedTableService {
  private static final long MIN_TTL_SECONDS = 60L;

  private final StagedTableRepository repository;
  private final Duration stageTtl;

  @Inject
  public StagedTableService(StagedTableRepository repository, Config config) {
    this.repository = repository;
    long seconds =
        config.getOptionalValue("floecat.gateway.stage-ttl-seconds", Long.class).orElse(900L);
    if (seconds < MIN_TTL_SECONDS) {
      seconds = MIN_TTL_SECONDS;
    }
    this.stageTtl = Duration.ofSeconds(seconds);
  }

  public StagedTableEntry saveStage(StagedTableEntry entry) {
    StagedTableEntry stored = repository.save(entry.initializeTimestamps(Instant.now()));
    expireStages();
    return stored;
  }

  public Optional<StagedTableEntry> getStage(StagedTableKey key) {
    expireStages();
    return repository.get(key);
  }

  public Optional<StagedTableEntry> findSingleStage(
      String accountId, String catalogName, List<String> namespacePath, String tableName) {
    expireStages();
    return repository.findSingle(accountId, catalogName, namespacePath, tableName);
  }

  public void deleteStage(StagedTableKey key) {
    repository.delete(key);
  }

  public int expireStages() {
    Instant cutoff = Instant.now().minus(stageTtl);
    return repository.deleteOlderThan(cutoff);
  }

  Duration stageTtl() {
    return stageTtl;
  }
}
