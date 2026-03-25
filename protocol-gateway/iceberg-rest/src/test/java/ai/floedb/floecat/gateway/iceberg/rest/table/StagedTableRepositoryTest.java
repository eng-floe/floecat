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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;

class StagedTableRepositoryTest {

  @Test
  void stageTtlHonorsMinimumAndConfiguredValues() {
    Config config = mock(Config.class);
    when(config.getOptionalValue("floecat.gateway.stage-ttl-seconds", Long.class))
        .thenReturn(Optional.of(5L), Optional.of(1800L));

    StagedTableRepository minTtlRepository = new StagedTableRepository(config);
    StagedTableRepository configuredTtlRepository = new StagedTableRepository(config);

    assertEquals(Duration.ofSeconds(60), minTtlRepository.stageTtl());
    assertEquals(Duration.ofSeconds(1800), configuredTtlRepository.stageTtl());
  }

  @Test
  void saveStageInitializesTimestampsAndSupportsLookups() {
    Config config = mock(Config.class);
    when(config.getOptionalValue("floecat.gateway.stage-ttl-seconds", Long.class))
        .thenReturn(Optional.of(900L));
    StagedTableRepository repository = new StagedTableRepository(config);

    StagedTableEntry entry = stagedEntry();
    StagedTableEntry stored = repository.saveStage(entry);
    StagedTableKey key = entry.key();

    Optional<StagedTableEntry> fetched = repository.getStage(key);
    Optional<StagedTableEntry> single =
        repository.findSingleStage("acct", "cat", List.of("db"), "orders");

    assertTrue(stored.createdAt() != null);
    assertEquals(stored, fetched.orElseThrow());
    assertEquals(stored, single.orElseThrow());
  }

  @Test
  void findSingleStageIgnoresNonStagedEntries() {
    Config config = mock(Config.class);
    when(config.getOptionalValue("floecat.gateway.stage-ttl-seconds", Long.class))
        .thenReturn(Optional.of(900L));
    StagedTableRepository repository = new StagedTableRepository(config);

    StagedTableEntry staged = repository.saveStage(stagedEntry());
    repository.save(withState(stagedEntryWithStageId("stage-2"), StageState.ABORTED));

    Optional<StagedTableEntry> single =
        repository.findSingleStage("acct", "cat", List.of("db"), "orders");

    assertEquals(staged, single.orElseThrow());
  }

  @Test
  void findSingleStageReturnsEmptyWhenMultipleStagedEntriesExist() {
    Config config = mock(Config.class);
    when(config.getOptionalValue("floecat.gateway.stage-ttl-seconds", Long.class))
        .thenReturn(Optional.of(900L));
    StagedTableRepository repository = new StagedTableRepository(config);

    repository.saveStage(stagedEntry());
    repository.saveStage(stagedEntryWithStageId("stage-2"));

    assertTrue(repository.findSingleStage("acct", "cat", List.of("db"), "orders").isEmpty());
  }

  @Test
  void deleteAndExpireStagesRemoveEntries() {
    Config config = mock(Config.class);
    when(config.getOptionalValue("floecat.gateway.stage-ttl-seconds", Long.class))
        .thenReturn(Optional.of(900L));
    StagedTableRepository repository = new StagedTableRepository(config);

    StagedTableEntry fresh = repository.saveStage(stagedEntry());
    repository.deleteStage(fresh.key());
    assertTrue(repository.getStage(fresh.key()).isEmpty());

    StagedTableEntry stale =
        new StagedTableEntry(
            stagedEntry().key(),
            stagedEntry().catalogId(),
            stagedEntry().namespaceId(),
            stagedEntry().request(),
            stagedEntry().spec(),
            stagedEntry().requirements(),
            stagedEntry().state(),
            Instant.now().minusSeconds(3600),
            Instant.now().minusSeconds(3600),
            stagedEntry().idempotencyKey());
    repository.save(stale);

    assertEquals(1, repository.expireStages());
    assertTrue(repository.get(stale.key()).isEmpty());
  }

  private static StagedTableEntry stagedEntry() {
    return stagedEntryWithStageId("stage-1");
  }

  private static StagedTableEntry stagedEntryWithStageId(String stageId) {
    return new StagedTableEntry(
        new StagedTableKey("acct", "cat", List.of("db"), "orders", stageId),
        ai.floedb.floecat.common.rpc.ResourceId.newBuilder().setId("cat").build(),
        ai.floedb.floecat.common.rpc.ResourceId.newBuilder().setId("ns").build(),
        new ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests.Create(
            "orders", null, null, null, null, null, null),
        ai.floedb.floecat.catalog.rpc.TableSpec.newBuilder().build(),
        List.of(),
        StageState.STAGED,
        null,
        null,
        "idem");
  }

  private static StagedTableEntry withState(StagedTableEntry entry, StageState state) {
    return new StagedTableEntry(
        entry.key(),
        entry.catalogId(),
        entry.namespaceId(),
        entry.request(),
        entry.spec(),
        entry.requirements(),
        state,
        entry.createdAt(),
        entry.updatedAt(),
        entry.idempotencyKey());
  }
}
