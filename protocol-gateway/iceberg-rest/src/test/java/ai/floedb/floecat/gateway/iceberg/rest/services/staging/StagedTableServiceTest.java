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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class StagedTableServiceTest {
  @Test
  void constructorUsesConfiguredTtlAndMinimum() {
    StagedTableRepository repository = mock(StagedTableRepository.class);
    Config config = mock(Config.class);
    when(config.getOptionalValue("floecat.gateway.stage-ttl-seconds", Long.class))
        .thenReturn(Optional.of(30L));
    StagedTableService minTtlService = new StagedTableService(repository, config);
    assertEquals(Duration.ofSeconds(60), minTtlService.stageTtl());

    when(config.getOptionalValue("floecat.gateway.stage-ttl-seconds", Long.class))
        .thenReturn(Optional.of(1200L));
    StagedTableService configuredTtlService = new StagedTableService(repository, config);
    assertEquals(Duration.ofSeconds(1200), configuredTtlService.stageTtl());
  }

  @Test
  void saveStageInitializesTimestampsAndExpires() {
    StagedTableRepository repository = mock(StagedTableRepository.class);
    Config config = mock(Config.class);
    when(config.getOptionalValue("floecat.gateway.stage-ttl-seconds", Long.class))
        .thenReturn(Optional.of(900L));
    StagedTableService service = new StagedTableService(repository, config);

    StagedTableEntry entry = stagedEntry();
    when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0, StagedTableEntry.class));
    when(repository.deleteOlderThan(any())).thenReturn(0);

    StagedTableEntry stored = service.saveStage(entry);

    assertSame(entry.key(), stored.key());
    assertEquals(StageState.STAGED, stored.state());
    assertEquals(stored.createdAt(), stored.updatedAt());
    verify(repository).save(any(StagedTableEntry.class));
    verify(repository).deleteOlderThan(any(Instant.class));
  }

  @Test
  void getAndFindSingleExpireBeforeLookup() {
    StagedTableRepository repository = mock(StagedTableRepository.class);
    Config config = mock(Config.class);
    when(config.getOptionalValue("floecat.gateway.stage-ttl-seconds", Long.class))
        .thenReturn(Optional.of(900L));
    StagedTableService service = new StagedTableService(repository, config);

    StagedTableEntry entry = stagedEntry();
    StagedTableKey key = entry.key();
    when(repository.deleteOlderThan(any())).thenReturn(0);
    when(repository.get(key)).thenReturn(Optional.of(entry));
    when(repository.findSingle("acct", "cat", List.of("db"), "orders")).thenReturn(Optional.of(entry));

    Optional<StagedTableEntry> fetched = service.getStage(key);
    Optional<StagedTableEntry> single = service.findSingleStage("acct", "cat", List.of("db"), "orders");

    assertEquals(Optional.of(entry), fetched);
    assertEquals(Optional.of(entry), single);
    verify(repository).get(key);
    verify(repository).findSingle("acct", "cat", List.of("db"), "orders");
  }

  @Test
  void deleteAndExpireDelegateToRepository() {
    StagedTableRepository repository = mock(StagedTableRepository.class);
    Config config = mock(Config.class);
    when(config.getOptionalValue("floecat.gateway.stage-ttl-seconds", Long.class))
        .thenReturn(Optional.of(900L));
    StagedTableService service = new StagedTableService(repository, config);

    StagedTableKey key = stagedEntry().key();
    service.deleteStage(key);
    verify(repository).delete(key);

    when(repository.deleteOlderThan(any())).thenReturn(3);
    int removed = service.expireStages();
    assertEquals(3, removed);
    ArgumentCaptor<Instant> cutoffCaptor = ArgumentCaptor.forClass(Instant.class);
    verify(repository).deleteOlderThan(cutoffCaptor.capture());
  }

  private static StagedTableEntry stagedEntry() {
    return new StagedTableEntry(
        new StagedTableKey("acct", "cat", List.of("db"), "orders", "stage-1"),
        ResourceId.newBuilder().setId("cat").build(),
        ResourceId.newBuilder().setId("cat:db").build(),
        new TableRequests.Create("orders", null, null, Map.of(), null, null, true),
        TableSpec.newBuilder().setDisplayName("orders").build(),
        List.of(),
        StageState.STAGED,
        null,
        null,
        "idem-1");
  }
}
