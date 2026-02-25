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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableCommitSideEffectService.ConnectorSyncResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class PostCommitSyncOutboxServiceTest {

  @Test
  void enqueueRetriesOnDrainAndSuppressesDuplicateQueuedTask() throws Exception {
    TableCommitSideEffectService sideEffectService =
        Mockito.mock(TableCommitSideEffectService.class);
    TableGatewaySupport tableGatewaySupport = Mockito.mock(TableGatewaySupport.class);
    PostCommitSyncOutboxService outbox = new PostCommitSyncOutboxService();
    outbox.sideEffectService = sideEffectService;
    outbox.tableGatewaySupport = tableGatewaySupport;
    outbox.mapper = new ObjectMapper();
    outbox.config = configWithStoreDir(null);
    outbox.init();

    when(sideEffectService.runConnectorSyncAttempt(any(), any(), any(), any()))
        .thenReturn(new ConnectorSyncResult(false, true, false))
        .thenReturn(new ConnectorSyncResult(true, true, false));

    ResourceId connectorId = ResourceId.newBuilder().setId("conn-1").build();
    outbox.enqueueConnectorSync(
        "dedupe-1", tableGatewaySupport, connectorId, List.of("db"), "orders");
    outbox.enqueueConnectorSync(
        "dedupe-1", tableGatewaySupport, connectorId, List.of("db"), "orders");
    drainUntilAttempts(sideEffectService, outbox, 2);

    verify(sideEffectService, times(2)).runConnectorSyncAttempt(any(), any(), any(), any());
    outbox.close();
  }

  @Test
  void outboxLoadsPersistedTaskAcrossInstances() throws Exception {
    Path dir = Files.createTempDirectory("post-commit-outbox");
    Config cfg = configWithStoreDir(dir.toString());

    TableCommitSideEffectService failingSideEffect =
        Mockito.mock(TableCommitSideEffectService.class);
    TableGatewaySupport tableGatewaySupport = Mockito.mock(TableGatewaySupport.class);
    when(failingSideEffect.runConnectorSyncAttempt(any(), any(), any(), any()))
        .thenReturn(new ConnectorSyncResult(false, true, false));

    PostCommitSyncOutboxService first = new PostCommitSyncOutboxService();
    first.sideEffectService = failingSideEffect;
    first.tableGatewaySupport = tableGatewaySupport;
    first.mapper = new ObjectMapper();
    first.config = cfg;
    first.init();
    first.enqueueConnectorSync(
        "dedupe-2",
        tableGatewaySupport,
        ResourceId.newBuilder().setId("conn-2").build(),
        List.of("db"),
        "orders");

    TableCommitSideEffectService succeedingSideEffect =
        Mockito.mock(TableCommitSideEffectService.class);
    when(succeedingSideEffect.runConnectorSyncAttempt(any(), any(), any(), any()))
        .thenReturn(new ConnectorSyncResult(true, true, false));

    PostCommitSyncOutboxService second = new PostCommitSyncOutboxService();
    second.sideEffectService = succeedingSideEffect;
    second.tableGatewaySupport = tableGatewaySupport;
    second.mapper = new ObjectMapper();
    second.config = cfg;
    second.init();
    drainUntilAttempts(succeedingSideEffect, second, 1);

    verify(succeedingSideEffect, times(1)).runConnectorSyncAttempt(any(), any(), any(), any());
    first.close();
    second.close();
  }

  private Config configWithStoreDir(String dir) {
    Config cfg = Mockito.mock(Config.class);
    when(cfg.getOptionalValue(eq("floecat.gateway.post-commit-outbox.enabled"), eq(Boolean.class)))
        .thenReturn(Optional.of(true));
    when(cfg.getOptionalValue(
            eq("floecat.gateway.post-commit-outbox.worker-enabled"), eq(Boolean.class)))
        .thenReturn(Optional.of(false));
    when(cfg.getOptionalValue(
            eq("floecat.gateway.post-commit-outbox.max-attempts"), eq(Integer.class)))
        .thenReturn(Optional.of(4));
    when(cfg.getOptionalValue(
            eq("floecat.gateway.post-commit-outbox.drain-batch-size"), eq(Integer.class)))
        .thenReturn(Optional.of(4));
    when(cfg.getOptionalValue(
            eq("floecat.gateway.post-commit-outbox.base-backoff-ms"), eq(Long.class)))
        .thenReturn(Optional.of(1L));
    when(cfg.getOptionalValue(
            eq("floecat.gateway.post-commit-outbox.poll-every-ms"), eq(Long.class)))
        .thenReturn(Optional.of(10_000L));
    when(cfg.getOptionalValue(eq("floecat.gateway.post-commit-outbox.store-dir"), eq(String.class)))
        .thenReturn(Optional.ofNullable(dir));
    return cfg;
  }

  private void drainUntilAttempts(
      TableCommitSideEffectService sideEffectService,
      PostCommitSyncOutboxService outbox,
      int expectedCalls)
      throws Exception {
    for (int i = 0; i < 40; i++) {
      outbox.drainNow(4);
      int calls = Mockito.mockingDetails(sideEffectService).getInvocations().size();
      if (calls >= expectedCalls) {
        return;
      }
      Thread.sleep(25L);
    }
  }
}
