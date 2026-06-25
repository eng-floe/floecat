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

package ai.floedb.floecat.service.execution.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.storage.impl.ServerSideFileIoPropertiesResolver;
import ai.floedb.floecat.stats.spi.StatsStore;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ScanBundleServiceTest {

  @Test
  void initScanUsesTableLocationForFileIoPropertiesWhileKeepingMetadataLocation() {
    var tableRepo = mock(TableRepository.class);
    var snapshotRepo = mock(SnapshotRepository.class);
    var statsStore = mock(StatsStore.class);
    var resolver = mock(ServerSideFileIoPropertiesResolver.class);

    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("tbl")
            .build();
    var table =
        Table.newBuilder()
            .setResourceId(tableId)
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
            .putProperties("storage_location", "s3://bucket/table")
            .putProperties("current-snapshot-id", "42")
            .build();
    var snapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(42L)
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
            .setMetadataLocation("s3://bucket/table/metadata/00001.metadata.json")
            .build();

    when(tableRepo.getById(tableId)).thenReturn(Optional.of(table));
    when(snapshotRepo.getById(tableId, 42L)).thenReturn(Optional.of(snapshot));
    when(resolver.applyToTableProperties(table, null, table.getPropertiesMap()))
        .thenReturn(Map.of("s3.region", "us-east-1"));

    var service = new ScanBundleService(tableRepo, snapshotRepo, statsStore, resolver);

    var init = service.initScan("corr", tableId, 42L);

    verify(resolver).applyToTableProperties(table, null, table.getPropertiesMap());
    assertEquals(
        "s3://bucket/table/metadata/00001.metadata.json", init.tableInfo().getMetadataLocation());
    assertEquals("us-east-1", init.tableInfo().getPropertiesMap().get("s3.region"));
    assertFalse(init.tableInfo().getPropertiesMap().containsKey("metadata-location"));
  }
}
