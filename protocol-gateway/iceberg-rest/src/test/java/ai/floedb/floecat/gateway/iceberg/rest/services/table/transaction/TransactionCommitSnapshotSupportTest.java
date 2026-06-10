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

package ai.floedb.floecat.gateway.iceberg.rest.services.table.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TransactionCommitSnapshotSupportTest {

  @Test
  void planAtomicSnapshotChangesPrefersMaterializedMetadataLocationOverRequestSnapshotLocation()
      throws Exception {
    TransactionCommitSnapshotSupport support = new TransactionCommitSnapshotSupport();
    support.grpcClient = Mockito.mock(GrpcServiceFacade.class);
    support.transactionCommitExecutionSupport =
        Mockito.mock(TransactionCommitExecutionSupport.class);

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build();
    List<Map<String, Object>> updates =
        List.of(
            Map.of(
                "action",
                "add-snapshot",
                "snapshot",
                Map.of(
                    "snapshot-id",
                    4652753989274070009L,
                    "timestamp-ms",
                    1781027618000L,
                    "metadata-location",
                    "s3://floecat/iceberg/trino_fmt_v1_smoke/metadata/00000-request.metadata.json",
                    "summary",
                    Map.of("operation", "append"))));

    var result =
        support.planAtomicSnapshotChanges(
            "acct-1",
            "tx-1",
            tableId,
            Table.newBuilder().setResourceId(tableId).build(),
            null,
            "s3://floecat/iceberg/trino_fmt_v1_smoke/metadata/00002-materialized.metadata.json",
            updates,
            List.of());

    assertNull(result.error());
    assertEquals(2, result.txChanges().size());
    Snapshot snapshot = Snapshot.parseFrom(result.txChanges().getFirst().getPayload());
    assertEquals(
        "s3://floecat/iceberg/trino_fmt_v1_smoke/metadata/00002-materialized.metadata.json",
        snapshot.getMetadataLocation());
    assertTrue(snapshot.hasMetadataLocation());
  }
}
