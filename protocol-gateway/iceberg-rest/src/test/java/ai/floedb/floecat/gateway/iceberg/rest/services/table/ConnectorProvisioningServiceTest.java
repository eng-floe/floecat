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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConnectorProvisioningServiceTest {

  @Test
  void resolveOrCreateForCommitUsesTableScopedFileIoDefaultsForConnectorProperties() {
    ConnectorProvisioningService service = new ConnectorProvisioningService();
    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
    Table table =
        Table.newBuilder()
            .putProperties("location", "s3://floecat/iceberg/duckdb_mutation_smoke")
            .build();
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();

    when(tableSupport.connectorIntegrationEnabled()).thenReturn(true);
    when(tableSupport.resolveTableLocation("s3://floecat/iceberg/duckdb_mutation_smoke", null))
        .thenReturn("s3://floecat/iceberg/duckdb_mutation_smoke");
    when(tableSupport.defaultFileIoProperties(table))
        .thenReturn(
            Map.of(
                "s3.endpoint", "http://localstack:4566",
                "s3.path-style-access", "true",
                "client.region", "us-east-1"));

    ConnectorProvisioningService.ProvisionResult result =
        service.resolveOrCreateForCommit(
            "examples", tableSupport, List.of("iceberg"), "duckdb_mutation_smoke", tableId, table);

    assertNull(result.error());
    assertFalse(result.connectorTxChanges().isEmpty());
    Map<String, String> props =
        result.connectorTxChanges().getFirst().getConnectorProvisioning().getPropertiesMap();
    assertEquals("http://localstack:4566", props.get("s3.endpoint"));
    assertEquals("true", props.get("s3.path-style-access"));
    assertEquals("us-east-1", props.get("client.region"));
  }
}
