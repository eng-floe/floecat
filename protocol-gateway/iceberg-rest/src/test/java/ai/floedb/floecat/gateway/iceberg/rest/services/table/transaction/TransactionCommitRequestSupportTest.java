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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TransactionCommitRequestSupportTest {

  @Test
  void nullMainSnapshotRefRequirementUsesPointerBackedRefExistence() {
    Table table =
        Table.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setKind(ResourceKind.RK_TABLE)
                    .setId("tbl")
                    .build())
            .putProperties("current-snapshot-id", "10")
            .build();
    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
    when(tableSupport.hasSnapshotRef(table, "main")).thenReturn(false);

    Response response =
        TransactionCommitRequestSupport.validateNullSnapshotRefRequirements(
            tableSupport, table, List.of(nullMainRefRequirement()));

    assertNull(response);
  }

  @Test
  void nullMainSnapshotRefRequirementConflictsWhenPointerBackedRefExists() {
    Table table =
        Table.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setKind(ResourceKind.RK_TABLE)
                    .setId("tbl")
                    .build())
            .build();
    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
    when(tableSupport.hasSnapshotRef(table, "main")).thenReturn(true);

    Response response =
        TransactionCommitRequestSupport.validateNullSnapshotRefRequirements(
            tableSupport, table, List.of(nullMainRefRequirement()));

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
  }

  private static Map<String, Object> nullMainRefRequirement() {
    Map<String, Object> requirement = new LinkedHashMap<>();
    requirement.put("type", "assert-ref-snapshot-id");
    requirement.put("ref", "main");
    requirement.put("snapshot-id", null);
    return requirement;
  }
}
