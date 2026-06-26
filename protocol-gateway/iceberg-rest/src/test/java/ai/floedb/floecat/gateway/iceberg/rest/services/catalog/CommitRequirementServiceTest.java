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
package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CommitRequirementServiceTest {

  private final CommitRequirementService service = new CommitRequirementService();

  @Test
  void assertMainRefSnapshotIdUsesCurrentSnapshotPointer() {
    var table =
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
    when(tableSupport.loadCurrentSnapshotId(table)).thenReturn(20L);

    Response error =
        service.validateRequirements(
            tableSupport,
            List.of(Map.of("type", "assert-ref-snapshot-id", "ref", "main", "snapshot-id", 10)),
            () -> table,
            CommitRequirementServiceTest::validation,
            CommitRequirementServiceTest::conflict);

    assertNotNull(error);
    assertEquals(Response.Status.CONFLICT.getStatusCode(), error.getStatus());
  }

  @Test
  void assertMainRefSnapshotIdIgnoresStaleTablePropertyWhenPointerMatches() {
    var table =
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
    when(tableSupport.loadCurrentSnapshotId(table)).thenReturn(20L);

    Response error =
        service.validateRequirements(
            tableSupport,
            List.of(Map.of("type", "assert-ref-snapshot-id", "ref", "main", "snapshot-id", 20)),
            () -> table,
            CommitRequirementServiceTest::validation,
            CommitRequirementServiceTest::conflict);

    assertNull(error);
  }

  private static Response validation(String message) {
    return Response.status(Response.Status.BAD_REQUEST).entity(message).build();
  }

  private static Response conflict(String message) {
    return Response.status(Response.Status.CONFLICT).entity(message).build();
  }
}
