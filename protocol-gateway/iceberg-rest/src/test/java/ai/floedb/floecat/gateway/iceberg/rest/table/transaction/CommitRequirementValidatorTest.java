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

package ai.floedb.floecat.gateway.iceberg.rest.table.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.table.IcebergMetadataService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CommitRequirementValidatorTest {

  @Test
  void assertRefSnapshotIdFailsWhenExpectedSnapshotIdIsSetButRefIsMissing() {
    CommitRequirementValidator validator = new CommitRequirementValidator();
    validator.icebergMetadataService = mock(IcebergMetadataService.class);
    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
    Table table = Table.newBuilder().build();
    when(validator.icebergMetadataService.resolveCurrentIcebergMetadata(table, tableSupport))
        .thenReturn(IcebergMetadata.newBuilder().build());

    Response response =
        validator.validate(
            tableSupport,
            List.of(Map.of("type", "assert-ref-snapshot-id", "ref", "branch", "snapshot-id", 7L)),
            () -> table,
            message -> Response.status(Response.Status.BAD_REQUEST).entity(message).build(),
            message -> Response.status(Response.Status.CONFLICT).entity(message).build());

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    assertEquals("assert-ref-snapshot-id failed for ref branch", response.getEntity());
  }

  @Test
  void assertRefSnapshotIdPassesOnExactMatch() {
    CommitRequirementValidator validator = new CommitRequirementValidator();
    validator.icebergMetadataService = mock(IcebergMetadataService.class);
    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
    Table table = Table.newBuilder().build();
    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .putRefs("branch", IcebergRef.newBuilder().setSnapshotId(7L).build())
            .build();
    when(validator.icebergMetadataService.resolveCurrentIcebergMetadata(table, tableSupport))
        .thenReturn(metadata);

    Response response =
        validator.validate(
            tableSupport,
            List.of(Map.of("type", "assert-ref-snapshot-id", "ref", "branch", "snapshot-id", 7L)),
            () -> table,
            message -> Response.status(Response.Status.BAD_REQUEST).entity(message).build(),
            message -> Response.status(Response.Status.CONFLICT).entity(message).build());

    assertNull(response);
  }

  @Test
  void nullSnapshotIdRequirementStillPassesWhenRefIsMissing() {
    CommitRequirementValidator validator = new CommitRequirementValidator();
    validator.icebergMetadataService = mock(IcebergMetadataService.class);
    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
    Table table = Table.newBuilder().build();
    when(validator.icebergMetadataService.resolveCurrentIcebergMetadata(table, tableSupport))
        .thenReturn(IcebergMetadata.newBuilder().build());

    Map<String, Object> requirement = new LinkedHashMap<>();
    requirement.put("type", "assert-ref-snapshot-id");
    requirement.put("ref", "branch");
    requirement.put("snapshot-id", null);

    Response response =
        validator.validateNullRefRequirements(tableSupport, table, List.of(requirement));

    assertNull(response);
  }
}
