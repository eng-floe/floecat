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

package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

class SnapshotUpdateServiceTest {
  private SnapshotUpdateService service;
  private SnapshotClient snapshotClient;

  @BeforeEach
  void setUp() {
    service = new SnapshotUpdateService();
    snapshotClient = Mockito.mock(SnapshotClient.class);
    service.mapper = new ObjectMapper();
    service.snapshotClient = snapshotClient;
  }

  @Test
  void validateSnapshotUpdatesRejectsMissingRemoveSnapshotIds() {
    Response response =
        service.validateSnapshotUpdates(List.of(Map.of("action", "remove-snapshots")));

    assertNotNull(response);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = (IcebergErrorResponse) response.getEntity();
    assertEquals("remove-snapshots requires snapshot-ids", error.error().message());
  }

  @Test
  void validateSnapshotUpdatesAcceptsMinimalAddSnapshotPayload() {
    Map<String, Object> snapshot = new LinkedHashMap<>();
    snapshot.put("snapshot-id", 11L);
    Map<String, Object> update = new LinkedHashMap<>();
    update.put("action", "add-snapshot");
    update.put("snapshot", snapshot);

    Response response = service.validateSnapshotUpdates(List.of(update));

    assertNull(response);
  }

  @Test
  void deleteSnapshotsTreatsNotFoundAsIdempotent() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(snapshotClient.deleteSnapshot(ArgumentMatchers.any()))
        .thenThrow(new StatusRuntimeException(Status.NOT_FOUND))
        .thenReturn(null);

    service.deleteSnapshots(tableId, List.of(1L, 2L));

    verify(snapshotClient, times(2)).deleteSnapshot(ArgumentMatchers.any());
  }

  @Test
  void deleteSnapshotsPropagatesNonNotFoundFailure() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(snapshotClient.deleteSnapshot(ArgumentMatchers.any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));

    assertThrows(StatusRuntimeException.class, () -> service.deleteSnapshots(tableId, List.of(1L)));
  }
}
