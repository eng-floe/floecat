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
package ai.floedb.floecat.service.catalog.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class CurrentSnapshotPointerServiceTest {

  @Test
  void maybeAdvanceByIdFailsWhenSnapshotIsMissing() {
    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("tbl")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    var service = new CurrentSnapshotPointerService();
    service.snapshotRepo = mock(SnapshotRepository.class);
    when(service.snapshotRepo.getById(tableId, 123L)).thenReturn(Optional.empty());

    StatusRuntimeException thrown =
        assertThrows(
            StatusRuntimeException.class, () -> service.maybeAdvance(tableId, 123L, "corr"));

    assertEquals(Status.NOT_FOUND.getCode(), thrown.getStatus().getCode());
  }
}
