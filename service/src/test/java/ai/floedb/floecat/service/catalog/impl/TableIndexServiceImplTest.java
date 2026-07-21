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
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.PutIndexArtifactsRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.IndexArtifactRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.service.testsupport.TestNodes;
import ai.floedb.floecat.service.testsupport.TestPrincipals;
import ai.floedb.floecat.storage.spi.BlobStore;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Multi;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class TableIndexServiceImplTest {

  @Test
  void putIndexArtifactsRejectsSystemTableBeforePersistence() {
    var svc = new TableIndexServiceImpl();
    svc.snapshots = mock(SnapshotRepository.class);
    svc.indexArtifacts = mock(IndexArtifactRepository.class);
    svc.blobStore = mock(BlobStore.class);
    svc.principal = mock(PrincipalProvider.class);
    svc.authz = mock(Authorizer.class);
    svc.idempotencyStore = mock(IdempotencyRepository.class);
    svc.overlay = mock(CatalogOverlay.class);

    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("sys_index_table")
            .build();

    when(svc.overlay.resolve(tableId)).thenReturn(Optional.of(TestNodes.systemTableNode(tableId)));
    var pc = TestPrincipals.stubPrincipal(svc.principal, svc.authz);

    var request =
        PutIndexArtifactsRequest.newBuilder().setTableId(tableId).setSnapshotId(123L).build();

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> svc.putIndexArtifacts(Multi.createFrom().item(request)).await().indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    verifyNoInteractions(svc.snapshots, svc.indexArtifacts, svc.blobStore, svc.idempotencyStore);
  }
}
