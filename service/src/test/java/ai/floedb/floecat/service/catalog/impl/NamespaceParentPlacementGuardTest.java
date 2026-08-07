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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.lang.reflect.Field;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NamespaceParentPlacementGuardTest {
  private static final String ACCOUNT = "acct";
  private static final ResourceId CATALOG_ID = id("cat", ResourceKind.RK_CATALOG);
  private static final ResourceId PARENT_ID = id("parent-id", ResourceKind.RK_NAMESPACE);

  private InMemoryPointerStore pointers;
  private NamespaceRepository namespaces;
  private NamespaceServiceImpl service;

  @BeforeEach
  void setUp() throws Exception {
    pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var markers = new MarkerStore();
    Field pointerStore = MarkerStore.class.getDeclaredField("pointerStore");
    pointerStore.setAccessible(true);
    pointerStore.set(markers, pointers);
    var catalogs = new CatalogRepository(pointers, blobs);
    namespaces = new NamespaceRepository(pointers, blobs);
    catalogs.create(
        Catalog.newBuilder().setResourceId(CATALOG_ID).setDisplayName("catalog").build());
    namespaces.create(
        Namespace.newBuilder()
            .setResourceId(PARENT_ID)
            .setCatalogId(CATALOG_ID)
            .setDisplayName("parent")
            .build());

    service = new NamespaceServiceImpl();
    service.namespaceRepo = namespaces;
    service.markerStore = markers;
  }

  @Test
  void publishPinsTheExactParentPathRowThatWasResolved() {
    var guard = service.parentNamespaceGuard(ACCOUNT, CATALOG_ID, List.of("parent"));
    String parentPath = Keys.namespacePointerByPath(ACCOUNT, CATALOG_ID.getId(), List.of("parent"));
    var placement = pointers.get(parentPath).orElseThrow();
    pointers.compareAndSet(
        parentPath,
        placement.getVersion(),
        placement.toBuilder().setVersion(placement.getVersion() + 1L).build());

    var child =
        Namespace.newBuilder()
            .setResourceId(id("child-id", ResourceKind.RK_NAMESPACE))
            .setCatalogId(CATALOG_ID)
            .addParents("parent")
            .setDisplayName("child")
            .build();

    assertThatThrownBy(() -> namespaces.create(child, guard))
        .isInstanceOf(BaseResourceRepository.BatchGuardFailedException.class)
        .hasMessageContaining("parent namespace path parent");
  }

  @Test
  void parentMoveBetweenPathResolutionAndCanonicalGuardCaptureIsRefused() {
    var racingNamespaces = spy(namespaces);
    doAnswer(
            invocation -> {
              var resolved = invocation.callRealMethod();
              var parent = namespaces.getById(PARENT_ID).orElseThrow();
              long version = namespaces.metaFor(PARENT_ID).getPointerVersion();
              namespaces.update(parent.toBuilder().setDisplayName("moved").build(), version);
              return resolved;
            })
        .when(racingNamespaces)
        .placementRefByPath(ACCOUNT, CATALOG_ID.getId(), List.of("parent"));
    service.namespaceRepo = racingNamespaces;

    assertThatThrownBy(() -> service.parentNamespaceGuard(ACCOUNT, CATALOG_ID, List.of("parent")))
        .isInstanceOf(BaseResourceRepository.BatchGuardFailedException.class)
        .hasMessageContaining("changed after it was resolved");
  }

  private static ResourceId id(String id, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId(ACCOUNT).setId(id).setKind(kind).build();
  }
}
