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

package ai.floedb.floecat.service.repo.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The catalog child fence makes namespace publication and catalog deletion mutually exclusive. */
class CatalogChildFenceTest {
  private static final String ACCOUNT = "acct-1";
  private static final ResourceId CATALOG_ID =
      ResourceId.newBuilder()
          .setAccountId(ACCOUNT)
          .setId("cat-1")
          .setKind(ResourceKind.RK_CATALOG)
          .build();
  private static final ResourceId NAMESPACE_ID =
      ResourceId.newBuilder()
          .setAccountId(ACCOUNT)
          .setId("ns-1")
          .setKind(ResourceKind.RK_NAMESPACE)
          .build();

  private MarkerStore markers;
  private CatalogRepository catalogs;
  private NamespaceRepository namespaces;

  @BeforeEach
  void setUp() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    markers = new MarkerStore();
    markers.pointerStore = pointers;
    catalogs = new CatalogRepository(pointers, blobs);
    namespaces = new NamespaceRepository(pointers, blobs);
    catalogs.create(
        Catalog.newBuilder().setResourceId(CATALOG_ID).setDisplayName("warehouse").build());
  }

  private static Namespace rootNamespace() {
    return Namespace.newBuilder()
        .setResourceId(NAMESPACE_ID)
        .setCatalogId(CATALOG_ID)
        .setDisplayName("sales")
        .build();
  }

  @Test
  void rootNamespacePublishAdvancesTheCatalogMarkerInsideItsBatch() {
    long before = markers.catalogMarkerVersion(CATALOG_ID);

    namespaces.create(rootNamespace(), markers.catalogChildGuard(CATALOG_ID));

    assertThat(namespaces.getById(NAMESPACE_ID)).isPresent();
    assertThat(markers.catalogMarkerVersion(CATALOG_ID)).isEqualTo(before + 1L);
  }

  @Test
  void catalogDeleteFailsWhenARootNamespaceWasPublishedAfterItsScan() {
    long scanned = markers.catalogMarkerVersion(CATALOG_ID);
    long catalogVersion = catalogs.metaFor(CATALOG_ID).getPointerVersion();
    var deleteGuard = markers.catalogChildrenUnchangedGuard(CATALOG_ID, scanned);

    namespaces.create(rootNamespace(), markers.catalogChildGuard(CATALOG_ID));

    assertThatThrownBy(
            () -> catalogs.deleteWithPrecondition(CATALOG_ID, catalogVersion, deleteGuard))
        .isInstanceOf(BaseResourceRepository.BatchGuardFailedException.class)
        .hasMessageContaining(CATALOG_ID.getId());
    assertThat(catalogs.getById(CATALOG_ID)).isPresent();
    assertThat(namespaces.getById(NAMESPACE_ID)).isPresent();
  }

  @Test
  void rootNamespacePublishFailsWhenTheCatalogDeleteCommittedFirst() {
    var publishGuard = markers.catalogChildGuard(CATALOG_ID);
    long scanned = markers.catalogMarkerVersion(CATALOG_ID);
    long catalogVersion = catalogs.metaFor(CATALOG_ID).getPointerVersion();

    assertThat(
            catalogs.deleteWithPrecondition(
                CATALOG_ID,
                catalogVersion,
                markers.catalogChildrenUnchangedGuard(CATALOG_ID, scanned)))
        .isTrue();

    assertThatThrownBy(() -> namespaces.create(rootNamespace(), publishGuard))
        .isInstanceOf(BaseResourceRepository.BatchGuardFailedException.class)
        .hasMessageContaining(CATALOG_ID.getId());
    assertThat(namespaces.getById(NAMESPACE_ID)).isEmpty();
  }
}
