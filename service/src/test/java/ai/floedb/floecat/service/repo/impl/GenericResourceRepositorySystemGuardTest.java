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

package ai.floedb.floecat.service.repo.impl;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The repository-layer backstop for system-object immutability: a schema that opts in via {@code
 * ResourceSchema#withSystemGuard} (catalog/namespace/table/view) must refuse every write path whose
 * target is a system-owned id, independent of the surface {@code CatalogSurfaceWritePolicy}.
 */
class GenericResourceRepositorySystemGuardTest {

  private InMemoryPointerStore ptr;
  private InMemoryBlobStore blobs;
  private CatalogRepository repo;

  @BeforeEach
  void setUp() {
    ptr = new InMemoryPointerStore();
    blobs = new InMemoryBlobStore();
    repo = new CatalogRepository(ptr, blobs);
  }

  private static Catalog catalog(ResourceId id, String name) {
    return Catalog.newBuilder().setResourceId(id).setDisplayName(name).build();
  }

  private static ResourceId systemCatalogId() {
    return SystemNodeRegistry.systemCatalogContainerId("engine");
  }

  @Test
  void create_systemCatalog_rejected() {
    assertThatThrownBy(() -> repo.create(catalog(systemCatalogId(), "sys")))
        .isInstanceOf(BaseResourceRepository.SystemObjectImmutableException.class);
  }

  @Test
  void update_systemCatalog_rejected() {
    assertThatThrownBy(() -> repo.update(catalog(systemCatalogId(), "sys"), 1L))
        .isInstanceOf(BaseResourceRepository.SystemObjectImmutableException.class);
  }

  @Test
  void delete_systemCatalog_rejected() {
    assertThatThrownBy(() -> repo.delete(systemCatalogId()))
        .isInstanceOf(BaseResourceRepository.SystemObjectImmutableException.class);
  }

  @Test
  void deleteWithPrecondition_systemCatalog_rejected() {
    assertThatThrownBy(() -> repo.deleteWithPrecondition(systemCatalogId(), 1L))
        .isInstanceOf(BaseResourceRepository.SystemObjectImmutableException.class);
  }

  @Test
  void create_userCatalog_allowed() {
    var userId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("cat-user-1")
            .build();
    assertThatCode(() -> repo.create(catalog(userId, "user"))).doesNotThrowAnyException();
  }
}
