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

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.InMemoryBlobStore;
import ai.floedb.floecat.storage.InMemoryPointerStore;
import ai.floedb.floecat.storage.PointerStore;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalogRepositoryTest {
  private CatalogRepository catalogRepo;
  private PointerStore ptr;
  private BlobStore blobs;

  @BeforeEach
  void setUp() {
    ptr = new InMemoryPointerStore();
    blobs = new InMemoryBlobStore();
    catalogRepo = new CatalogRepository(ptr, blobs);
  }

  @Test
  void putAndGetRoundTrip() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var rid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    var cat =
        Catalog.newBuilder()
            .setResourceId(rid)
            .setDisplayName("sales")
            .setDescription("Sales")
            .build();

    catalogRepo.create(cat);
    var fetched = catalogRepo.getById(rid).orElseThrow();
    assertEquals("sales", fetched.getDisplayName());
    fetched = catalogRepo.getByName(account, "sales").orElseThrow();
    assertEquals(rid, fetched.getResourceId());

    catalogRepo.delete(rid);
    fetched = catalogRepo.getByName(account, "sales").orElse(null);
    assertNull(fetched);
  }

  @Test
  void listCatalogs() {
    var ptr = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var catalogRepo = new CatalogRepository(ptr, blobs);

    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();

    var cat1Rid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    var cat1 =
        Catalog.newBuilder()
            .setResourceId(cat1Rid)
            .setDisplayName("sales")
            .setDescription("Sales")
            .build();
    catalogRepo.create(cat1);

    var cat2Rid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    var cat2 =
        Catalog.newBuilder()
            .setResourceId(cat2Rid)
            .setDisplayName("orders")
            .setDescription("orders")
            .build();
    catalogRepo.create(cat2);

    var cat3Rid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    var cat3 =
        Catalog.newBuilder()
            .setResourceId(cat3Rid)
            .setDisplayName("lineitem")
            .setDescription("lineitem")
            .build();
    catalogRepo.create(cat3);

    List<Catalog> catalogs = catalogRepo.list(account, Integer.MAX_VALUE, null, null);
    assertEquals(3, catalogs.size());

    String token = "";
    StringBuilder next = new StringBuilder();
    List<Catalog> batch = catalogRepo.list(account, 1, token, next);
    assertEquals("lineitem", batch.get(0).getDisplayName());
    token = next.toString();
    next.setLength(0);
    batch = catalogRepo.list(account, 1, token, next);
    assertEquals("orders", batch.get(0).getDisplayName());
    token = next.toString();
    next.setLength(0);
    batch = catalogRepo.list(account, 1, token, next);
    assertEquals("sales", batch.get(0).getDisplayName());
    token = next.toString();
    next.setLength(0);
    assertTrue(token.isEmpty());
  }

  @Test
  void updateRefreshesSecondaryPointerForCas() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var rid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    var cat =
        Catalog.newBuilder()
            .setResourceId(rid)
            .setDisplayName("sales")
            .setDescription("v1")
            .build();

    catalogRepo.create(cat);
    var before = catalogRepo.getByName(account, "sales").orElseThrow();
    assertEquals("v1", before.getDescription());

    var meta = catalogRepo.metaFor(rid);
    var updated = cat.toBuilder().setDescription("v2").build();
    assertTrue(catalogRepo.update(updated, meta.getPointerVersion()));

    var after = catalogRepo.getByName(account, "sales").orElseThrow();
    assertEquals("v2", after.getDescription());
  }
}
