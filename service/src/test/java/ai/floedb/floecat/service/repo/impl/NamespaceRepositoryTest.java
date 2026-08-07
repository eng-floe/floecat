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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NamespaceRepositoryTest {

  private CatalogRepository catalogRepo;
  private NamespaceRepository namespaceRepo;
  private PointerStore ptr;
  private BlobStore blobs;

  @BeforeEach
  void setUp() {
    ptr = new InMemoryPointerStore();
    blobs = new InMemoryBlobStore();
    catalogRepo = new CatalogRepository(ptr, blobs);
    namespaceRepo = new NamespaceRepository(ptr, blobs);
  }

  @Test
  void putAndGetRoundTrip() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var catRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_CATALOG)
            .build();

    Catalog cat = Catalog.newBuilder().setResourceId(catRid).setDisplayName("sales").build();
    catalogRepo.create(cat);

    var nsRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();

    var ns =
        Namespace.newBuilder()
            .setResourceId(nsRid)
            .setDisplayName("core")
            .setDescription("Core namespace")
            .setCatalogId(catRid)
            .build();
    namespaceRepo.create(ns);

    var fetched = namespaceRepo.getById(nsRid).orElseThrow();
    assertEquals("core", fetched.getDisplayName());
  }

  @Test
  void liveRefProbeDistinguishesOrdinaryChildrenFromStrandedPaths() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var catRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    catalogRepo.create(Catalog.newBuilder().setResourceId(catRid).setDisplayName("sales").build());

    var nsRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    namespaceRepo.create(
        Namespace.newBuilder()
            .setResourceId(nsRid)
            .setDisplayName("core")
            .setCatalogId(catRid)
            .build());

    assertTrue(namespaceRepo.hasLiveRefUnder(account, catRid.getId(), List.of()));

    ptr.delete(Keys.namespacePointerById(account, nsRid.getId()));

    assertFalse(namespaceRepo.hasLiveRefUnder(account, catRid.getId(), List.of()));
    assertEquals(1, namespaceRepo.count(account, catRid.getId(), List.of()));
  }

  @Test
  void descendantProbeFindsDeepRowsEvenWhenTheImmediateParentRowIsMissing() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var catRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    catalogRepo.create(Catalog.newBuilder().setResourceId(catRid).setDisplayName("sales").build());
    var damagedDescendant =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    String deepKey =
        Keys.namespacePointerByPath(
            account, catRid.getId(), List.of("source", "missing", "grandchild"));
    ptr.compareAndSet(
        deepKey,
        0L,
        Pointer.newBuilder()
            .setKey(deepKey)
            .setVersion(1L)
            .setResourceId(damagedDescendant)
            .build());

    assertFalse(namespaceRepo.hasChildUnder(account, catRid.getId(), List.of("source")));
    assertTrue(namespaceRepo.hasAnyDescendantUnder(account, catRid.getId(), List.of("source")));
  }

  @Test
  void descendantProbeContinuesPastAnEmptyPage() {
    var calls = new java.util.concurrent.atomic.AtomicInteger();
    var pointer = Pointer.newBuilder().setKey("/descendant").setVersion(1L).build();
    var paged =
        new InMemoryPointerStore() {
          @Override
          public List<Pointer> listPointersByPrefix(
              String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
            if (calls.getAndIncrement() == 0) {
              nextTokenOut.append("next");
              return List.of();
            }
            return List.of(pointer);
          }
        };
    var repository = new NamespaceRepository(paged, new InMemoryBlobStore());

    assertTrue(repository.hasAnyDescendantUnder("acct", "cat", List.of("parent")));
    assertEquals(2, calls.get());
  }
}
