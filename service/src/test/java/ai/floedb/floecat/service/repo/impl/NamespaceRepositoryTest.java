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
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
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

  /**
   * A namespace whose intermediate ancestors were never materialised still counts as a descendant.
   *
   * <p>Not a contrived shape: bootstrap seeding used to write {@code iceberg/staging/2025} with no
   * {@code iceberg/staging} row, so rows like this exist in any store seeded before that was fixed.
   * An immediate-child test reads such a row as absent, which would let {@code iceberg} be renamed
   * or deleted while nothing re-derives that row's by-path key. This check does not depend on every
   * writer having materialised its ancestors, which is why it survives the data that predates it.
   */
  @Test
  void aSparseDescendantWhoseParentWasNeverMaterialisedStillBlocksItsAncestor() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var catRid = rid(account, ResourceKind.RK_CATALOG);
    catalogRepo.create(
        Catalog.newBuilder().setResourceId(catRid).setDisplayName("examples").build());

    namespaceRepo.create(namespaceAt(account, catRid, "iceberg", List.of()));
    // No iceberg/staging row, exactly as seeding leaves it.
    namespaceRepo.create(namespaceAt(account, catRid, "2025", List.of("iceberg", "staging")));

    assertTrue(
        namespaceRepo.hasDescendants(account, catRid.getId(), List.of("iceberg")),
        "a row at iceberg/staging/2025 is a descendant of iceberg even with no iceberg/staging");
    assertTrue(
        namespaceRepo.hasDescendants(account, catRid.getId(), List.of("iceberg", "staging")),
        "and a descendant of the path that has no row of its own");
  }

  /** A sibling whose name merely starts with the same text is not a descendant. */
  @Test
  void aSiblingSharingANamePrefixIsNotADescendant() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var catRid = rid(account, ResourceKind.RK_CATALOG);
    catalogRepo.create(
        Catalog.newBuilder().setResourceId(catRid).setDisplayName("examples").build());

    namespaceRepo.create(namespaceAt(account, catRid, "iceberg", List.of()));
    namespaceRepo.create(namespaceAt(account, catRid, "iceberg2", List.of()));

    assertFalse(
        namespaceRepo.hasDescendants(account, catRid.getId(), List.of("iceberg")),
        "iceberg2 is a sibling, not a child -- the prefix must stop at a separator");
  }

  private static ResourceId rid(String account, ResourceKind kind) {
    return ResourceId.newBuilder()
        .setAccountId(account)
        .setId(UUID.randomUUID().toString())
        .setKind(kind)
        .build();
  }

  private static Namespace namespaceAt(
      String account, ResourceId catalogId, String displayName, List<String> parents) {
    return Namespace.newBuilder()
        .setResourceId(rid(account, ResourceKind.RK_NAMESPACE))
        .setDisplayName(displayName)
        .addAllParents(parents)
        .setCatalogId(catalogId)
        .build();
  }
}
