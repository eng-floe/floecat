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

package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.DeleteCatalogRequest;
import ai.floedb.floecat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.storage.spi.PointerStore;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Deleting a catalog that still holds a namespace.
 *
 * <p>A namespace's by-path pointer embeds the catalog id, so a catalog deleted from under one
 * leaves it addressable under a catalog that does not exist. A catalog's own name is derived from
 * its own field and its children reference it by id, so rename and move are safe -- only delete can
 * orphan, which is why this guards one path rather than four.
 */
@QuarkusTest
class CatalogDeleteGuardIT {

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @Inject PointerStore ptr;
  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  private final String prefix = getClass().getSimpleName() + "_";

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void deletingACatalogThatStillHoldsANamespaceIsRefused() {
    var cat = TestSupport.createCatalog(catalog, prefix + "cat_full", "");
    TestSupport.createNamespace(namespace, cat.getResourceId(), "resident", List.of(), "ns");

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                catalog.deleteCatalog(
                    DeleteCatalogRequest.newBuilder().setCatalogId(cat.getResourceId()).build()));
    assertEquals(Status.Code.ABORTED, ex.getStatus().getCode());
    assertTrue(
        ptr.get(
                Keys.catalogPointerById(
                    cat.getResourceId().getAccountId(), cat.getResourceId().getId()))
            .isPresent(),
        "the catalog must still be there");
  }

  @Test
  void deletingAnEmptyCatalogIsAllowedAndTakesItsChildSetMarkersWithIt() {
    var cat = TestSupport.createCatalog(catalog, prefix + "cat_empty", "");
    String account = cat.getResourceId().getAccountId();
    String children = Keys.catalogChildrenMarker(account, cat.getResourceId().getId());

    // The marker has to exist before the delete, or "gone afterwards" is true either way and this
    // asserts nothing. A catalog that never held a namespace never had the marker written.
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "brief", List.of(), "ns");
    assertTrue(version(children) > 0, "creating a namespace wrote the marker");
    namespace.deleteNamespace(
        DeleteNamespaceRequest.newBuilder().setNamespaceId(ns.getResourceId()).build());
    assertTrue(version(children) > 0, "and deleting it left the catalog's marker in place");

    catalog.deleteCatalog(
        DeleteCatalogRequest.newBuilder().setCatalogId(cat.getResourceId()).build());

    assertTrue(
        ptr.get(Keys.catalogPointerById(account, cat.getResourceId().getId())).isEmpty(),
        "the catalog is gone");
    assertTrue(
        ptr.get(children).isEmpty(),
        "its child-set marker goes with it -- advancing would leave a row counting nothing");
  }

  /**
   * Creating a namespace must advance its catalog's child marker, or a catalog delete cannot
   * exclude it. Asserted as participation because that is the deterministic half: exclusion follows
   * from participation plus the CAS, and two RPCs released from one latch never overlap reliably.
   */
  @Test
  void everyNamespaceCreateAdvancesItsCatalogsChildMarker() {
    var cat = TestSupport.createCatalog(catalog, prefix + "cat_marker", "");
    String account = cat.getResourceId().getAccountId();
    String children = Keys.catalogChildrenMarker(account, cat.getResourceId().getId());

    long before = version(children);
    TestSupport.createNamespace(namespace, cat.getResourceId(), "top", List.of(), "top level");
    assertTrue(
        version(children) > before,
        "a top-level namespace has no parent namespace, so the catalog marker is the only thing a"
            + " catalog delete could lose to");

    long afterTop = version(children);
    TestSupport.createNamespace(namespace, cat.getResourceId(), "nested", List.of("top"), "nested");
    assertTrue(
        version(children) > afterTop,
        "a nested namespace is still a namespace in this catalog, so it advances the marker too");
  }

  private long version(String pointerKey) {
    return ptr.get(pointerKey).map(p -> p.getVersion()).orElse(0L);
  }
}
