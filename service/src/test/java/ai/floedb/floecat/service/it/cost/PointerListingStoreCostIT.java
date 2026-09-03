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

package ai.floedb.floecat.service.it.cost;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.it.profiles.StoreCostProfile;
import ai.floedb.floecat.service.testsupport.RecordingStoreReadObserver;
import ai.floedb.floecat.service.testsupport.StoreCostMeter;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Store-cost gates for public RPCs that should enumerate the complete pointer index. */
@QuarkusTest
@TestProfile(StoreCostProfile.class)
class PointerListingStoreCostIT {

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalogs;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;
  @Inject RecordingStoreReadObserver reads;
  @Inject StoreCostMeter meter;

  @BeforeEach
  void setUp() {
    meter.resetBetweenTests();
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void warmNamespacePaginationNeverFallsBackToKv() {
    ResourceId catalogId =
        TestSupport.createCatalog(catalogs, "pointer_listing_catalog", "").getResourceId();
    for (String name : List.of("alpha", "bravo", "charlie")) {
      TestSupport.createNamespace(namespaces, catalogId, name, List.of(), "");
    }

    meter.assertWiredAndLive();

    // Traverse once to make the account index and immutable namespace bodies warm. Page size one
    // is intentional: the regression only appears when the RPC synthesizes a continuation after
    // the last emitted namespace and then consumes it on the next request.
    assertEquals(List.of("alpha", "bravo", "charlie"), listUserNamespaces(catalogId));

    meter.measure(
        () -> assertEquals(List.of("alpha", "bravo", "charlie"), listUserNamespaces(catalogId)));

    System.out.println(meter.report("warm ListNamespaces pagination"));
    assertEquals(
        0,
        reads.pointerRoundTrips(),
        "every page token produced by the cached listing must resume in the complete index");
    assertEquals(
        0,
        reads.pointerPrefixWalks(),
        "a warm multi-page namespace listing must not scan or count in the pointer store");
  }

  private List<String> listUserNamespaces(ResourceId catalogId) {
    List<String> names = new ArrayList<>();
    String token = "";
    do {
      var response =
          namespaces.listNamespaces(
              ListNamespacesRequest.newBuilder()
                  .setCatalogId(catalogId)
                  .setChildrenOnly(true)
                  .setPage(PageRequest.newBuilder().setPageSize(1).setPageToken(token).build())
                  .build());
      response.getNamespacesList().stream()
          .map(Namespace::getDisplayName)
          .filter(name -> !"information_schema".equals(name))
          .forEach(names::add);
      token = response.getPage().getNextPageToken();
    } while (!token.isBlank());
    return names;
  }
}
