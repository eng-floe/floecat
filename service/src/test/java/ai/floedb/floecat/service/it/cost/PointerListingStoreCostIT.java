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
import ai.floedb.floecat.catalog.rpc.ListRelationsRequest;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.RelationReference;
import ai.floedb.floecat.catalog.rpc.RelationServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ResolveRelationsRequest;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.it.profiles.StoreCostProfile;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.testsupport.RecordingStoreReadObserver;
import ai.floedb.floecat.service.testsupport.StoreCostMeter;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Store-cost gates for public RPCs that should enumerate the complete pointer index. */
@QuarkusTest
@TestProfile(PointerListingStoreCostIT.Profile.class)
class PointerListingStoreCostIT {

  /** Keeps object-cache hits from hiding blob reads in these pointer-only cost assertions. */
  public static final class Profile extends StoreCostProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> overrides = new HashMap<>(super.getConfigOverrides());
      overrides.put("floecat.cache.blob.disk.enabled", "false");
      return overrides;
    }
  }

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalogs;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub tables;

  @GrpcClient("floecat")
  ViewServiceGrpc.ViewServiceBlockingStub views;

  @GrpcClient("floecat")
  RelationServiceGrpc.RelationServiceBlockingStub relation;

  @Inject TestDataResetter resetter;
  @Inject AccountRepository accounts;
  @Inject SeedRunner seeder;
  @Inject RecordingStoreReadObserver reads;
  @Inject StoreCostMeter meter;

  @BeforeEach
  void setUp() {
    meter.resetBetweenTests();
    resetter.wipeAll();
    seeder.seedData();
    resetter.warmPointerIndexAndWait(
        accounts.getByName(TestSupport.DEFAULT_SEED_ACCOUNT).orElseThrow().getResourceId().getId());
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
        reads.plannerPointerRoundTrips(),
        "every page token produced by the cached listing must resume in the complete index");
    assertEquals(
        4,
        reads.accountDirectoryRoundTrips(),
        "each page RPC pays only its fixed account-directory lookup");
    assertEquals(
        0,
        reads.pointerPrefixWalks(),
        "a warm multi-page namespace listing must not scan or count in the pointer store");
  }

  @Test
  void pagedDirectoryTableListingUsesOnlyPointerMetadata() {
    ResourceId catalogId =
        TestSupport.createCatalog(catalogs, "pointer_table_listing", "").getResourceId();
    var namespace = TestSupport.createNamespace(namespaces, catalogId, "sales", List.of(), "");
    for (String name : List.of("alpha", "bravo", "charlie")) {
      TestSupport.createTable(
          tables, catalogId, namespace.getResourceId(), name, "s3://bucket/" + name, "{}", "");
    }

    meter.assertWiredAndLive();
    assertEquals(List.of("alpha", "bravo", "charlie"), listTables(namespace.getResourceId()));

    meter.measure(
        () ->
            assertEquals(
                List.of("alpha", "bravo", "charlie"), listTables(namespace.getResourceId())));

    System.out.println(meter.report("paged RelationService ListRelations"));
    assertEquals(
        0, reads.plannerPointerRoundTrips(), "a warm Directory listing must remain in memory");
    assertEquals(
        3,
        reads.accountDirectoryRoundTrips(),
        "each page RPC pays only its fixed account-directory lookup");
    assertEquals(
        3,
        reads.blobObjectGets(),
        "each of the three RPC pages owes only its fixed account read, not metadata blobs");
  }

  @Test
  void directoryViewListResolutionUsesOnlyPointerMetadata() {
    ResourceId catalogId =
        TestSupport.createCatalog(catalogs, "pointer_view_listing", "").getResourceId();
    var namespace = TestSupport.createNamespace(namespaces, catalogId, "sales", List.of(), "");
    for (String name : List.of("alpha", "bravo")) {
      TestSupport.createView(views, catalogId, namespace.getResourceId(), name, "select 1", "");
    }

    var names =
        ResolveRelationsRequest.newBuilder()
            .addReferences(
                RelationReference.newBuilder()
                    .addCandidates(
                        NameRef.newBuilder()
                            .setCatalog("pointer_view_listing")
                            .addPath("sales")
                            .setName("alpha")))
            .addReferences(
                RelationReference.newBuilder()
                    .addCandidates(
                        NameRef.newBuilder()
                            .setCatalog("pointer_view_listing")
                            .addPath("sales")
                            .setName("bravo")))
            .build();
    meter.assertWiredAndLive();
    assertEquals(2, relation.resolveRelations(names).getResultsCount());

    meter.measure(() -> assertEquals(2, relation.resolveRelations(names).getResultsCount()));

    System.out.println(meter.report("RelationService ResolveRelations list"));
    assertEquals(
        0, reads.plannerPointerRoundTrips(), "a warm Directory resolve must remain in memory");
    assertEquals(
        1,
        reads.accountDirectoryRoundTrips(),
        "the RPC pays only its fixed account-directory lookup");
    assertEquals(
        1,
        reads.blobObjectGets(),
        "one RPC owes only its fixed account read, not catalog, namespace, or view blobs");
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

  private List<String> listTables(ResourceId namespaceId) {
    List<String> names = new ArrayList<>();
    String token = "";
    do {
      var response =
          relation.listRelations(
              ListRelationsRequest.newBuilder()
                  .setNamespaceId(namespaceId)
                  .addKinds(ResourceKind.RK_TABLE)
                  .setPage(PageRequest.newBuilder().setPageSize(1).setPageToken(token).build())
                  .build());
      response.getResultsList().stream()
          .filter(result -> result.hasRelation())
          .map(result -> result.getRelation().getDisplayName())
          .forEach(names::add);
      token = response.getPage().getNextPageToken();
    } while (!token.isBlank());
    return names;
  }
}
