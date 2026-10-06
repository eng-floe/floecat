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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetRelationRequest;
import ai.floedb.floecat.catalog.rpc.ListRelationsRequest;
import ai.floedb.floecat.catalog.rpc.ListTablesRequest;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.RelationReference;
import ai.floedb.floecat.catalog.rpc.RelationServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ResolveRelationsRequest;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class DirectoryIT {
  @GrpcClient("floecat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  RelationServiceGrpc.RelationServiceBlockingStub relation;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void directoryKeepsCatalogAndNamespaceIdentity() {
    Catalog cat = TestSupport.createCatalog(catalog, "directory_identity", "");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "core", null, "");

    var catalogId =
        directory
            .resolveCatalog(
                ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest.newBuilder()
                    .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName()))
                    .build())
            .getResourceId();
    assertEquals(cat.getResourceId(), catalogId);

    var namespaceRef =
        directory
            .lookupNamespace(
                ai.floedb.floecat.catalog.rpc.LookupNamespaceRequest.newBuilder()
                    .setResourceId(ns.getResourceId())
                    .build())
            .getRef();
    assertEquals(cat.getDisplayName(), namespaceRef.getCatalog());
    assertEquals("core", namespaceRef.getName());
  }

  @Test
  void relationNamesAreMatchedExactly() {
    Catalog cat = TestSupport.createCatalog(catalog, "relation_case", "");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "core", null, "");
    TestSupport.createTable(
        table, cat.getResourceId(), ns.getResourceId(), "orders", "s3://orders", "{}", "");

    // One policy: a relation is keyed by its exact stored name, so another spelling is a different
    // name and resolves to nothing. This holds for a builtin as much as for a user relation.
    NameRef shoutedUser =
        NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addPath("core")
            .setName("ORDERS")
            .build();
    NameRef shoutedBuiltin =
        NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addPath("INFORMATION_SCHEMA")
            .setName("TABLES")
            .build();
    NameRef storedBuiltin =
        NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addPath("information_schema")
            .setName("tables")
            .build();

    var response =
        relation.resolveRelations(
            ResolveRelationsRequest.newBuilder()
                .addReferences(RelationReference.newBuilder().addCandidates(shoutedUser))
                .addReferences(RelationReference.newBuilder().addCandidates(shoutedBuiltin))
                .addReferences(RelationReference.newBuilder().addCandidates(storedBuiltin))
                .build());

    assertTrue(response.getResults(0).hasError());
    assertEquals(ErrorCode.MC_NOT_FOUND, response.getResults(0).getError().getCode());

    assertTrue(response.getResults(1).hasError());
    assertEquals(ErrorCode.MC_NOT_FOUND, response.getResults(1).getError().getCode());

    // Positive control: the stored spelling resolves, so the two misses above are about case and
    // not about the builtin being absent from this catalog.
    assertTrue(response.getResults(2).hasRelation());
    assertEquals("tables", response.getResults(2).getResolvedName().getName());
  }

  @Test
  void relationServiceResolvesListsAndGetsRelations() {
    Catalog cat = TestSupport.createCatalog(catalog, "relation_surface", "");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "core", null, "");
    var created =
        TestSupport.createTable(
            table, cat.getResourceId(), ns.getResourceId(), "orders", "s3://orders", "{}", "");
    NameRef ref =
        NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addPath("core")
            .setName("orders")
            .build();

    var resolved =
        relation
            .resolveRelations(
                ResolveRelationsRequest.newBuilder()
                    .addReferences(RelationReference.newBuilder().addCandidates(ref))
                    .build())
            .getResults(0);
    assertTrue(resolved.hasRelation());
    assertEquals(created.getResourceId(), resolved.getRelation().getResourceId());
    assertEquals(ref, resolved.getRelation().getName());

    var listed =
        relation
            .listRelations(
                ListRelationsRequest.newBuilder()
                    .setNamespaceId(ns.getResourceId())
                    .addKinds(ResourceKind.RK_TABLE)
                    .setPage(PageRequest.newBuilder().setPageSize(1))
                    .build())
            .getResultsList()
            .stream()
            .filter(result -> result.hasRelation())
            .map(result -> result.getRelation())
            .toList();
    assertEquals(List.of("orders"), listed.stream().map(r -> r.getDisplayName()).toList());

    var fetched =
        relation
            .getRelation(
                GetRelationRequest.newBuilder().setRelationId(created.getResourceId()).build())
            .getRelation();
    assertEquals(created.getResourceId(), fetched.getResourceId());
  }

  @Test
  void relationResolutionReturnsPerReferenceNotFound() {
    var result =
        relation
            .resolveRelations(
                ResolveRelationsRequest.newBuilder()
                    .addReferences(
                        RelationReference.newBuilder()
                            .addCandidates(
                                NameRef.newBuilder()
                                    .setCatalog("missing")
                                    .addPath("core")
                                    .setName("orders")))
                    .build())
            .getResults(0);
    assertFalse(result.hasRelation());
    assertTrue(result.hasError());
  }

  @Test
  void typedAndGenericTableListingsShareRowsAndOrder() {
    Catalog cat = TestSupport.createCatalog(catalog, "listing_parity", "");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "core", null, "");
    for (String name : List.of("alpha", "bravo", "charlie")) {
      TestSupport.createTable(
          table, cat.getResourceId(), ns.getResourceId(), name, "s3://" + name, "{}", "");
    }

    var typed = new ArrayList<String>();
    String typedToken = "";
    do {
      var response =
          table.listTables(
              ListTablesRequest.newBuilder()
                  .setNamespaceId(ns.getResourceId())
                  .setPage(PageRequest.newBuilder().setPageSize(1).setPageToken(typedToken))
                  .build());
      typed.addAll(response.getTablesList().stream().map(t -> t.getDisplayName()).toList());
      typedToken = response.getPage().getNextPageToken();
    } while (!typedToken.isBlank());

    var generic = new ArrayList<String>();
    String genericToken = "";
    do {
      var response =
          relation.listRelations(
              ListRelationsRequest.newBuilder()
                  .setNamespaceId(ns.getResourceId())
                  .addKinds(ResourceKind.RK_TABLE)
                  .setPage(PageRequest.newBuilder().setPageSize(1).setPageToken(genericToken))
                  .build());
      generic.addAll(
          response.getResultsList().stream()
              .filter(result -> result.hasRelation())
              .map(result -> result.getRelation().getDisplayName())
              .toList());
      genericToken = response.getPage().getNextPageToken();
    } while (!genericToken.isBlank());

    assertEquals(typed, generic);
  }
}
