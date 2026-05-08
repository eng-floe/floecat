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

import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.CatalogSpec;
import ai.floedb.floecat.catalog.rpc.CreateCatalogRequest;
import ai.floedb.floecat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.CreateSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.CreateTableRequest;
import ai.floedb.floecat.catalog.rpc.CreateViewRequest;
import ai.floedb.floecat.catalog.rpc.GetCatalogRequest;
import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceSpec;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.SnapshotSpec;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.catalog.rpc.ViewSqlDefinition;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import com.google.protobuf.FieldMask;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class PersistedSecretPropertiesIT {
  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  ViewServiceGrpc.ViewServiceBlockingStub view;

  @GrpcClient("floecat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void createCatalogRejectsSecretLookingProperties() throws Exception {
    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                catalog.createCatalog(
                    CreateCatalogRequest.newBuilder()
                        .setSpec(
                            CatalogSpec.newBuilder()
                                .setDisplayName("secret-cat")
                                .putProperties("access_key", "nope"))
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.INVALID_ARGUMENT, ErrorCode.MC_INVALID_ARGUMENT, null);
  }

  @Test
  void createCatalogAllowsNonSecretMetadataKeysAndPersistsProperties() {
    var created =
        catalog.createCatalog(
            CreateCatalogRequest.newBuilder()
                .setSpec(
                    CatalogSpec.newBuilder()
                        .setDisplayName("metadata-cat")
                        .putProperties("primary_key", "id")
                        .putProperties("oauth2-server-uri", "https://issuer.example/token"))
                .build());

    var fetched =
        catalog.getCatalog(
            GetCatalogRequest.newBuilder()
                .setCatalogId(created.getCatalog().getResourceId())
                .build());

    assertEquals("id", fetched.getCatalog().getPropertiesMap().get("primary_key"));
    assertEquals(
        "https://issuer.example/token",
        fetched.getCatalog().getPropertiesMap().get("oauth2-server-uri"));
  }

  @Test
  void createNamespacePersistsProperties() {
    var cat = TestSupport.createCatalog(catalog, "namespace-props-cat", "");
    var created =
        namespace.createNamespace(
            CreateNamespaceRequest.newBuilder()
                .setSpec(
                    NamespaceSpec.newBuilder()
                        .setCatalogId(cat.getResourceId())
                        .setDisplayName("ns")
                        .addAllPath(List.of("db"))
                        .putProperties("primary_key", "id")
                        .putProperties("oauth2-server-uri", "https://issuer.example/token"))
                .build());

    var fetched =
        namespace.getNamespace(
            GetNamespaceRequest.newBuilder()
                .setNamespaceId(created.getNamespace().getResourceId())
                .build());

    assertEquals("id", fetched.getNamespace().getPropertiesMap().get("primary_key"));
    assertEquals(
        "https://issuer.example/token",
        fetched.getNamespace().getPropertiesMap().get("oauth2-server-uri"));
  }

  @Test
  void updateNamespaceRejectsSecretLookingProperties() throws Exception {
    var cat = TestSupport.createCatalog(catalog, "secret-ns-cat", "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "ns", List.of("db"), "desc");

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.updateNamespace(
                    UpdateNamespaceRequest.newBuilder()
                        .setNamespaceId(ns.getResourceId())
                        .setSpec(NamespaceSpec.newBuilder().putProperties("session_token", "nope"))
                        .setUpdateMask(FieldMask.newBuilder().addPaths("properties").build())
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.INVALID_ARGUMENT, ErrorCode.MC_INVALID_ARGUMENT, null);
  }

  @Test
  void createTableRejectsSecretLookingProperties() throws Exception {
    var cat = TestSupport.createCatalog(catalog, "secret-table-cat", "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "ns", List.of("db"), "desc");

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                table.createTable(
                    CreateTableRequest.newBuilder()
                        .setSpec(
                            TableSpec.newBuilder()
                                .setCatalogId(cat.getResourceId())
                                .setNamespaceId(ns.getResourceId())
                                .setDisplayName("orders")
                                .setSchemaJson("{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}")
                                .setUpstream(UpstreamRef.newBuilder().build())
                                .putProperties("authorization", "nope"))
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.INVALID_ARGUMENT, ErrorCode.MC_INVALID_ARGUMENT, null);
  }

  @Test
  void createViewRejectsSecretLookingProperties() throws Exception {
    var cat = TestSupport.createCatalog(catalog, "secret-view-cat", "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "ns", List.of("db"), "desc");

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                view.createView(
                    CreateViewRequest.newBuilder()
                        .setSpec(
                            ViewSpec.newBuilder()
                                .setCatalogId(cat.getResourceId())
                                .setNamespaceId(ns.getResourceId())
                                .setDisplayName("orders_view")
                                .addSqlDefinitions(
                                    ViewSqlDefinition.newBuilder().setSql("select 1").build())
                                .addOutputColumns(
                                    SchemaColumn.newBuilder()
                                        .setName("c1")
                                        .setNullable(true)
                                        .build())
                                .putProperties("private_key", "nope"))
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.INVALID_ARGUMENT, ErrorCode.MC_INVALID_ARGUMENT, null);
  }

  @Test
  void createSnapshotRejectsSecretLookingSummaryEntries() throws Exception {
    var cat = TestSupport.createCatalog(catalog, "secret-snap-cat", "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "ns", List.of("db"), "desc");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders",
            "s3://bucket/orders",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                snapshot.createSnapshot(
                    CreateSnapshotRequest.newBuilder()
                        .setSpec(
                            SnapshotSpec.newBuilder()
                                .setTableId(tbl.getResourceId())
                                .setSnapshotId(1L)
                                .setUpstreamCreatedAt(
                                    Timestamps.fromMillis(System.currentTimeMillis()))
                                .putAllSummary(Map.of("secret_key", "nope")))
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.INVALID_ARGUMENT, ErrorCode.MC_INVALID_ARGUMENT, null);
  }

  @Test
  void updateTableRejectsSecretLookingProperties() throws Exception {
    var cat = TestSupport.createCatalog(catalog, "secret-table-update-cat", "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "ns", List.of("db"), "desc");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders",
            "s3://bucket/orders",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                table.updateTable(
                    UpdateTableRequest.newBuilder()
                        .setTableId(tbl.getResourceId())
                        .setSpec(TableSpec.newBuilder().putProperties("token", "nope"))
                        .setUpdateMask(FieldMask.newBuilder().addPaths("properties").build())
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.INVALID_ARGUMENT, ErrorCode.MC_INVALID_ARGUMENT, null);
  }
}
