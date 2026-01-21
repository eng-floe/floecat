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

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.*;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.*;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.FetchScanBundleRequest;
import ai.floedb.floecat.query.rpc.GetUserObjectsRequest;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.RelationKind;
import ai.floedb.floecat.query.rpc.RelationResolution;
import ai.floedb.floecat.query.rpc.ResolutionStatus;
import ai.floedb.floecat.query.rpc.TableReferenceCandidate;
import ai.floedb.floecat.query.rpc.UserObjectsBundleChunk;
import ai.floedb.floecat.query.rpc.UserObjectsServiceGrpc;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import com.google.protobuf.FieldMask;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@QuarkusTest
class UserObjectsServiceIT {

  @GrpcClient("floecat")
  QueryServiceGrpc.QueryServiceBlockingStub lifecycle;

  @GrpcClient("floecat")
  Channel channel;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalogService;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @GrpcClient("floecat")
  ConnectorsGrpc.ConnectorsBlockingStub connectors;

  @GrpcClient("floecat")
  ViewServiceGrpc.ViewServiceBlockingStub view;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  private final String catalogPrefix = this.getClass().getSimpleName() + "_";

  @BeforeEach
  void reset() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void GetUserObjectsPinsTable() {
    var catName = catalogPrefix + "table_cat";
    var cat = TestSupport.createCatalog(catalogService, catName, "");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "table_ns", List.of("table"), "table namespace");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "table_orders",
            "s3://bucket/table_orders",
            "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"int\",\"required\":true}]}",
            "table for catalog bundle test");

    TestSupport.createSnapshot(
        snapshot, tbl.getResourceId(), 1001L, System.currentTimeMillis() - 1000L);

    var connector = createDummyConnector(cat.getResourceId(), ns.getResourceId(), "table");
    attachConnectorToTable(tbl.getResourceId(), connector);

    var begin =
        lifecycle.beginQuery(
            BeginQueryRequest.newBuilder().setDefaultCatalogId(cat.getResourceId()).build());
    var queryId = begin.getQuery().getQueryId();

    var candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(
                QueryInput.newBuilder()
                    .setName(TestSupport.fq(catName, ns, "table_orders"))
                    .build())
            .build();

    List<UserObjectsBundleChunk> chunks =
        collectUserObjectBundle(
                GetUserObjectsRequest.newBuilder().setQueryId(queryId).addTables(candidate).build())
            .toCompletableFuture()
            .join();

    assertEquals(3, chunks.size());
    UserObjectsBundleChunk header = chunks.get(0);
    assertTrue(header.hasHeader());

    UserObjectsBundleChunk resolutions = chunks.get(1);
    assertTrue(resolutions.hasResolutions());
    assertEquals(1, resolutions.getResolutions().getItemsCount());
    RelationResolution resolution = resolutions.getResolutions().getItems(0);
    assertEquals(ResolutionStatus.RESOLUTION_STATUS_FOUND, resolution.getStatus());
    RelationInfo relation = resolution.getRelation();
    assertEquals(RelationKind.RELATION_KIND_TABLE, relation.getKind());
    assertEquals(tbl.getResourceId(), relation.getRelationId());
    assertEquals(1, relation.getColumnsCount());
    assertEquals("id", relation.getColumns(0).getName());

    UserObjectsBundleChunk end = chunks.get(2);
    assertTrue(end.hasEnd());
    assertEquals(1, end.getEnd().getResolutionCount());
    assertEquals(1, end.getEnd().getFoundCount());
    assertEquals(0, end.getEnd().getNotFoundCount());

    var files =
        fetchScanBundle(
                FetchScanBundleRequest.newBuilder()
                    .setQueryId(queryId)
                    .setTableId(tbl.getResourceId())
                    .build())
            .toCompletableFuture()
            .join();
    assertTrue(files.isEmpty());
  }

  @Test
  void GetUserObjectsRelationNotFound() {
    var catName = catalogPrefix + "missing_cat";
    var cat = TestSupport.createCatalog(catalogService, catName, "");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "table_ns", List.of("table"), "table namespace");

    var begin =
        lifecycle.beginQuery(
            BeginQueryRequest.newBuilder().setDefaultCatalogId(cat.getResourceId()).build());
    var queryId = begin.getQuery().getQueryId();

    ResourceId missingTableId =
        ResourceId.newBuilder()
            .setAccountId(cat.getResourceId().getAccountId())
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();

    var candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(missingTableId).build())
            .build();

    List<UserObjectsBundleChunk> chunks =
        collectUserObjectBundle(
                GetUserObjectsRequest.newBuilder().setQueryId(queryId).addTables(candidate).build())
            .toCompletableFuture()
            .join();

    assertEquals(3, chunks.size());
    UserObjectsBundleChunk resolutions = chunks.get(1);
    assertTrue(resolutions.hasResolutions());
    assertEquals(1, resolutions.getResolutions().getItemsCount());
    RelationResolution resolution = resolutions.getResolutions().getItems(0);
    assertEquals(ResolutionStatus.RESOLUTION_STATUS_NOT_FOUND, resolution.getStatus());
    assertTrue(resolution.hasFailure());
    assertEquals("catalog_bundle.relation_not_found", resolution.getFailure().getCode());
    assertEquals(1, resolution.getFailure().getAttemptedList().size());
    assertEquals(
        missingTableId.getId(), resolution.getFailure().getAttempted(0).getTableId().getId());

    UserObjectsBundleChunk end = chunks.get(2);
    assertTrue(end.hasEnd());
    assertEquals(1, end.getEnd().getResolutionCount());
    assertEquals(0, end.getEnd().getFoundCount());
    assertEquals(1, end.getEnd().getNotFoundCount());
  }

  @Test
  void GetUserObjectsRelationNotFoundByName() {
    var catName = catalogPrefix + "missing_name_cat";
    var cat = TestSupport.createCatalog(catalogService, catName, "");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "table_ns", List.of("table"), "table namespace");

    var begin =
        lifecycle.beginQuery(
            BeginQueryRequest.newBuilder().setDefaultCatalogId(cat.getResourceId()).build());
    var queryId = begin.getQuery().getQueryId();

    NameRef missingName = TestSupport.fq(catName, ns, "does_not_exist");
    var candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setName(missingName).build())
            .build();

    List<UserObjectsBundleChunk> chunks =
        collectUserObjectBundle(
                GetUserObjectsRequest.newBuilder().setQueryId(queryId).addTables(candidate).build())
            .toCompletableFuture()
            .join();

    assertEquals(3, chunks.size());
    UserObjectsBundleChunk resolutions = chunks.get(1);
    assertTrue(resolutions.hasResolutions());
    assertEquals(1, resolutions.getResolutions().getItemsCount());
    RelationResolution resolution = resolutions.getResolutions().getItems(0);
    assertEquals(ResolutionStatus.RESOLUTION_STATUS_NOT_FOUND, resolution.getStatus());
    assertTrue(resolution.hasFailure());
    assertEquals("catalog_bundle.relation_not_found", resolution.getFailure().getCode());
    assertEquals(1, resolution.getFailure().getAttemptedList().size());
    assertEquals(missingName, resolution.getFailure().getAttempted(0).getName());

    UserObjectsBundleChunk end = chunks.get(2);
    assertTrue(end.hasEnd());
    assertEquals(1, end.getEnd().getResolutionCount());
    assertEquals(0, end.getEnd().getFoundCount());
    assertEquals(1, end.getEnd().getNotFoundCount());
  }

  // Disabled until views can populate the base table dependency
  // list.
  // TODO: re-enable when view support is ready
  @Disabled
  @Test
  void GetUserObjectsStreamsViewDefinition() {
    var catName = catalogPrefix + "view_cat";
    var cat = TestSupport.createCatalog(catalogService, catName, "");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "view_ns", List.of("view"), "view namespace");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "view_orders",
            "s3://bucket/view_orders",
            "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"int\",\"required\":true}]}",
            "view backing table");
    TestSupport.createSnapshot(
        snapshot, tbl.getResourceId(), 2002L, System.currentTimeMillis() - 5000L);
    var connector = createDummyConnector(cat.getResourceId(), ns.getResourceId(), "view");
    attachConnectorToTable(tbl.getResourceId(), connector);

    var viewName = "summary";
    var sql =
        String.format(
            "select id from %s.%s.%s", catName, ns.getDisplayName(), tbl.getDisplayName());
    var createdView =
        TestSupport.createView(
            view,
            cat.getResourceId(),
            ns.getResourceId(),
            viewName,
            sql,
            "view for catalog bundle test");

    var begin =
        lifecycle.beginQuery(
            BeginQueryRequest.newBuilder().setDefaultCatalogId(cat.getResourceId()).build());
    var queryId = begin.getQuery().getQueryId();

    var candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(
                QueryInput.newBuilder().setName(TestSupport.fq(catName, ns, viewName)).build())
            .build();

    List<UserObjectsBundleChunk> chunks =
        collectUserObjectBundle(
                GetUserObjectsRequest.newBuilder().setQueryId(queryId).addTables(candidate).build())
            .toCompletableFuture()
            .join();

    assertEquals(3, chunks.size());
    UserObjectsBundleChunk resolutions = chunks.get(1);
    RelationResolution resolution = resolutions.getResolutions().getItems(0);
    assertEquals(ResolutionStatus.RESOLUTION_STATUS_FOUND, resolution.getStatus());
    RelationInfo relation = resolution.getRelation();
    assertEquals(RelationKind.RELATION_KIND_VIEW, relation.getKind());
    assertEquals(createdView.getResourceId(), relation.getRelationId());
    assertTrue(relation.hasViewDefinition());
    assertEquals(sql, relation.getViewDefinition().getCanonicalSql());

    UserObjectsBundleChunk end = chunks.get(2);
    assertEquals(1, end.getEnd().getResolutionCount());
    assertEquals(1, end.getEnd().getFoundCount());
  }

  private CompletionStage<List<UserObjectsBundleChunk>> collectUserObjectBundle(
      GetUserObjectsRequest request) {
    UserObjectsServiceGrpc.UserObjectsServiceStub async =
        UserObjectsServiceGrpc.newStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
    CompletableFuture<List<UserObjectsBundleChunk>> future = new CompletableFuture<>();
    List<UserObjectsBundleChunk> chunks = Collections.synchronizedList(new ArrayList<>());

    async.getUserObjects(
        request,
        new StreamObserver<UserObjectsBundleChunk>() {
          @Override
          public void onNext(UserObjectsBundleChunk chunk) {
            chunks.add(chunk);
          }

          @Override
          public void onError(Throwable t) {
            future.completeExceptionally(t);
          }

          @Override
          public void onCompleted() {
            future.complete(new ArrayList<>(chunks));
          }
        });

    return future.orTimeout(5, TimeUnit.SECONDS);
  }

  private CompletionStage<List<ScanFile>> fetchScanBundle(FetchScanBundleRequest request) {
    CompletableFuture<List<ScanFile>> future = new CompletableFuture<>();
    List<ScanFile> files = Collections.synchronizedList(new ArrayList<>());
    QueryScanServiceGrpc.QueryScanServiceStub async =
        QueryScanServiceGrpc.newStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
    async.fetchScanBundle(
        request,
        new StreamObserver<>() {
          @Override
          public void onNext(ScanFile response) {
            files.add(response);
          }

          @Override
          public void onError(Throwable t) {
            future.completeExceptionally(t);
          }

          @Override
          public void onCompleted() {
            future.complete(new ArrayList<>(files));
          }
        });
    return future.orTimeout(5, TimeUnit.SECONDS);
  }

  private Connector createDummyConnector(
      ResourceId catalogId, ResourceId namespaceId, String suffix) {
    SourceSelector source =
        SourceSelector.newBuilder()
            .setNamespace(NamespacePath.newBuilder().addSegments("examples").addSegments("iceberg"))
            .build();

    DestinationTarget destination =
        DestinationTarget.newBuilder().setCatalogId(catalogId).setNamespaceId(namespaceId).build();

    ConnectorSpec spec =
        ConnectorSpec.newBuilder()
            .setDisplayName("qc-" + suffix)
            .setKind(ConnectorKind.CK_UNITY)
            .setUri("dummy://ignored")
            .setSource(source)
            .setDestination(destination)
            .setAuth(AuthConfig.newBuilder().setScheme("none"))
            .build();

    return TestSupport.createConnector(connectors, spec);
  }

  private void attachConnectorToTable(ResourceId tableId, Connector connector) {
    UpstreamRef upstream =
        UpstreamRef.newBuilder()
            .setConnectorId(connector.getResourceId())
            .setUri("dummy://ignored")
            .setTableDisplayName(connector.getDisplayName() + "_src")
            .setFormat(TableFormat.TF_ICEBERG)
            .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_FIELD_ID)
            .addNamespacePath("examples")
            .addNamespacePath("iceberg")
            .build();

    TableSpec spec = TableSpec.newBuilder().setUpstream(upstream).build();

    FieldMask mask = FieldMask.newBuilder().addPaths("upstream").build();

    table.updateTable(
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(spec)
            .setUpdateMask(mask)
            .build());
  }
}
