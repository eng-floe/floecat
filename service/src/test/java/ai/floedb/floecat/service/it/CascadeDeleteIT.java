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
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.gc.RelationPurgeQueue;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class CascadeDeleteIT {
  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;
  @Inject RelationPurgeQueue relationPurgeQueue;
  @Inject PointerStore pointerStore;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void nonCascadeDeleteOfNonEmptyNamespaceIsRefused() throws Exception {
    var cat = TestSupport.createCatalog(catalog, "cascade_refuse", "");
    var parent = TestSupport.createNamespace(namespace, cat.getResourceId(), "db", List.of(), "");
    TestSupport.createNamespace(namespace, cat.getResourceId(), "child", List.of("db"), "");

    var refused =
        assertThrows(
            StatusRuntimeException.class,
            () -> TestSupport.deleteNamespace(namespace, parent.getResourceId(), false));
    TestSupport.assertGrpcAndMc(
        refused, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "Namespace \"db\" contains");
  }

  @Test
  void namespaceCascadeDeletesSubtreeAndHidesRelations() {
    var cat = TestSupport.createCatalog(catalog, "cascade_ns", "");
    ResourceId catId = cat.getResourceId();
    var db = TestSupport.createNamespace(namespace, catId, "db", List.of(), "");
    var child = TestSupport.createNamespace(namespace, catId, "child", List.of("db"), "");
    var sibling = TestSupport.createNamespace(namespace, catId, "other", List.of(), "");
    var stranded =
        TestSupport.createTable(
            table, catId, child.getResourceId(), "events", "s3://events", "{}", "");
    var kept =
        TestSupport.createTable(
            table, catId, sibling.getResourceId(), "kept", "s3://kept", "{}", "");

    TestSupport.deleteNamespace(namespace, db.getResourceId(), true);

    assertNotFound(
        () ->
            namespace.getNamespace(
                GetNamespaceRequest.newBuilder().setNamespaceId(db.getResourceId()).build()));
    assertNotFound(
        () ->
            namespace.getNamespace(
                GetNamespaceRequest.newBuilder().setNamespaceId(child.getResourceId()).build()));
    assertNotFound(
        () ->
            table.getTable(
                GetTableRequest.newBuilder().setTableId(stranded.getResourceId()).build()));
    table.getTable(GetTableRequest.newBuilder().setTableId(kept.getResourceId()).build());

    String tableKey = Keys.tablePointerById(catId.getAccountId(), stranded.getResourceId().getId());
    assertTrue(pointerStore.get(tableKey).isPresent(), "row stays until GC");
    relationPurgeQueue.drain(System.currentTimeMillis() + 10_000L, 500);
    assertTrue(pointerStore.get(tableKey).isEmpty(), "deletion queue reclaims the stranded table");
    assertTrue(
        pointerStore
            .get(Keys.tablePointerById(catId.getAccountId(), kept.getResourceId().getId()))
            .isPresent());

    // The name is free again, and the new namespace does not inherit the old subtree.
    var reborn = TestSupport.createNamespace(namespace, catId, "db", List.of(), "");
    var children =
        namespace.listNamespaces(
            ListNamespacesRequest.newBuilder()
                .setNamespaceId(reborn.getResourceId())
                .setChildrenOnly(true)
                .build());
    assertEquals(0, children.getNamespacesCount());
  }

  @Test
  void catalogCascadeDeletesEverything() throws Exception {
    var cat = TestSupport.createCatalog(catalog, "cascade_cat", "");
    ResourceId catId = cat.getResourceId();
    var db = TestSupport.createNamespace(namespace, catId, "db", List.of(), "");
    TestSupport.createNamespace(namespace, catId, "child", List.of("db"), "");
    var events =
        TestSupport.createTable(table, catId, db.getResourceId(), "events", "s3://e", "{}", "");

    var refused =
        assertThrows(
            StatusRuntimeException.class, () -> TestSupport.deleteCatalog(catalog, catId, false));
    TestSupport.assertGrpcAndMc(refused, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, null);

    TestSupport.deleteCatalog(catalog, catId, true);

    assertNotFound(
        () -> catalog.getCatalog(GetCatalogRequest.newBuilder().setCatalogId(catId).build()));
    assertNotFound(
        () ->
            table.getTable(
                GetTableRequest.newBuilder().setTableId(events.getResourceId()).build()));

    relationPurgeQueue.drain(System.currentTimeMillis() + 10_000L, 500);
    assertTrue(
        pointerStore
            .get(Keys.tablePointerById(catId.getAccountId(), events.getResourceId().getId()))
            .isEmpty());
    assertEquals(
        0,
        pointerStore.countByPrefix(
            Keys.namespacePointerByPathPrefix(catId.getAccountId(), catId.getId(), List.of())));
  }

  @Test
  void plainCatalogMutationsReportCascadeDeletionInProgress() throws Exception {
    var cat = TestSupport.createCatalog(catalog, "fenced_cat", "");
    ResourceId id = cat.getResourceId();
    installFence(Keys.catalogDeletionMarker(id.getAccountId(), id.getId()));

    var update =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                catalog.updateCatalog(
                    UpdateCatalogRequest.newBuilder()
                        .setCatalogId(id)
                        .setSpec(CatalogSpec.newBuilder().setDescription("changed"))
                        .setUpdateMask(FieldMask.newBuilder().addPaths("description"))
                        .build()));
    TestSupport.assertGrpcAndMc(
        update,
        Status.Code.ABORTED,
        ErrorCode.MC_CONFLICT,
        "Catalog operation blocked: cascade deletion in progress; retry with cascade");

    var delete =
        assertThrows(
            StatusRuntimeException.class, () -> TestSupport.deleteCatalog(catalog, id, false));
    TestSupport.assertGrpcAndMc(
        delete,
        Status.Code.ABORTED,
        ErrorCode.MC_CONFLICT,
        "Catalog operation blocked: cascade deletion in progress; retry with cascade");
  }

  @Test
  void plainNamespaceMutationsReportCascadeDeletionInProgress() throws Exception {
    var cat = TestSupport.createCatalog(catalog, "fenced_ns_cat", "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "fenced_ns", List.of(), "");
    ResourceId id = ns.getResourceId();
    installFence(Keys.namespaceDeletionMarker(id.getAccountId(), id.getId()));

    var update =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.updateNamespace(
                    UpdateNamespaceRequest.newBuilder()
                        .setNamespaceId(id)
                        .setSpec(NamespaceSpec.newBuilder().setDescription("changed"))
                        .setUpdateMask(FieldMask.newBuilder().addPaths("description"))
                        .build()));
    TestSupport.assertGrpcAndMc(
        update,
        Status.Code.ABORTED,
        ErrorCode.MC_CONFLICT,
        "Namespace operation blocked: cascade deletion in progress; retry with cascade");

    var delete =
        assertThrows(
            StatusRuntimeException.class, () -> TestSupport.deleteNamespace(namespace, id, false));
    TestSupport.assertGrpcAndMc(
        delete,
        Status.Code.ABORTED,
        ErrorCode.MC_CONFLICT,
        "Namespace operation blocked: cascade deletion in progress; retry with cascade");
  }

  private void installFence(String key) {
    assertTrue(
        pointerStore.compareAndSet(
            key, 0L, PointerReferences.opaqueMarkerPointer(key, "deleting", 1L)));
  }

  private static void assertNotFound(org.junit.jupiter.api.function.Executable call) {
    var missing = assertThrows(StatusRuntimeException.class, call);
    assertEquals(Status.Code.NOT_FOUND, missing.getStatus().getCode());
  }
}
