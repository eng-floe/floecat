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
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class NamespaceMutationIT {
  @Inject PointerStore ptr;
  @Inject BlobStore blob;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  ViewServiceGrpc.ViewServiceBlockingStub view;

  @GrpcClient("floecat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  String namespacePrefix = this.getClass().getSimpleName() + "_";

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void namespaceExists() throws Exception {
    var cat = TestSupport.createCatalog(catalog, namespacePrefix + "cat1", "cat1");

    TestSupport.createNamespace(
        namespace, cat.getResourceId(), "2025", List.of("staging"), "2025 ns");

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                TestSupport.createNamespace(
                    namespace, cat.getResourceId(), "2025", List.of("staging"), "2025 namespace"));
    TestSupport.assertGrpcAndMc(
        ex, Status.Code.ALREADY_EXISTS, ErrorCode.MC_CONFLICT, "already exists");
  }

  @Test
  void namespaceCreateRenameDelete() throws Exception {
    var cat = TestSupport.createCatalog(catalog, namespacePrefix + "cat2", "cat2");

    var parents = List.of("db_it", "schema_it");
    var leaf = "it_schema";
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), leaf, parents, "ns desc");
    ResourceId nsId = ns.getResourceId();
    assertEquals(ResourceKind.RK_NAMESPACE, nsId.getKind());

    var full = new ArrayList<>(parents);
    full.add(leaf);
    var resolved =
        directory.resolveNamespace(
            ResolveNamespaceRequest.newBuilder()
                .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName()).addAllPath(full))
                .build());
    assertEquals(nsId.getId(), resolved.getResourceId().getId());

    FieldMask mask_name = FieldMask.newBuilder().addPaths("display_name").build();
    var nsSpec = NamespaceSpec.newBuilder().setDisplayName(leaf + "_ren").build();
    var m1 =
        namespace
            .updateNamespace(
                UpdateNamespaceRequest.newBuilder()
                    .setNamespaceId(nsId)
                    .setSpec(nsSpec)
                    .setUpdateMask(mask_name)
                    .setPrecondition(
                        Precondition.newBuilder()
                            .setExpectedVersion(
                                TestSupport.metaForNamespace(
                                        ptr,
                                        blob,
                                        cat.getResourceId().getAccountId(),
                                        cat.getDisplayName(),
                                        full)
                                    .getPointerVersion())
                            .setExpectedEtag(
                                TestSupport.metaForNamespace(
                                        ptr,
                                        blob,
                                        cat.getResourceId().getAccountId(),
                                        cat.getDisplayName(),
                                        full)
                                    .getEtag())
                            .build())
                    .build())
            .getMeta();

    var fullRen = new ArrayList<>(parents);
    fullRen.add(leaf + "_ren");
    var resolvedRen =
        directory.resolveNamespace(
            ResolveNamespaceRequest.newBuilder()
                .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName()).addAllPath(fullRen))
                .build());
    assertEquals(nsId.getId(), resolvedRen.getResourceId().getId());

    FieldMask mask_path = FieldMask.newBuilder().addPaths("path").build();
    var m2Spec = NamespaceSpec.newBuilder().addAllPath(List.of(leaf + "_root")).build();
    var m2Resp =
        namespace.updateNamespace(
            UpdateNamespaceRequest.newBuilder()
                .setNamespaceId(nsId)
                .setSpec(m2Spec)
                .setUpdateMask(mask_path)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(m1.getPointerVersion())
                        .setExpectedEtag(m1.getEtag())
                        .build())
                .build());
    var m2 = m2Resp.getMeta();
    assertTrue(m2.getPointerVersion() > m1.getPointerVersion());

    var resolvedRoot =
        directory.resolveNamespace(
            ResolveNamespaceRequest.newBuilder()
                .setRef(
                    NameRef.newBuilder().setCatalog(cat.getDisplayName()).addPath(leaf + "_root"))
                .build());
    assertEquals(nsId.getId(), resolvedRoot.getResourceId().getId());

    var badSpec = NamespaceSpec.newBuilder().setDisplayName(leaf + "_root2").build();
    var bad =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.updateNamespace(
                    UpdateNamespaceRequest.newBuilder()
                        .setNamespaceId(nsId)
                        .setSpec(badSpec)
                        .setUpdateMask(mask_name)
                        .setPrecondition(
                            Precondition.newBuilder()
                                .setExpectedVersion(123456L)
                                .setExpectedEtag("bogus")
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        bad, Status.Code.FAILED_PRECONDITION, ErrorCode.MC_PRECONDITION_FAILED, "mismatch");

    var before =
        TestSupport.metaForNamespace(
            ptr,
            blob,
            cat.getResourceId().getAccountId(),
            cat.getDisplayName(),
            List.of(leaf + "_root"));

    // Bump the version
    var m3Spec = NamespaceSpec.newBuilder().setDisplayName(leaf + "_root3").build();
    var m3Resp =
        namespace.updateNamespace(
            UpdateNamespaceRequest.newBuilder()
                .setNamespaceId(nsId)
                .setSpec(m3Spec)
                .setUpdateMask(mask_name)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(before.getPointerVersion())
                        .setExpectedEtag(before.getEtag())
                        .build())
                .build());
    var m3 = m3Resp.getMeta();

    // Now try to delete with the stale precondition
    var stale =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.deleteNamespace(
                    DeleteNamespaceRequest.newBuilder()
                        .setNamespaceId(nsId)
                        .setRequireEmpty(true)
                        .setPrecondition(
                            Precondition.newBuilder()
                                .setExpectedVersion(before.getPointerVersion()) // stale
                                .setExpectedEtag(before.getEtag()) // stale
                                .build())
                        .build()));

    TestSupport.assertGrpcAndMc(
        stale, Status.Code.FAILED_PRECONDITION, ErrorCode.MC_PRECONDITION_FAILED, "mismatch");

    var tbl =
        TestSupport.createTable(
            table, cat.getResourceId(), nsId, "orders", "s3://ns/orders", "{}", "none");

    StatusRuntimeException nsDelBlocked =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.deleteNamespace(
                    DeleteNamespaceRequest.newBuilder()
                        .setNamespaceId(nsId)
                        .setRequireEmpty(true)
                        .setPrecondition(
                            Precondition.newBuilder()
                                .setExpectedVersion(m3.getPointerVersion())
                                .setExpectedEtag(m3.getEtag())
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        nsDelBlocked,
        Status.Code.ABORTED,
        ErrorCode.MC_CONFLICT,
        "Namespace \"" + leaf + "_root3" + "\" contains tables, views, and/or children.");

    TestSupport.deleteTable(table, nsId, tbl.getResourceId());

    var delOk =
        namespace.deleteNamespace(
            DeleteNamespaceRequest.newBuilder()
                .setNamespaceId(nsId)
                .setRequireEmpty(true)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(m3.getPointerVersion())
                        .setExpectedEtag(m3.getEtag())
                        .build())
                .build());
    assertFalse(delOk.getMeta().getPointerKey().isEmpty());

    var nf =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                directory.resolveNamespace(
                    ResolveNamespaceRequest.newBuilder()
                        .setRef(
                            NameRef.newBuilder()
                                .setCatalog(cat.getDisplayName())
                                .addPath(leaf + "_root3"))
                        .build()));
    TestSupport.assertGrpcAndMc(
        nf, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "Namespace not found");
  }

  @Test
  void namespaceDeleteRecursiveDropsDescendantsAndTables() throws Exception {
    var cat = TestSupport.createCatalog(catalog, namespacePrefix + "recursive_cat", "recursive");
    var parent =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "parent", List.of(), "");
    var child =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "child", List.of("parent"), "");
    var tableToDrop =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            child.getResourceId(),
            "orders",
            "s3://bucket/orders",
            "{\"cols\":[]}",
            "none");

    var deleted =
        namespace.deleteNamespace(
            DeleteNamespaceRequest.newBuilder()
                .setNamespaceId(parent.getResourceId())
                .setRecursive(true)
                .build());

    // The response reports the teardown, root included — two namespaces, not just the descendant.
    assertEquals(2, deleted.getDeletedNamespaces());
    assertEquals(1, deleted.getDeletedTables());
    assertEquals(0, deleted.getDeletedViews());

    assertThrows(
        StatusRuntimeException.class,
        () ->
            namespace.getNamespace(
                GetNamespaceRequest.newBuilder().setNamespaceId(parent.getResourceId()).build()));
    assertThrows(
        StatusRuntimeException.class,
        () ->
            namespace.getNamespace(
                GetNamespaceRequest.newBuilder().setNamespaceId(child.getResourceId()).build()));
    assertThrows(
        StatusRuntimeException.class,
        () ->
            table.getTable(
                GetTableRequest.newBuilder().setTableId(tableToDrop.getResourceId()).build()));
  }

  /**
   * require_empty is not consulted anywhere: a non-recursive delete refuses a non-empty namespace
   * whether or not it is set. So a caller whose request defaults have always carried it — the habit
   * of anything careful, TestSupport included — must be able to add recursive without first
   * unsetting a flag that never did anything. recursive wins, and the subtree goes.
   */
  /**
   * A namespace cannot be moved inside its own subtree. Reparenting {@code db_self} to sit under
   * itself would write {@code by-path/db_self/leaf} and delete {@code by-path/db_self} in one
   * batch, committing a namespace whose parent path resolves to nothing — reachable by id,
   * invisible to every FQ-name lookup and subtree walk. Its own childlessness is no defence: the
   * destination fence resolves the namespace being moved as its own parent.
   */
  @Test
  void aNamespaceCannotBeMovedUnderItself() throws Exception {
    var cat = TestSupport.createCatalog(catalog, namespacePrefix + "self_parent", "self");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "db_self", List.of(), "");

    var refused =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.updateNamespace(
                    UpdateNamespaceRequest.newBuilder()
                        .setNamespaceId(ns.getResourceId())
                        // Full path: the last segment is the name, the rest is the parent — so this
                        // asks for a namespace whose parent is itself.
                        .setSpec(NamespaceSpec.newBuilder().addAllPath(List.of("db_self", "leaf")))
                        .setUpdateMask(FieldMask.newBuilder().addPaths("path"))
                        .build()));
    TestSupport.assertGrpcAndMc(
        refused, Status.Code.INVALID_ARGUMENT, ErrorCode.MC_INVALID_ARGUMENT, "own subtree");

    // Still exactly where it was, and still resolvable by name.
    assertEquals(
        ns.getResourceId().getId(),
        TestSupport.resolveNamespaceId(directory, cat.getDisplayName(), List.of("db_self"))
            .getId());
  }

  /**
   * The teardown counts describe a teardown, so a plain delete leaves them at zero — it removed
   * exactly the namespace the request named, which is 1 by construction and says nothing, and
   * {@code meta} is what reports its outcome. Zero here does not mean nothing was deleted.
   */
  @Test
  void aPlainDeleteReportsNoTeardownCounts() throws Exception {
    var cat = TestSupport.createCatalog(catalog, namespacePrefix + "plain_counts", "plain");
    var leaf =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "leaf", List.of("db_plain"), "");

    var deleted =
        namespace.deleteNamespace(
            DeleteNamespaceRequest.newBuilder().setNamespaceId(leaf.getResourceId()).build());

    assertEquals(0, deleted.getDeletedNamespaces());
    assertEquals(0, deleted.getDeletedTables());
    assertEquals(0, deleted.getDeletedViews());
    // And it really is gone — the zeros are about teardown, not about the delete.
    assertThrows(
        StatusRuntimeException.class,
        () ->
            namespace.getNamespace(
                GetNamespaceRequest.newBuilder().setNamespaceId(leaf.getResourceId()).build()));
  }

  @Test
  void namespaceDeleteRejectsRecursiveWithRequireEmpty() throws Exception {
    var cat = TestSupport.createCatalog(catalog, namespacePrefix + "req_empty_cat", "both flags");
    var parent =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "parent", List.of(), "");
    var child =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "child", List.of("parent"), "");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            child.getResourceId(),
            "orders",
            "s3://bucket/orders_both",
            "{\"cols\":[]}",
            "none");

    var rejected =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.deleteNamespace(
                    DeleteNamespaceRequest.newBuilder()
                        .setNamespaceId(parent.getResourceId())
                        .setRecursive(true)
                        .setRequireEmpty(true)
                        .build()));

    assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, rejected.getStatus().getCode());
    assertEquals(
        child.getResourceId(),
        namespace
            .getNamespace(
                GetNamespaceRequest.newBuilder().setNamespaceId(child.getResourceId()).build())
            .getNamespace()
            .getResourceId());
    assertEquals(
        tbl.getResourceId(),
        table
            .getTable(GetTableRequest.newBuilder().setTableId(tbl.getResourceId()).build())
            .getTable()
            .getResourceId());
  }

  @Test
  void namespaceDeleteRecursiveDropsNestedTreeTablesAndViews() throws Exception {
    // Three levels (db/schema/sub) with relations at multiple depths exercises the marker protocol:
    // deleting each descendant must not advance the root (db) marker, so the service's single
    // advance holds and the delete succeeds without a spurious retry (regression for #397).
    var cat =
        TestSupport.createCatalog(catalog, namespacePrefix + "recursive_nested_cat", "nested");
    var db = TestSupport.createNamespace(namespace, cat.getResourceId(), "db", List.of(), "");
    var schema =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "schema", List.of("db"), "");
    var sub =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "sub", List.of("db", "schema"), "");

    var schemaTable =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            schema.getResourceId(),
            "events",
            "s3://bucket/events",
            "{\"cols\":[]}",
            "none");
    var subTable =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            sub.getResourceId(),
            "orders",
            "s3://bucket/orders",
            "{\"cols\":[]}",
            "none");
    var schemaView =
        TestSupport.createView(
            view, cat.getResourceId(), schema.getResourceId(), "events_view", "SELECT 1", "nested");

    namespace.deleteNamespace(
        DeleteNamespaceRequest.newBuilder()
            .setNamespaceId(db.getResourceId())
            .setRecursive(true)
            .build());

    for (var ns : List.of(db, schema, sub)) {
      assertThrows(
          StatusRuntimeException.class,
          () ->
              namespace.getNamespace(
                  GetNamespaceRequest.newBuilder().setNamespaceId(ns.getResourceId()).build()));
    }
    for (var t : List.of(schemaTable, subTable)) {
      assertThrows(
          StatusRuntimeException.class,
          () -> table.getTable(GetTableRequest.newBuilder().setTableId(t.getResourceId()).build()));
    }
    assertThrows(
        StatusRuntimeException.class,
        () ->
            view.getView(
                GetViewRequest.newBuilder().setViewId(schemaView.getResourceId()).build()));
  }

  @Test
  void namespaceWithOnlyViewsIsNotEmptyWithoutRecursive() throws Exception {
    // Views are namespace-owned relations: a namespace holding only a view must be rejected as
    // non-empty on a plain delete, not silently deleted leaving orphaned view pointers (#397).
    var cat = TestSupport.createCatalog(catalog, namespacePrefix + "views_only_cat", "views");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "reports", List.of(), "");
    TestSupport.createView(
        view, cat.getResourceId(), ns.getResourceId(), "daily", "SELECT 1", "views");

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.deleteNamespace(
                    DeleteNamespaceRequest.newBuilder()
                        .setNamespaceId(ns.getResourceId())
                        .build()));
    TestSupport.assertGrpcAndMc(ex, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "views");

    // The namespace and its view survive the rejected delete.
    namespace.getNamespace(
        GetNamespaceRequest.newBuilder().setNamespaceId(ns.getResourceId()).build());
  }

  @Test
  void namespaceCreateIdempotent() throws Exception {
    var cat = TestSupport.createCatalog(catalog, namespacePrefix + "cat3", "cat3");

    var key = IdempotencyKey.newBuilder().setKey(namespacePrefix + "k-ns-1").build();

    var spec =
        NamespaceSpec.newBuilder()
            .setCatalogId(cat.getResourceId())
            .setDisplayName("idem_ns")
            .addAllPath(List.of("staging"))
            .setDescription("x")
            .build();

    var r1 =
        namespace.createNamespace(
            CreateNamespaceRequest.newBuilder().setSpec(spec).setIdempotency(key).build());
    var r2 =
        namespace.createNamespace(
            CreateNamespaceRequest.newBuilder().setSpec(spec).setIdempotency(key).build());

    assertEquals(
        r1.getNamespace().getResourceId().getId(), r2.getNamespace().getResourceId().getId());
    assertEquals(r1.getMeta().getPointerKey(), r2.getMeta().getPointerKey());
    assertEquals(r1.getMeta().getPointerVersion(), r2.getMeta().getPointerVersion());
    assertEquals(r1.getMeta().getEtag(), r2.getMeta().getEtag());
  }

  @Test
  void namespaceCreateIdempotencyMismatch() throws Exception {
    var cat = TestSupport.createCatalog(catalog, namespacePrefix + "cat4", "cat4");
    var key = IdempotencyKey.newBuilder().setKey(namespacePrefix + "k-ns-2").build();

    namespace.createNamespace(
        CreateNamespaceRequest.newBuilder()
            .setSpec(
                NamespaceSpec.newBuilder()
                    .setCatalogId(cat.getResourceId())
                    .setDisplayName("idem_ns2")
                    .addAllPath(List.of("db"))
                    .build())
            .setIdempotency(key)
            .build());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.createNamespace(
                    CreateNamespaceRequest.newBuilder()
                        .setSpec(
                            NamespaceSpec.newBuilder()
                                .setCatalogId(cat.getResourceId())
                                .setDisplayName("idem_ns2_DIFFERENT")
                                .addAllPath(List.of("db"))
                                .build())
                        .setIdempotency(key)
                        .build()));
    TestSupport.assertGrpcAndMc(
        ex, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "Idempotency key mismatch");
  }

  @Test
  void namespaceConcurrentParentCreation() throws Exception {
    var cat = TestSupport.createCatalog(catalog, namespacePrefix + "cat_conc", "cat_conc");
    var parents = List.of("db_conc", "sch_conc");

    var outA = new AtomicReference<Namespace>();
    var outB = new AtomicReference<Namespace>();
    var err = new AtomicReference<Throwable>();
    var start = new CountDownLatch(1);

    Runnable r =
        () -> {
          try {
            start.await();
            outA.set(
                TestSupport.createNamespace(
                    namespace, cat.getResourceId(), "ns_a", parents, "ns a"));
          } catch (Throwable t) {
            err.compareAndSet(null, t);
          }
        };

    Runnable s =
        () -> {
          try {
            start.await();
            outB.set(
                TestSupport.createNamespace(
                    namespace, cat.getResourceId(), "ns_b", parents, "ns b"));
          } catch (Throwable t) {
            err.compareAndSet(null, t);
          }
        };

    var t1 = new Thread(r);
    var t2 = new Thread(s);
    t1.start();
    t2.start();
    start.countDown();
    t1.join();
    t2.join();

    assertNull(err.get(), "unexpected error in concurrent parent chain creation");
    assertNotNull(outA.get());
    assertNotNull(outB.get());

    var pathA = new ArrayList<>(parents);
    pathA.add("ns_a");
    var resolvedA =
        directory.resolveNamespace(
            ResolveNamespaceRequest.newBuilder()
                .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName()).addAllPath(pathA))
                .build());
    assertEquals(outA.get().getResourceId().getId(), resolvedA.getResourceId().getId());

    var pathB = new ArrayList<>(parents);
    pathB.add("ns_b");
    var resolvedB =
        directory.resolveNamespace(
            ResolveNamespaceRequest.newBuilder()
                .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName()).addAllPath(pathB))
                .build());
    assertEquals(outB.get().getResourceId().getId(), resolvedB.getResourceId().getId());
  }

  /**
   * Reparenting to a path that does not exist is unsatisfiable input, not contention. The
   * destination fence reports a missing parent as retryable — correct where a create has just
   * ensured the chain — so taking that route here would retry eight times with backoff and answer
   * ABORTED for a request that can never succeed.
   */
  @Test
  void reparentingToAMissingParentIsRefusedWithoutRetrying() {
    var cat = TestSupport.createCatalog(catalog, namespacePrefix + "reparent_cat", "");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "leaf", List.of("db_reparent"), "leaf");

    long startedAt = System.nanoTime();
    var refused =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.updateNamespace(
                    UpdateNamespaceRequest.newBuilder()
                        .setNamespaceId(ns.getResourceId())
                        .setSpec(
                            NamespaceSpec.newBuilder()
                                .setCatalogId(cat.getResourceId())
                                .setDisplayName("leaf")
                                // Full path: the last segment is the name, the rest is the parent —
                                // so this moves "leaf" under a parent that does not exist.
                                .addAllPath(List.of("db_reparent", "no_such_parent", "leaf")))
                        .setUpdateMask(FieldMask.newBuilder().addPaths("path"))
                        .build()));
    long elapsedMs = (System.nanoTime() - startedAt) / 1_000_000;

    assertEquals(Status.Code.NOT_FOUND, refused.getStatus().getCode());
    // Eight retries with backoff would take far longer than this; the point is that it does not
    // retry.
    assertTrue(elapsedMs < 500, "should fail immediately, took " + elapsedMs + "ms");
    // And the namespace stays where it was.
    assertTrue(
        ptr.get(
                Keys.namespacePointerByPath(
                    cat.getResourceId().getAccountId(),
                    cat.getResourceId().getId(),
                    List.of("db_reparent", "leaf")))
            .isPresent());
  }

  /**
   * A child namespace is indexed under its parent's path, and that row is built from the child's
   * own blob — so renaming the parent does not move it. Allowing the rename would leave the child
   * under a first segment that resolves to nothing: unreachable by every walk, by the emptiness
   * gate, and by a recursive delete, which would then report the renamed parent empty and delete
   * it.
   */
  @Test
  void renamingANamespaceThatHasChildrenIsRefused() throws Exception {
    var cat = TestSupport.createCatalog(catalog, namespacePrefix + "cat_strand", "cat_strand");

    // Creating db_strand.child creates db_strand as its ancestor.
    TestSupport.createNamespace(
        namespace, cat.getResourceId(), "child", List.of("db_strand"), "child ns");
    var parentId =
        TestSupport.resolveNamespaceId(directory, cat.getDisplayName(), List.of("db_strand"));

    var refused =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.updateNamespace(
                    UpdateNamespaceRequest.newBuilder()
                        .setNamespaceId(parentId)
                        .setSpec(NamespaceSpec.newBuilder().setDisplayName("db_strand_renamed"))
                        .setUpdateMask(FieldMask.newBuilder().addPaths("display_name"))
                        .build()));
    TestSupport.assertGrpcAndMc(
        refused, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "child namespaces");

    // Nothing moved: the parent keeps its name and the child is still resolvable beneath it.
    assertEquals(
        parentId.getId(),
        TestSupport.resolveNamespaceId(directory, cat.getDisplayName(), List.of("db_strand"))
            .getId());
    assertTrue(
        ptr.get(
                Keys.namespacePointerByPath(
                    cat.getResourceId().getAccountId(),
                    cat.getResourceId().getId(),
                    List.of("db_strand", "child")))
            .isPresent());
  }

  /**
   * Tables and views are keyed by namespace id, not by the namespace's path, so a rename inside one
   * catalog leaves them addressable. The guard above must not reach this case.
   */
  @Test
  void renamingANamespaceThatOnlyHoldsRelationsIsAllowed() throws Exception {
    var cat = TestSupport.createCatalog(catalog, namespacePrefix + "cat_keep", "cat_keep");

    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "leaf_keep", List.of("db_keep"), "leaf ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders",
            "s3://bucket/orders_keep",
            "{\"cols\":[]}",
            "none");

    namespace.updateNamespace(
        UpdateNamespaceRequest.newBuilder()
            .setNamespaceId(ns.getResourceId())
            .setSpec(NamespaceSpec.newBuilder().setDisplayName("leaf_keep_renamed"))
            .setUpdateMask(FieldMask.newBuilder().addPaths("display_name"))
            .build());

    assertEquals(
        ns.getResourceId().getId(),
        TestSupport.resolveNamespaceId(
                directory, cat.getDisplayName(), List.of("db_keep", "leaf_keep_renamed"))
            .getId());
    // And the table it holds is still there, keyed by the namespace id the rename did not change.
    assertEquals(
        tbl.getResourceId().getId(),
        table
            .getTable(GetTableRequest.newBuilder().setTableId(tbl.getResourceId()).build())
            .getTable()
            .getResourceId()
            .getId());
  }

  /**
   * A relation's by-name row does name the catalog, so moving its namespace to another catalog
   * would leave that row behind in the catalog being left.
   */
  @Test
  void movingANamespaceThatHoldsRelationsToAnotherCatalogIsRefused() throws Exception {
    var from = TestSupport.createCatalog(catalog, namespacePrefix + "cat_from", "cat_from");
    var to = TestSupport.createCatalog(catalog, namespacePrefix + "cat_to", "cat_to");

    var ns =
        TestSupport.createNamespace(
            namespace, from.getResourceId(), "leaf_move", List.of("db_move"), "leaf ns");
    TestSupport.createTable(
        table,
        from.getResourceId(),
        ns.getResourceId(),
        "orders",
        "s3://bucket/orders_move",
        "{\"cols\":[]}",
        "none");

    var refused =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.updateNamespace(
                    UpdateNamespaceRequest.newBuilder()
                        .setNamespaceId(ns.getResourceId())
                        .setSpec(NamespaceSpec.newBuilder().setCatalogId(to.getResourceId()))
                        .setUpdateMask(FieldMask.newBuilder().addPaths("catalog_id"))
                        .build()));
    TestSupport.assertGrpcAndMc(
        refused, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "tables or views");

    // The namespace stays in the catalog it started in.
    assertEquals(
        ns.getResourceId().getId(),
        TestSupport.resolveNamespaceId(
                directory, from.getDisplayName(), List.of("db_move", "leaf_move"))
            .getId());
  }

  /**
   * The count the refusal above rests on is over by-name index rows, and a row whose relation is
   * already gone strands nothing — there is nothing left to strand. A corrupt-blob delete leaves
   * exactly that row, so trusting the raw count refuses a move that is perfectly safe, and refuses
   * it forever: no relocation clears those rows, so residue from a delete that already happened
   * would pin the namespace to its catalog for good.
   *
   * <p>The delete path reconciles before trusting the same count. This asserts the relocation path
   * does too.
   */
  @Test
  void movingANamespaceHoldingOnlyStrandedRelationRowsToAnotherCatalogSucceeds() throws Exception {
    var from = TestSupport.createCatalog(catalog, namespacePrefix + "cat_sfrom", "cat_sfrom");
    var to = TestSupport.createCatalog(catalog, namespacePrefix + "cat_sto", "cat_sto");

    // Root-level, so the destination path is the namespace's own name and the move needs no
    // ancestor to already exist in the catalog it lands in.
    var ns =
        TestSupport.createNamespace(
            namespace, from.getResourceId(), "leaf_strand", List.of(), "leaf ns");
    var tbl =
        TestSupport.createTable(
            table,
            from.getResourceId(),
            ns.getResourceId(),
            "orders",
            "s3://bucket/orders_strandmove",
            "{\"cols\":[]}",
            "none");
    var tid = tbl.getResourceId();

    String byName =
        Keys.tablePointerByName(
            tid.getAccountId(), from.getResourceId().getId(), ns.getResourceId().getId(), "orders");
    String byId = Keys.tablePointerById(tid.getAccountId(), tid.getId());

    // Delete the blob out from under the table, then delete it. The repository cannot read the
    // secondary keys, so it removes the canonical pointer alone and the by-name row is stranded.
    assertTrue(blob.delete(ptr.get(byId).orElseThrow().getBlobUri()));
    table.deleteTable(DeleteTableRequest.newBuilder().setTableId(tid).build());
    assertTrue(ptr.get(byId).isEmpty(), "canonical pointer gone");
    assertTrue(
        ptr.get(byName).isPresent(), "by-name row stranded, so the raw count still reports 1");

    // The namespace holds nothing that resolves, so the move is allowed once the row is reconciled.
    namespace.updateNamespace(
        UpdateNamespaceRequest.newBuilder()
            .setNamespaceId(ns.getResourceId())
            .setSpec(NamespaceSpec.newBuilder().setCatalogId(to.getResourceId()))
            .setUpdateMask(FieldMask.newBuilder().addPaths("catalog_id"))
            .build());

    assertEquals(
        ns.getResourceId().getId(),
        TestSupport.resolveNamespaceId(directory, to.getDisplayName(), List.of("leaf_strand"))
            .getId(),
        "the namespace moved to the destination catalog");
    assertTrue(ptr.get(byName).isEmpty(), "and the stranded row was released, not left behind");
  }
}
