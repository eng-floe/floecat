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
import ai.floedb.floecat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.DeleteTableRequest;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceSpec;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveTableRequest;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.UpdateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Changing a namespace's identity when something below it derives a key from that identity.
 *
 * <p>A namespace's by-path pointer is derived from {@code catalogId + parents + displayName}, and a
 * relation's by-name pointer from {@code catalogId + namespaceId + name}. A repository recomputes
 * secondaries for the row it is writing, so nothing re-derives the keys of rows underneath. Rename
 * a parent and its children keep pointers under the old path; move a namespace to another catalog
 * and its relations keep pointers under the old catalog.
 *
 * <p>Relations are immune to a rename, because their key carries the namespace id rather than its
 * path -- which is why the guard here is narrower than the one on delete.
 */
@QuarkusTest
class NamespaceRestructureIT {

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
  void renamingANamespaceThatHasAChildIsRefused() {
    var cat = TestSupport.createCatalog(catalog, prefix + "cat_child", "");
    var parent =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "parent", List.of(), "parent namespace");
    TestSupport.createNamespace(
        namespace, cat.getResourceId(), "child", List.of("parent"), "child namespace");

    var ex =
        assertThrows(
            StatusRuntimeException.class, () -> rename(parent.getResourceId(), "parent_renamed"));
    assertEquals(Status.Code.ABORTED, ex.getStatus().getCode());

    // And the tree is untouched: the child still resolves where it always did.
    assertTrue(resolves(cat.getDisplayName(), List.of("parent", "child")));
  }

  @Test
  void renamingALeafWithRelationsIsAllowed() {
    // The case Postgres and Unity both permit: a schema with tables and no child schemas. A
    // relation's key carries the namespace id, not its path, so a rename strands nothing.
    var cat = TestSupport.createCatalog(catalog, prefix + "cat_leaf", "");
    var leaf =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "leaf", List.of(), "leaf namespace");
    TestSupport.createTable(
        table,
        cat.getResourceId(),
        leaf.getResourceId(),
        "orders",
        "s3://bucket/orders",
        "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"int\",\"required\":true}]}",
        "a table under the leaf");

    rename(leaf.getResourceId(), "leaf_renamed");

    assertTrue(resolves(cat.getDisplayName(), List.of("leaf_renamed")));
  }

  @Test
  void reParentingANamespaceThatHasAChildIsRefused() {
    var cat = TestSupport.createCatalog(catalog, prefix + "cat_move", "");
    TestSupport.createNamespace(namespace, cat.getResourceId(), "top", List.of(), "top namespace");
    var mid =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "mid", List.of(), "mid namespace");
    TestSupport.createNamespace(
        namespace, cat.getResourceId(), "leaf", List.of("mid"), "leaf under mid");

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.updateNamespace(
                    UpdateNamespaceRequest.newBuilder()
                        .setNamespaceId(mid.getResourceId())
                        // The path mask carries the full path including the leaf, so re-parenting
                        // mid under top is ["top", "mid"] -- ["top"] would rename it to "top".
                        .setSpec(NamespaceSpec.newBuilder().addPath("top").addPath("mid").build())
                        .setUpdateMask(FieldMask.newBuilder().addPaths("path").build())
                        .build()));
    assertEquals(Status.Code.ABORTED, ex.getStatus().getCode());
  }

  @Test
  void aRenameThatChangesNothingIsStillAllowed() {
    // The mask carries display_name but the value is unchanged, so no key moves and nothing below
    // can be stranded. The REST gateway's property update takes this shape: it copies the current
    // identity into the spec and masks only properties.
    var cat = TestSupport.createCatalog(catalog, prefix + "cat_noop", "");
    var parent =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "same", List.of(), "parent");
    TestSupport.createNamespace(namespace, cat.getResourceId(), "below", List.of("same"), "child");

    rename(parent.getResourceId(), "same");

    assertTrue(resolves(cat.getDisplayName(), List.of("same", "below")));
  }

  /**
   * Moving a namespace beneath itself strands it with no concurrency involved: the destination
   * parent resolves to the namespace itself, so parent-exists and no-children both pass, and the
   * write then vacates the path it just claimed to live under.
   */
  @Test
  void movingANamespaceBeneathItselfIsRefused() {
    var cat = TestSupport.createCatalog(catalog, prefix + "cat_cycle", "");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "loop", List.of(), "ns");

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.updateNamespace(
                    UpdateNamespaceRequest.newBuilder()
                        .setNamespaceId(ns.getResourceId())
                        .setSpec(NamespaceSpec.newBuilder().addPath("loop").addPath("loop").build())
                        .setUpdateMask(FieldMask.newBuilder().addPaths("path").build())
                        .build()));
    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    assertTrue(resolves(cat.getDisplayName(), List.of("loop")));
  }

  /**
   * A destination parent that does not exist is the caller's error, and has to be reported as one.
   *
   * <p>NOT_FOUND rather than ABORTED is the whole assertion. The fence resolves the destination
   * parent, so an unresolvable path arrives here as a lost fence unless it is classified first --
   * and ABORTED tells a client to retry a path that does not exist, which is an instruction to loop
   * forever.
   */
  @Test
  void movingANamespaceUnderAParentThatDoesNotExistIsNotFound() {
    var cat = TestSupport.createCatalog(catalog, prefix + "cat_absent", "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "orphaned", List.of(), "ns");

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.updateNamespace(
                    UpdateNamespaceRequest.newBuilder()
                        .setNamespaceId(ns.getResourceId())
                        .setSpec(
                            NamespaceSpec.newBuilder().addPath("nope").addPath("orphaned").build())
                        .setUpdateMask(FieldMask.newBuilder().addPaths("path").build())
                        .build()));
    assertEquals(Status.Code.NOT_FOUND, ex.getStatus().getCode());
    assertTrue(resolves(cat.getDisplayName(), List.of("orphaned")));
  }

  /**
   * A deleted namespace leaves no marker rows behind.
   *
   * <p>The markers count a shape that no longer exists, and namespace ids never recur, so an
   * advanced marker would be a row nothing can ever read. Removing them with the row keeps the
   * delete from leaking two pointers per namespace.
   */
  @Test
  void deletingANamespaceRemovesItsShapeMarkers() {
    var cat = TestSupport.createCatalog(catalog, prefix + "cat_markers_gone", "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "transient", List.of(), "ns");
    String account = cat.getResourceId().getAccountId();
    String children = Keys.namespaceChildrenMarker(account, ns.getResourceId().getId());
    String relations = Keys.namespaceRelationsMarker(account, ns.getResourceId().getId());

    // Give both markers a version, so the delete has something to remove rather than to require
    // absent.
    TestSupport.createNamespace(namespace, cat.getResourceId(), "child", List.of("transient"), "c");
    TestSupport.createTable(
        table,
        cat.getResourceId(),
        ns.getResourceId(),
        "t_gone",
        "s3://bucket/t_gone",
        "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"int\",\"required\":true}]}",
        "a table");
    assertTrue(version(children) > 0 && version(relations) > 0, "both markers exist to be removed");

    // Empty it, then delete it.
    table.deleteTable(
        DeleteTableRequest.newBuilder()
            .setTableId(
                directory
                    .resolveTable(
                        ResolveTableRequest.newBuilder()
                            .setRef(
                                NameRef.newBuilder()
                                    .setCatalog(cat.getDisplayName())
                                    .addAllPath(List.of("transient"))
                                    .setName("t_gone"))
                            .build())
                    .getResourceId())
            .build());
    namespace.deleteNamespace(
        DeleteNamespaceRequest.newBuilder()
            .setNamespaceId(
                directory
                    .resolveNamespace(
                        ResolveNamespaceRequest.newBuilder()
                            .setRef(
                                NameRef.newBuilder()
                                    .setCatalog(cat.getDisplayName())
                                    .addAllPath(List.of("transient", "child")))
                            .build())
                    .getResourceId())
            .build());
    namespace.deleteNamespace(
        DeleteNamespaceRequest.newBuilder().setNamespaceId(ns.getResourceId()).build());

    assertEquals(0L, version(children), "the child marker goes with the namespace");
    assertEquals(0L, version(relations), "so does the relation marker");
  }

  private void rename(ResourceId namespaceId, String newName) {
    namespace.updateNamespace(
        UpdateNamespaceRequest.newBuilder()
            .setNamespaceId(namespaceId)
            .setSpec(NamespaceSpec.newBuilder().setDisplayName(newName).build())
            .setUpdateMask(FieldMask.newBuilder().addPaths("display_name").build())
            .build());
  }

  private boolean resolves(String catalogName, List<String> path) {
    try {
      var resolved =
          directory.resolveNamespace(
              ResolveNamespaceRequest.newBuilder()
                  .setRef(
                      NameRef.newBuilder()
                          .setCatalog(catalogName)
                          .addAllPath(new ArrayList<>(path)))
                  .build());
      return !resolved.getResourceId().getId().isBlank();
    } catch (StatusRuntimeException notFound) {
      return false;
    }
  }

  @Test
  void aNamespaceHoldingOnlyViewsIsNotEmpty() {
    // Tables and views carry separate by-name prefixes, so a count that covers only one reports a
    // namespace holding the other as empty -- and deleting it leaves every view addressable under a
    // namespace id that does not exist. No race required.
    var cat = TestSupport.createCatalog(catalog, prefix + "cat_views", "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "holds_views", List.of(), "ns");
    TestSupport.createView(
        view, cat.getResourceId(), ns.getResourceId(), "v_orders", "SELECT 1", "a view");

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.deleteNamespace(
                    DeleteNamespaceRequest.newBuilder()
                        .setNamespaceId(ns.getResourceId())
                        .build()));
    assertEquals(Status.Code.ABORTED, ex.getStatus().getCode());
    assertTrue(resolves(cat.getDisplayName(), List.of("holds_views")));
  }

  @Test
  void movingANamespaceHoldingOnlyViewsToAnotherCatalogIsRefused() {
    // A relation's by-name key carries the catalog, so a catalog move re-keys views exactly as it
    // re-keys tables.
    var from = TestSupport.createCatalog(catalog, prefix + "cat_from", "");
    var to = TestSupport.createCatalog(catalog, prefix + "cat_to", "");
    var ns =
        TestSupport.createNamespace(namespace, from.getResourceId(), "movable", List.of(), "ns");
    TestSupport.createView(
        view, from.getResourceId(), ns.getResourceId(), "v_only", "SELECT 1", "a view");

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.updateNamespace(
                    UpdateNamespaceRequest.newBuilder()
                        .setNamespaceId(ns.getResourceId())
                        .setSpec(
                            NamespaceSpec.newBuilder().setCatalogId(to.getResourceId()).build())
                        .setUpdateMask(FieldMask.newBuilder().addPaths("catalog_id").build())
                        .build()));
    assertEquals(Status.Code.ABORTED, ex.getStatus().getCode());
  }

  /**
   * Every write that changes a namespace's shape must advance the marker the guarding operations
   * fence on. Asserted directly, because exclusion follows from participation plus the CAS -- and
   * participation is deterministic where a race is not: two RPCs released from one latch finish
   * microseconds apart, so a timing test passes whether or not anything is fenced.
   */
  @Test
  void everyShapeChangeAdvancesTheMarkerItsGuardsFenceOn() {
    var cat = TestSupport.createCatalog(catalog, prefix + "cat_markers", "");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "shape", List.of(), "ns");
    String accountId = cat.getResourceId().getAccountId();
    String relations = Keys.namespaceRelationsMarker(accountId, ns.getResourceId().getId());
    String children = Keys.namespaceChildrenMarker(accountId, ns.getResourceId().getId());

    long relationsBefore = version(relations);
    TestSupport.createTable(
        table,
        cat.getResourceId(),
        ns.getResourceId(),
        "t_marker",
        "s3://bucket/t_marker",
        "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"int\",\"required\":true}]}",
        "a table");
    assertTrue(
        version(relations) > relationsBefore,
        "creating a table must advance the namespace's relation marker, or a namespace delete"
            + " cannot exclude it");

    long afterTable = version(relations);
    TestSupport.createView(
        view, cat.getResourceId(), ns.getResourceId(), "v_marker", "SELECT 1", "a view");
    assertTrue(
        version(relations) > afterTable,
        "creating a view must advance it too -- views are relations");

    long childrenBefore = version(children);
    TestSupport.createNamespace(namespace, cat.getResourceId(), "below", List.of("shape"), "child");
    assertTrue(
        version(children) > childrenBefore,
        "creating a child namespace must advance the parent's child marker, or a rename cannot"
            + " exclude it");
  }

  /**
   * The other half of the two-marker split, and the half a participation test alone cannot see:
   * relation traffic must leave the child-namespace marker alone. Crossing the two is silent -- it
   * excludes nothing extra -- but it costs every concurrent rename its fence, so a namespace with
   * ordinary table traffic becomes one that cannot be renamed.
   */
  @Test
  void relationTrafficDoesNotDisturbTheChildNamespaceMarker() {
    var cat = TestSupport.createCatalog(catalog, prefix + "cat_split", "");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "split", List.of(), "ns");
    String children =
        Keys.namespaceChildrenMarker(
            cat.getResourceId().getAccountId(), ns.getResourceId().getId());

    long before = version(children);
    TestSupport.createTable(
        table,
        cat.getResourceId(),
        ns.getResourceId(),
        "t_split",
        "s3://bucket/t_split",
        "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"int\",\"required\":true}]}",
        "a table");
    TestSupport.createView(
        view, cat.getResourceId(), ns.getResourceId(), "v_split", "SELECT 1", "a view");

    assertEquals(
        before,
        version(children),
        "a table or view is not a child namespace: touching that marker costs every concurrent"
            + " rename its fence");
  }

  /**
   * A namespace with children can still have its properties updated. The guard is on the identity
   * its descendants derive keys from, not on the row being written at all -- and the REST gateway's
   * property update copies the current identity into the spec, so it must not read as a rename.
   */
  @Test
  void updatingPropertiesOfANamespaceWithAChildIsAllowed() {
    var cat = TestSupport.createCatalog(catalog, prefix + "cat_props", "");
    var parent =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "holder", List.of(), "parent namespace");
    TestSupport.createNamespace(
        namespace, cat.getResourceId(), "kept", List.of("holder"), "child namespace");

    namespace.updateNamespace(
        UpdateNamespaceRequest.newBuilder()
            .setNamespaceId(parent.getResourceId())
            .setSpec(
                NamespaceSpec.newBuilder()
                    .setDisplayName("holder")
                    .putProperties("owner", "analytics"))
            .setUpdateMask(FieldMask.newBuilder().addPaths("properties").build())
            .build());

    assertTrue(resolves(cat.getDisplayName(), List.of("holder", "kept")));
  }

  private long version(String pointerKey) {
    return ptr.get(pointerKey).map(p -> p.getVersion()).orElse(0L);
  }
}
