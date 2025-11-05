package ai.floedb.metacat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.ErrorCode;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.PageRequest;
import ai.floedb.metacat.common.rpc.Precondition;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.bootstrap.impl.SeedRunner;
import ai.floedb.metacat.service.util.TestDataResetter;
import ai.floedb.metacat.service.util.TestSupport;
import ai.floedb.metacat.storage.BlobStore;
import ai.floedb.metacat.storage.PointerStore;
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
class ViewMutationIT {
  @Inject PointerStore ptr;
  @Inject BlobStore blob;

  @GrpcClient("metacat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("metacat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("metacat")
  ViewServiceGrpc.ViewServiceBlockingStub view;

  @GrpcClient("metacat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  String viewPrefix = this.getClass().getSimpleName() + "_";

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void viewRenameUpdateAndDelete() throws Exception {
    var cat = TestSupport.createCatalog(catalog, viewPrefix + "cat", "vcat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "views_ns", List.of("db_view"), "namespace for views");
    var nsId = ns.getResourceId();
    assertEquals(ResourceKind.RK_NAMESPACE, nsId.getKind());
    var nsPath = List.of("db_view", "views_ns");

    var created =
        TestSupport.createView(
            view,
            cat.getResourceId(),
            nsId,
            "recent_orders",
            "SELECT order_id, customer_id FROM sales.core.orders "
                + "WHERE order_date >= current_date - INTERVAL '7' DAY",
            "recent orders view");

    var viewId = created.getResourceId();
    assertEquals(ResourceKind.RK_VIEW, viewId.getKind());

    var resolved =
        directory.resolveView(
            ResolveViewRequest.newBuilder()
                .setRef(
                    NameRef.newBuilder()
                        .setCatalog(cat.getDisplayName())
                        .addAllPath(nsPath)
                        .setName("recent_orders"))
                .build());
    assertEquals(viewId.getId(), resolved.getResourceId().getId());

    var lookup = directory.lookupView(LookupViewRequest.newBuilder().setResourceId(viewId).build());
    assertEquals("recent_orders", lookup.getName().getName());

    var weekly =
        TestSupport.createView(
            view,
            cat.getResourceId(),
            nsId,
            "weekly_summary",
            "SELECT customer_id, count(*) AS weekly_orders "
                + "FROM sales.core.orders GROUP BY customer_id",
            "weekly summary");
    var weeklyId = weekly.getResourceId();

    var listed =
        view.listViews(
            ListViewsRequest.newBuilder()
                .setNamespaceId(nsId)
                .setPage(PageRequest.newBuilder().setPageSize(10).build())
                .build());
    assertEquals(2, listed.getViewsCount());

    var prefixResolved =
        directory.resolveFQViews(
            ResolveFQViewsRequest.newBuilder()
                .setPrefix(NameRef.newBuilder().setCatalog(cat.getDisplayName()).addAllPath(nsPath))
                .setPage(PageRequest.newBuilder().setPageSize(10).build())
                .build());
    assertEquals(2, prefixResolved.getViewsCount());

    var listResolved =
        directory.resolveFQViews(
            ResolveFQViewsRequest.newBuilder()
                .setList(
                    NameList.newBuilder()
                        .addNames(
                            NameRef.newBuilder()
                                .setCatalog(cat.getDisplayName())
                                .addAllPath(nsPath)
                                .setName("recent_orders"))
                        .addNames(
                            NameRef.newBuilder()
                                .setCatalog(cat.getDisplayName())
                                .addAllPath(nsPath)
                                .setName("weekly_summary")))
                .setPage(PageRequest.newBuilder().setPageSize(10).build())
                .build());
    assertEquals(2, listResolved.getViewsCount());

    var beforeRename = TestSupport.metaForView(ptr, blob, viewId);
    FieldMask mask = FieldMask.newBuilder().addPaths("display_name").build();
    var renameResp =
        view.updateView(
            UpdateViewRequest.newBuilder()
                .setViewId(viewId)
                .setSpec(ViewSpec.newBuilder().setDisplayName("recent_orders_v2").build())
                .setUpdateMask(mask)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(beforeRename.getPointerVersion())
                        .setExpectedEtag(beforeRename.getEtag())
                        .build())
                .build());
    assertEquals("recent_orders_v2", renameResp.getView().getDisplayName());
    assertTrue(renameResp.getMeta().getPointerVersion() > beforeRename.getPointerVersion());

    var resolveRenamed =
        directory.resolveView(
            ResolveViewRequest.newBuilder()
                .setRef(
                    NameRef.newBuilder()
                        .setCatalog(cat.getDisplayName())
                        .addAllPath(nsPath)
                        .setName("recent_orders_v2"))
                .build());
    assertEquals(viewId.getId(), resolveRenamed.getResourceId().getId());

    var lookupRenamed =
        directory.lookupView(LookupViewRequest.newBuilder().setResourceId(viewId).build());
    assertEquals("recent_orders_v2", lookupRenamed.getName().getName());

    var nfOldName =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                directory.resolveView(
                    ResolveViewRequest.newBuilder()
                        .setRef(
                            NameRef.newBuilder()
                                .setCatalog(cat.getDisplayName())
                                .addAllPath(nsPath)
                                .setName("recent_orders"))
                        .build()));
    TestSupport.assertGrpcAndMc(
        nfOldName, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "not found");

    var staleRename =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                view.updateView(
                    UpdateViewRequest.newBuilder()
                        .setViewId(viewId)
                        .setSpec(ViewSpec.newBuilder().setDisplayName("recent_orders_v3").build())
                        .setUpdateMask(mask)
                        .setPrecondition(
                            Precondition.newBuilder()
                                .setExpectedVersion(beforeRename.getPointerVersion())
                                .setExpectedEtag(beforeRename.getEtag())
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        staleRename, Status.Code.FAILED_PRECONDITION, ErrorCode.MC_PRECONDITION_FAILED, "mismatch");

    var beforeSql = TestSupport.metaForView(ptr, blob, viewId);
    FieldMask mask_sql = FieldMask.newBuilder().addPaths("sql").build();
    var sqlUpdate =
        view.updateView(
            UpdateViewRequest.newBuilder()
                .setViewId(viewId)
                .setSpec(
                    ViewSpec.newBuilder()
                        .setSql(
                            "SELECT order_id, customer_id, total_price FROM sales.core.orders "
                                + "WHERE order_date >= current_date - INTERVAL '14' DAY")
                        .build())
                .setUpdateMask(mask_sql)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(beforeSql.getPointerVersion())
                        .setExpectedEtag(beforeSql.getEtag())
                        .build())
                .build());
    assertEquals(
        "SELECT order_id, customer_id, total_price FROM sales.core.orders WHERE order_date >="
            + " current_date - INTERVAL '14' DAY",
        sqlUpdate.getView().getSql());
    assertTrue(sqlUpdate.getMeta().getPointerVersion() > beforeSql.getPointerVersion());

    var staleSql =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                view.updateView(
                    UpdateViewRequest.newBuilder()
                        .setViewId(viewId)
                        .setSpec(ViewSpec.newBuilder().setSql("SELECT 1").build())
                        .setUpdateMask(mask_sql)
                        .setPrecondition(
                            Precondition.newBuilder()
                                .setExpectedVersion(beforeSql.getPointerVersion())
                                .setExpectedEtag(beforeSql.getEtag())
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        staleSql, Status.Code.FAILED_PRECONDITION, ErrorCode.MC_PRECONDITION_FAILED, "mismatch");

    var beforeNoop = TestSupport.metaForView(ptr, blob, viewId);
    FieldMask mask_all = FieldMask.newBuilder().addAllPaths(List.of("sql", "display_name")).build();
    var noop =
        view.updateView(
            UpdateViewRequest.newBuilder()
                .setViewId(viewId)
                .setSpec(
                    ViewSpec.newBuilder()
                        .setDisplayName("recent_orders_v2")
                        .setSql(
                            "SELECT order_id, customer_id, total_price FROM sales.core.orders "
                                + "WHERE order_date >= current_date - INTERVAL '14' DAY")
                        .build())
                .setUpdateMask(mask_all)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(beforeNoop.getPointerVersion())
                        .setExpectedEtag(beforeNoop.getEtag())
                        .build())
                .build());
    assertEquals(beforeNoop.getPointerVersion(), noop.getMeta().getPointerVersion());
    assertEquals(beforeNoop.getEtag(), noop.getMeta().getEtag());

    var beforeDelete = TestSupport.metaForView(ptr, blob, viewId);
    view.deleteView(
        DeleteViewRequest.newBuilder()
            .setViewId(viewId)
            .setPrecondition(
                Precondition.newBuilder()
                    .setExpectedVersion(beforeDelete.getPointerVersion())
                    .setExpectedEtag(beforeDelete.getEtag())
                    .build())
            .build());

    var notFound =
        assertThrows(
            StatusRuntimeException.class,
            () -> view.getView(GetViewRequest.newBuilder().setViewId(viewId).build()));
    TestSupport.assertGrpcAndMc(
        notFound, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "not found");

    var dirNotFound =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                directory.resolveView(
                    ResolveViewRequest.newBuilder()
                        .setRef(
                            NameRef.newBuilder()
                                .setCatalog(cat.getDisplayName())
                                .addAllPath(nsPath)
                                .setName("recent_orders_v2"))
                        .build()));
    TestSupport.assertGrpcAndMc(
        dirNotFound, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "not found");

    view.deleteView(DeleteViewRequest.newBuilder().setViewId(weeklyId).build());

    var emptyList =
        view.listViews(
            ListViewsRequest.newBuilder()
                .setNamespaceId(nsId)
                .setPage(PageRequest.newBuilder().setPageSize(10).build())
                .build());
    assertEquals(0, emptyList.getViewsCount());
  }
}
