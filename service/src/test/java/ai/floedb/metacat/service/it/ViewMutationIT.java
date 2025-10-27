package ai.floedb.metacat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.ErrorCode;
import ai.floedb.metacat.common.rpc.PageRequest;
import ai.floedb.metacat.common.rpc.Precondition;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.util.TestSupport;
import ai.floedb.metacat.storage.BlobStore;
import ai.floedb.metacat.storage.PointerStore;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.Test;

@QuarkusTest
class ViewMutationIT {
  @Inject PointerStore ptr;
  @Inject BlobStore blob;

  @GrpcClient("catalog-service")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("namespace-service")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("view-service")
  ViewServiceGrpc.ViewServiceBlockingStub view;

  String viewPrefix = this.getClass().getSimpleName() + "_";

  @Test
  void viewRenameUpdateAndDelete() throws Exception {
    var cat = TestSupport.createCatalog(catalog, viewPrefix + "cat", "vcat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "views_ns", List.of("db_view"), "namespace for views");
    var nsId = ns.getResourceId();
    assertEquals(ResourceKind.RK_NAMESPACE, nsId.getKind());

    var created =
      TestSupport.createView(
          view,
          cat.getResourceId(),
          nsId,
          "recent_orders",
          "SELECT order_id, customer_id FROM sales.core.orders WHERE order_date >= current_date - INTERVAL '7' DAY",
          "recent orders view");

    var viewId = created.getResourceId();
    assertEquals(ResourceKind.RK_VIEW, viewId.getKind());

    var listed =
        view.listViews(
            ListViewsRequest.newBuilder()
                .setNamespaceId(nsId)
                .setPage(PageRequest.newBuilder().setPageSize(10).build())
                .build());
    assertEquals(1, listed.getViewsCount());
    assertEquals("recent_orders", listed.getViews(0).getDisplayName());

    var beforeRename = TestSupport.metaForView(ptr, blob, viewId);
    var renameResp =
        view.updateView(
            UpdateViewRequest.newBuilder()
                .setViewId(viewId)
                .setSpec(ViewSpec.newBuilder().setDisplayName("recent_orders_v2").build())
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(beforeRename.getPointerVersion())
                        .setExpectedEtag(beforeRename.getEtag())
                        .build())
                .build());
    assertEquals("recent_orders_v2", renameResp.getView().getDisplayName());
    assertTrue(renameResp.getMeta().getPointerVersion() > beforeRename.getPointerVersion());

    var staleRename =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                view.updateView(
                    UpdateViewRequest.newBuilder()
                        .setViewId(viewId)
                        .setSpec(ViewSpec.newBuilder().setDisplayName("recent_orders_v3").build())
                        .setPrecondition(
                            Precondition.newBuilder()
                                .setExpectedVersion(beforeRename.getPointerVersion())
                                .setExpectedEtag(beforeRename.getEtag())
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        staleRename,
        Status.Code.FAILED_PRECONDITION,
        ErrorCode.MC_PRECONDITION_FAILED,
        "mismatch");

    var beforeSql = TestSupport.metaForView(ptr, blob, viewId);
    var sqlUpdate =
        view.updateView(
            UpdateViewRequest.newBuilder()
                .setViewId(viewId)
                .setSpec(
                    ViewSpec.newBuilder()
                        .setSql(
                            "SELECT order_id, customer_id, total_price FROM sales.core.orders WHERE order_date >= current_date - INTERVAL '14' DAY")
                        .build())
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(beforeSql.getPointerVersion())
                        .setExpectedEtag(beforeSql.getEtag())
                        .build())
                .build());
    assertEquals(
        "SELECT order_id, customer_id, total_price FROM sales.core.orders WHERE order_date >= current_date - INTERVAL '14' DAY",
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
                        .setPrecondition(
                            Precondition.newBuilder()
                                .setExpectedVersion(beforeSql.getPointerVersion())
                                .setExpectedEtag(beforeSql.getEtag())
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        staleSql,
        Status.Code.FAILED_PRECONDITION,
        ErrorCode.MC_PRECONDITION_FAILED,
        "mismatch");

    var beforeNoop = TestSupport.metaForView(ptr, blob, viewId);
    var noop =
        view.updateView(
            UpdateViewRequest.newBuilder()
                .setViewId(viewId)
                .setSpec(
                    ViewSpec.newBuilder()
                        .setDisplayName("recent_orders_v2")
                        .setSql(
                            "SELECT order_id, customer_id, total_price FROM sales.core.orders WHERE order_date >= current_date - INTERVAL '14' DAY")
                        .build())
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

    var emptyList =
        view.listViews(
            ListViewsRequest.newBuilder()
                .setNamespaceId(nsId)
                .setPage(PageRequest.newBuilder().setPageSize(10).build())
                .build());
    assertEquals(0, emptyList.getViewsCount());
  }
}

