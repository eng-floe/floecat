package ai.floedb.metacat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.metacat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.metacat.common.rpc.ErrorCode;
import ai.floedb.metacat.common.rpc.PageRequest;
import ai.floedb.metacat.service.bootstrap.impl.SeedRunner;
import ai.floedb.metacat.service.util.PagingTestUtil;
import ai.floedb.metacat.service.util.TestDataResetter;
import ai.floedb.metacat.service.util.TestSupport;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CatalogPagingIT {

  @GrpcClient("metacat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  private static final int LIMIT = 10;
  private static final int TOTAL = 25;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
    for (int i = 1; i <= TOTAL; i++) {
      String name = String.format("it-cat-%03d", i);
      TestSupport.createCatalog(catalog, name, "");
    }
  }

  @Test
  void listCatalogsPagingAndTotals_generalHelper() {
    PagingTestUtil.GrpcPager<Catalog> pager =
        (pageSize, token) -> {
          var resp =
              catalog.listCatalogs(
                  ListCatalogsRequest.newBuilder()
                      .setPage(
                          PageRequest.newBuilder()
                              .setPageSize(pageSize)
                              .setPageToken(token == null ? "" : token))
                      .build());
          return new PagingTestUtil.PageChunk<>(
              resp.getCatalogsList(),
              resp.getPage().getNextPageToken(),
              resp.getPage().getTotalSize());
        };

    PagingTestUtil.assertBasicTwoPageFlow(pager, LIMIT);

    var pageAll =
        catalog.listCatalogs(
            ListCatalogsRequest.newBuilder()
                .setPage(PageRequest.newBuilder().setPageSize(1000))
                .build());
    assertTrue(pageAll.getPage().getTotalSize() >= TOTAL);
  }

  @Test
  void listPagingInvalidToken_catalogs() throws Exception {
    var page1 =
        catalog.listCatalogs(
            ListCatalogsRequest.newBuilder()
                .setPage(PageRequest.newBuilder().setPageSize(2).build())
                .build());
    assertEquals(2, page1.getCatalogsCount());
    assertFalse(page1.getPage().getNextPageToken().isBlank());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                catalog.listCatalogs(
                    ListCatalogsRequest.newBuilder()
                        .setPage(
                            PageRequest.newBuilder().setPageSize(2).setPageToken("bogus-token"))
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex,
        Status.Code.INVALID_ARGUMENT,
        ErrorCode.MC_INVALID_ARGUMENT,
        "Invalid page token: bogus-token");
  }
}
