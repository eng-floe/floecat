package ai.floedb.metacat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.metacat.catalog.rpc.ResourceAccessGrpc;
import ai.floedb.metacat.common.rpc.PageRequest;
import ai.floedb.metacat.common.rpc.PrincipalContext;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.util.TestSupport;
import com.google.protobuf.util.Timestamps;
import io.grpc.ClientInterceptor;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.UUID;
import org.junit.jupiter.api.*;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CatalogPagingIT {
  @Inject CatalogRepository repo;

  @GrpcClient("resource-access")
  ResourceAccessGrpc.ResourceAccessBlockingStub resourceAccess;

  private static final String TENANT = "t-0001";
  private static final int LIMIT = 10;
  private static final int TOTAL = 25;

  private final Clock clock = Clock.systemUTC();

  @BeforeAll
  void seedAndClient() {
    var tenantId = TestSupport.createTenantId(TENANT);

    for (int i = 1; i <= TOTAL; i++) {
      String name = String.format("it-cat-%03d", i);
      String cid = UUID.nameUUIDFromBytes((TENANT + "/" + name).getBytes()).toString();

      var rid =
          ResourceId.newBuilder()
              .setTenantId(tenantId.getId())
              .setId(cid)
              .setKind(ResourceKind.RK_CATALOG)
              .build();

      var cat =
          Catalog.newBuilder()
              .setResourceId(rid)
              .setDisplayName(name)
              .setDescription("paging test")
              .setCreatedAt(Timestamps.fromMillis(clock.millis()))
              .build();

      repo.create(cat);
    }

    var pc =
        PrincipalContext.newBuilder()
            .setTenantId(tenantId)
            .setSubject("it-user")
            .addPermissions("catalog.read")
            .build();

    Metadata headers = new Metadata();
    Metadata.Key<byte[]> pincipalContextBytes =
        Metadata.Key.of("x-principal-bin", Metadata.BINARY_BYTE_MARSHALLER);
    headers.put(pincipalContextBytes, pc.toByteArray());

    ClientInterceptor attach = MetadataUtils.newAttachHeadersInterceptor(headers);
    this.resourceAccess = resourceAccess.withInterceptors(attach);
  }

  @Test
  void listCatalogsPagingAndTotals() {
    var pageAllReq =
        ListCatalogsRequest.newBuilder()
            .setPage(PageRequest.newBuilder().setPageSize(1000))
            .build();
    var pageAll = resourceAccess.listCatalogs(pageAllReq);
    var total = pageAll.getPage().getTotalSize();
    assertTrue(total >= TOTAL);

    var page1Req =
        ListCatalogsRequest.newBuilder()
            .setPage(PageRequest.newBuilder().setPageSize(LIMIT))
            .build();

    var page1 = resourceAccess.listCatalogs(page1Req);
    assertEquals(LIMIT, page1.getCatalogsCount(), "first page should return LIMIT items");
    assertFalse(page1.getPage().getNextPageToken().isEmpty(), "next_page_token should be set");
    assertEquals(total, page1.getPage().getTotalSize(), "total_size should be TOTAL");

    var page2Req =
        ListCatalogsRequest.newBuilder()
            .setPage(
                PageRequest.newBuilder()
                    .setPageSize(LIMIT)
                    .setPageToken(page1.getPage().getNextPageToken()))
            .build();

    var page2 = resourceAccess.listCatalogs(page2Req);
    assertEquals(LIMIT, page2.getCatalogsCount(), "second page should have the remainder");
    assertEquals(
        total, page2.getPage().getTotalSize(), "total_size should remain TOTAL across pages");
  }
}
