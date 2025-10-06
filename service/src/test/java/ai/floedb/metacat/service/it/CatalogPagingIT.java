package ai.floedb.metacat.service.it;

import java.util.UUID;

import io.grpc.ClientInterceptor;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.metacat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.metacat.common.rpc.PrincipalContext;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CatalogPagingIT {
  @Inject CatalogRepository repo;
  @GrpcClient("catalog")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  private static final String TENANT = "t-it-0001";
  private static final int LIMIT = 10;
  private static final int TOTAL = 13; // > LIMIT, leaves TOTAL-LIMIT on page 2

  @BeforeAll
  void seedAndClient() {
    for (int i = 1; i <= TOTAL; i++) {
      String name = String.format("it-cat-%03d", i);
      String id = UUID.nameUUIDFromBytes((TENANT + "/" + name).getBytes()).toString();

      var rid = ResourceId.newBuilder()
          .setTenantId(TENANT)
          .setId(id)
          .setKind(ResourceKind.RK_CATALOG)
          .build();

      var cat = Catalog.newBuilder()
          .setResourceId(rid)
          .setDisplayName(name)
          .setDescription("paging test")
          .setCreatedAtMs(System.currentTimeMillis())
          .build();

      repo.putCatalog(cat);
    }

    // Build a stub that carries a PrincipalContext for TENANT
    var pc = PrincipalContext.newBuilder()
        .setTenantId(TENANT)
        .setSubject("it-user")
        .addPermissions("catalog.read")
        .build();

    // If your server interceptor expects binary header:
    Metadata headers = new Metadata();
    Metadata.Key<byte[]> PRINC_BIN =
        Metadata.Key.of("x-principal-bin", Metadata.BINARY_BYTE_MARSHALLER);
    headers.put(PRINC_BIN, pc.toByteArray());

    ClientInterceptor attach = MetadataUtils.newAttachHeadersInterceptor(headers);
    this.catalog = catalog.withInterceptors(attach);
  }

  @Test
  void listCatalogs_pagingAndTotals() {
    // Page 1
    var page1Req = ListCatalogsRequest.newBuilder()
        .setPage(ai.floedb.metacat.common.rpc.PageRequest.newBuilder()
            .setPageSize(LIMIT))
        .build();

    var page1 = catalog.listCatalogs(page1Req);
    assertEquals(LIMIT, page1.getCatalogsCount(), "first page should return LIMIT items");
    assertFalse(page1.getPage().getNextPageToken().isEmpty(), "next_page_token should be set");
    assertEquals(TOTAL, page1.getPage().getTotalSize(), "total_size should be TOTAL");

    // Page 2
    var page2Req = ListCatalogsRequest.newBuilder()
        .setPage(ai.floedb.metacat.common.rpc.PageRequest.newBuilder()
            .setPageSize(LIMIT)
            .setPageToken(page1.getPage().getNextPageToken()))
        .build();

    var page2 = catalog.listCatalogs(page2Req);
    assertEquals(TOTAL - LIMIT, page2.getCatalogsCount(), "second page should have the remainder");
    assertTrue(page2.getPage().getNextPageToken().isEmpty(), "no further pages expected");
    assertEquals(TOTAL, page2.getPage().getTotalSize(), "total_size should remain TOTAL across pages");
  }
}