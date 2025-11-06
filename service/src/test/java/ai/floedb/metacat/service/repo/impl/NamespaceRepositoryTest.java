package ai.floedb.metacat.service.repo.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.util.TestSupport;
import ai.floedb.metacat.storage.BlobStore;
import ai.floedb.metacat.storage.InMemoryBlobStore;
import ai.floedb.metacat.storage.InMemoryPointerStore;
import ai.floedb.metacat.storage.PointerStore;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NamespaceRepositoryTest {

  private CatalogRepository catalogRepo;
  private NamespaceRepository namespaceRepo;
  private PointerStore ptr;
  private BlobStore blobs;

  @BeforeEach
  void setUp() {
    ptr = new InMemoryPointerStore();
    blobs = new InMemoryBlobStore();
    catalogRepo = new CatalogRepository(ptr, blobs);
    namespaceRepo = new NamespaceRepository(ptr, blobs);
  }

  @Test
  void putAndGetRoundTrip() {
    String tenant = TestSupport.createTenantId(TestSupport.DEFAULT_SEED_TENANT).getId();
    var catRid =
        ResourceId.newBuilder()
            .setTenantId(tenant)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_CATALOG)
            .build();

    Catalog cat = Catalog.newBuilder().setResourceId(catRid).setDisplayName("sales").build();
    catalogRepo.create(cat);

    var nsRid =
        ResourceId.newBuilder()
            .setTenantId(tenant)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();

    var ns =
        Namespace.newBuilder()
            .setResourceId(nsRid)
            .setDisplayName("core")
            .setDescription("Core namespace")
            .setCatalogId(catRid)
            .build();
    namespaceRepo.create(ns);

    var fetched = namespaceRepo.getById(nsRid).orElseThrow();
    assertEquals("core", fetched.getDisplayName());
  }
}
