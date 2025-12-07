package ai.floedb.floecat.service.repo.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.InMemoryBlobStore;
import ai.floedb.floecat.storage.InMemoryPointerStore;
import ai.floedb.floecat.storage.PointerStore;
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
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var catRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_CATALOG)
            .build();

    Catalog cat = Catalog.newBuilder().setResourceId(catRid).setDisplayName("sales").build();
    catalogRepo.create(cat);

    var nsRid =
        ResourceId.newBuilder()
            .setAccountId(account)
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
