package ai.floedb.floecat.service.repo.impl;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.InMemoryBlobStore;
import ai.floedb.floecat.storage.InMemoryPointerStore;
import ai.floedb.floecat.storage.PointerStore;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalogRepositoryTest {
  private CatalogRepository catalogRepo;
  private PointerStore ptr;
  private BlobStore blobs;

  @BeforeEach
  void setUp() {
    ptr = new InMemoryPointerStore();
    blobs = new InMemoryBlobStore();
    catalogRepo = new CatalogRepository(ptr, blobs);
  }

  @Test
  void putAndGetRoundTrip() {
    String tenant = TestSupport.createTenantId(TestSupport.DEFAULT_SEED_TENANT).getId();
    var rid =
        ResourceId.newBuilder()
            .setTenantId(tenant)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    var cat =
        Catalog.newBuilder()
            .setResourceId(rid)
            .setDisplayName("sales")
            .setDescription("Sales")
            .build();

    catalogRepo.create(cat);
    var fetched = catalogRepo.getById(rid).orElseThrow();
    assertEquals("sales", fetched.getDisplayName());
    fetched = catalogRepo.getByName(tenant, "sales").orElseThrow();
    assertEquals(rid, fetched.getResourceId());

    catalogRepo.delete(rid);
    fetched = catalogRepo.getByName(tenant, "sales").orElse(null);
    assertNull(fetched);
  }

  @Test
  void listCatalogs() {
    var ptr = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var catalogRepo = new CatalogRepository(ptr, blobs);

    String tenant = TestSupport.createTenantId(TestSupport.DEFAULT_SEED_TENANT).getId();

    var cat1Rid =
        ResourceId.newBuilder()
            .setTenantId(tenant)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    var cat1 =
        Catalog.newBuilder()
            .setResourceId(cat1Rid)
            .setDisplayName("sales")
            .setDescription("Sales")
            .build();
    catalogRepo.create(cat1);

    var cat2Rid =
        ResourceId.newBuilder()
            .setTenantId(tenant)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    var cat2 =
        Catalog.newBuilder()
            .setResourceId(cat2Rid)
            .setDisplayName("orders")
            .setDescription("orders")
            .build();
    catalogRepo.create(cat2);

    var cat3Rid =
        ResourceId.newBuilder()
            .setTenantId(tenant)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    var cat3 =
        Catalog.newBuilder()
            .setResourceId(cat3Rid)
            .setDisplayName("lineitem")
            .setDescription("lineitem")
            .build();
    catalogRepo.create(cat3);

    List<Catalog> catalogs = catalogRepo.list(tenant, Integer.MAX_VALUE, null, null);
    assertEquals(3, catalogs.size());

    String token = "";
    StringBuilder next = new StringBuilder();
    List<Catalog> batch = catalogRepo.list(tenant, 1, token, next);
    assertEquals("lineitem", batch.get(0).getDisplayName());
    token = next.toString();
    next.setLength(0);
    batch = catalogRepo.list(tenant, 1, token, next);
    assertEquals("orders", batch.get(0).getDisplayName());
    token = next.toString();
    next.setLength(0);
    batch = catalogRepo.list(tenant, 1, token, next);
    assertEquals("sales", batch.get(0).getDisplayName());
    token = next.toString();
    next.setLength(0);
    assertTrue(token.isEmpty());
  }
}
