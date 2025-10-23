package ai.floedb.metacat.service.repo.impl;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.impl.InMemoryBlobStore;
import ai.floedb.metacat.service.storage.impl.InMemoryPointerStore;
import ai.floedb.metacat.service.util.TestSupport;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CatalogRepositoryTest {
  @Test
  void putAndGetRoundTrip() {
    var ptr = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var catalogRepo = new CatalogRepository(ptr, blobs);

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
  }

  @Test
  void listCatalogs() {
    var ptr = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var catalogRepo = new CatalogRepository(ptr, blobs);
    var namespaceRepo = new NamespaceRepository(ptr, blobs);

    String tenant = TestSupport.createTenantId(TestSupport.DEFAULT_SEED_TENANT).getId();
    var catRid =
        ResourceId.newBuilder()
            .setTenantId(tenant)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    var cat =
        Catalog.newBuilder()
            .setResourceId(catRid)
            .setDisplayName("sales")
            .setDescription("Sales")
            .build();
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
            .setDisplayName("eu")
            .setDescription("EU namespace")
            .setCatalogId(catRid)
            .build();
    namespaceRepo.create(ns);

    nsRid =
        ResourceId.newBuilder()
            .setTenantId(tenant)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    ns =
        Namespace.newBuilder()
            .setResourceId(nsRid)
            .setDisplayName("us")
            .setDescription("US namespace")
            .setCatalogId(catRid)
            .build();
    namespaceRepo.create(ns);

    var next = new StringBuilder();
    List<Catalog> catalogs = catalogRepo.listByName(tenant, 10, "", next);
    assertEquals(1, catalogs.size());

    var catsPrefix = Keys.catPtr(tenant, "");
    var catKeys = ptr.listPointersByPrefix(catsPrefix, 100, "", new StringBuilder());
    assertEquals(1, catKeys.size());
    assertTrue(catKeys.get(0).key().startsWith(catsPrefix));

    var nsNext = new StringBuilder();
    var nss = namespaceRepo.listByName(catRid, null, 10, "", nsNext);
    assertEquals(2, nss.size());
  }
}
