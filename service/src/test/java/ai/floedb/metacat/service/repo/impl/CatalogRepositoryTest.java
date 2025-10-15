package ai.floedb.metacat.service.repo.impl;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.impl.InMemoryBlobStore;
import ai.floedb.metacat.service.storage.impl.InMemoryPointerStore;

class CatalogRepositoryTest {
  @Test
  void putAndGetRoundTrip() {
    var ptr = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var nameIndexRepo = new NameIndexRepository(ptr, blobs);
    var catalogRepo = new CatalogRepository(nameIndexRepo, ptr, blobs);

    var rid = ResourceId.newBuilder()
        .setTenantId("t-0001")
        .setId(UUID.randomUUID().toString())
        .setKind(ResourceKind.RK_CATALOG).build();
    var cat = Catalog.newBuilder()
        .setResourceId(rid).setDisplayName("sales").setDescription("Sales").build();

    catalogRepo.put(cat);
    var fetched = catalogRepo.get(rid).orElseThrow();
    assertEquals("sales", fetched.getDisplayName());
  }

  @Test
  void listCatalogs() {
    var ptr = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var nameIndexRepo = new NameIndexRepository(ptr, blobs);
    var catalogRepo = new CatalogRepository(nameIndexRepo, ptr, blobs);
    var namespaceRepo = new NamespaceRepository(nameIndexRepo, ptr, blobs);

    String tenant = "t-0001";
    var catRid = ResourceId.newBuilder()
        .setTenantId(tenant)
        .setId(UUID.randomUUID().toString())
        .setKind(ResourceKind.RK_CATALOG).build();
    var cat = Catalog.newBuilder()
        .setResourceId(catRid).setDisplayName("sales").setDescription("Sales").build();
    catalogRepo.put(cat);

    var nsRid = ResourceId.newBuilder()
      .setTenantId(tenant).setId(UUID.randomUUID().toString())
      .setKind(ResourceKind.RK_NAMESPACE).build();
    var ns = Namespace.newBuilder()
      .setResourceId(nsRid).setDisplayName("eu")
      .setDescription("EU namespace").build();
    namespaceRepo.put(ns, catRid, null);

    nsRid = ResourceId.newBuilder()
      .setTenantId(tenant).setId(UUID.randomUUID().toString())
      .setKind(ResourceKind.RK_NAMESPACE).build();
    ns = Namespace.newBuilder()
      .setResourceId(nsRid).setDisplayName("us")
      .setDescription("US namespace").build();
    namespaceRepo.put(ns, catRid, null);

    var next = new StringBuilder();
    List<NameRef> catalogs = catalogRepo.list(tenant, 10, "", next);
    assertEquals(1, catalogs.size());

    var catsPrefix = Keys.catPtr(tenant, "");
    var catKeys = ptr.listPointersByPrefix(catsPrefix, 100, "", new StringBuilder());
    assertEquals(1, catKeys.size());
    assertTrue(catKeys.get(0).key().startsWith(catsPrefix));

    var nsNext = new StringBuilder();
    var nss = namespaceRepo.list(catRid, null, 10, "", nsNext);
    assertEquals(2, nss.size());
  }
}
