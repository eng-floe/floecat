package ai.floedb.metacat.service.repo.impl;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.storage.impl.InMemoryBlobStore;
import ai.floedb.metacat.service.storage.impl.InMemoryPointerStore;

class NamespaceRepositoryTest {

  @Test
  void putAndGetRoundTrip() {
    var ptr = new InMemoryPointerStore();
    var blob = new InMemoryBlobStore();
    var nameIndexRepo = new NameIndexRepository(ptr, blob);
    var repo = new NamespaceRepository(nameIndexRepo, ptr, blob);
    var catRepo = new CatalogRepository(nameIndexRepo, ptr, blob);

    String tenant = "t-0001";

    var catRid = ResourceId.newBuilder()
        .setTenantId(tenant)
        .setId(UUID.randomUUID().toString())
        .setKind(ResourceKind.RK_CATALOG)
        .build();

    Catalog cat = Catalog.newBuilder()
        .setResourceId(catRid)
        .setDisplayName("sales")
        .build();
    catRepo.put(cat);

    var nsRid = ResourceId.newBuilder()
        .setTenantId(tenant)
        .setId(UUID.randomUUID().toString())
        .setKind(ResourceKind.RK_NAMESPACE)
        .build();

    var ns = Namespace.newBuilder()
        .setResourceId(nsRid)
        .setDisplayName("core")
        .setDescription("Core namespace")
        .build();
    repo.put(ns, catRid, null);

    var fetched = repo.get(nsRid).orElseThrow();
    assertEquals("core", fetched.getDisplayName());
  }
}
