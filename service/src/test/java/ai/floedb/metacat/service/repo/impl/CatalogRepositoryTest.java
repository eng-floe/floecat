package ai.floedb.metacat.service.repo.impl;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.storage.impl.InMemoryBlobStore;
import ai.floedb.metacat.service.storage.impl.InMemoryPointerStore;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class CatalogRepositoryTest {
  @Test
  void putAndGetRoundTrip() {
    var repo = new CatalogRepository();
    repo.ptr = new InMemoryPointerStore();
    repo.blobs = new InMemoryBlobStore();

    var rid = ResourceId.newBuilder()
        .setTenantId("t-0001")
        .setId(UUID.randomUUID().toString())
        .setKind(ResourceKind.RK_CATALOG).build();
    var cat = Catalog.newBuilder()
        .setResourceId(rid).setDisplayName("sales").setDescription("Sales").build();

    repo.putCatalog(cat);
    var fetched = repo.getCatalog(rid).orElseThrow();
    assertEquals("sales", fetched.getDisplayName());
  }
}