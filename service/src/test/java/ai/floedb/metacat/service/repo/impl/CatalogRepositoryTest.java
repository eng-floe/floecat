package ai.floedb.metacat.service.repo.impl;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.storage.impl.InMemoryBlobStore;
import ai.floedb.metacat.service.storage.impl.InMemoryPointerStore;

class CatalogRepositoryTest {
  @Test
  void putAndGetRoundTrip() {
    var repo = new CatalogRepository(new InMemoryPointerStore(), new InMemoryBlobStore());

    var rid = ResourceId.newBuilder()
      .setTenantId("t-0001")
      .setId(UUID.randomUUID().toString())
      .setKind(ResourceKind.RK_CATALOG).build();
    var cat = Catalog.newBuilder()
      .setResourceId(rid).setDisplayName("sales").setDescription("Sales").build();

    repo.put(cat);
    var fetched = repo.get(rid).orElseThrow();
    assertEquals("sales", fetched.getDisplayName());
  }
}