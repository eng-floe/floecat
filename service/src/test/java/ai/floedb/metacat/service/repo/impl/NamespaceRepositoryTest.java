package ai.floedb.metacat.service.repo.impl;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.storage.impl.InMemoryBlobStore;
import ai.floedb.metacat.service.storage.impl.InMemoryPointerStore;

class NamespaceRepositoryTest {

  @Test
  void putAndGetRoundTrip() {
    var repo = new NamespaceRepository(new InMemoryPointerStore(), new InMemoryBlobStore());

    String tenant = "t-0001";

    var catRid = ResourceId.newBuilder()
      .setTenantId(tenant)
      .setId(UUID.randomUUID().toString())
      .setKind(ResourceKind.RK_CATALOG)
      .build();

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

    repo.put(ns, catRid);

    var fetched = repo.get(nsRid, catRid).orElseThrow();
    assertEquals("core", fetched.getDisplayName());
  }
}