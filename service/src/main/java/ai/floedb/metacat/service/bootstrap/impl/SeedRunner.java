package ai.floedb.metacat.service.bootstrap.impl;

import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.service.directory.impl.DirectoryServiceImpl;
import ai.floedb.metacat.catalog.rpc.NamespaceRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;

@ApplicationScoped
public class SeedRunner {
  private static final Logger LOG = Logger.getLogger(SeedRunner.class.getName());

  @Inject CatalogRepository repo;

  void onStart(@Observes StartupEvent ev) {
    final String tenant = "t-0001";

    seedCatalog(tenant, "sales", "Sales catalog");
    seedCatalog(tenant, "finance", "Finance catalog");

    DirectoryServiceImpl.putIndex("sales", uuidFor(tenant + "/catalog:sales"));
    DirectoryServiceImpl.putIndex("finance", uuidFor(tenant + "/catalog:finance"));

    putNs(tenant,
      uuidFor(tenant + "/catalog:sales"),
      List.of("core"),
      uuidFor(tenant + "/ns:sales/core"));

    putNs(tenant,
      uuidFor(tenant + "/catalog:sales"),
      List.of("staging", "2025"),
      uuidFor(tenant + "/ns:sales/staging/2025"));

    putNs(tenant,
        uuidFor(tenant + "/catalog:finance"),
        List.of("core"),
        uuidFor(tenant + "/ns:finance/core"));

    LOG.info("Seeded catalogs and namespaces for tenant " + tenant);
  }

  private void seedCatalog(String tenant, String displayName, String description) {
    String id = uuidFor(tenant + "/catalog:" + displayName);
    ResourceId rid = ResourceId.newBuilder()
      .setTenantId(tenant)
      .setId(id)
      .setKind(ResourceKind.RK_CATALOG)
      .build();

    Catalog cat = Catalog.newBuilder()
      .setResourceId(rid)
      .setDisplayName(displayName)
      .setDescription(description)
      .setCreatedAtMs(System.currentTimeMillis())
      .build();

    repo.putCatalog(cat);
  }

  private static void putNs(String tenant, String catalogId, List<String> path, String nsId) {
    NamespaceRef ref = NamespaceRef.newBuilder()
      .setCatalogId(ResourceId.newBuilder()
        .setTenantId(tenant)
        .setId(catalogId)
        .setKind(ResourceKind.RK_CATALOG)
        .build())
      .addAllNamespacePath(path)
      .build();

    DirectoryServiceImpl.putNamespaceIndex(ref, nsId);
  }

  private static String uuidFor(String seed) {
    return UUID.nameUUIDFromBytes(seed.getBytes()).toString();
  }
}