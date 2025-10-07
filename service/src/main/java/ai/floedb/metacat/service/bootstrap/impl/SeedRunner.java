package ai.floedb.metacat.service.bootstrap.impl;

import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.NamespaceRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.catalog.impl.DirectoryImpl;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.NamespaceRepository;

@ApplicationScoped
public class SeedRunner {
  @Inject CatalogRepository catalogs;
  @Inject NamespaceRepository namespaces;

  private static final Logger LOG = Logger.getLogger(SeedRunner.class.getName());

  void onStart(@Observes StartupEvent ev) {
    final String tenant = "t-0001";
    final long now = System.currentTimeMillis();

    var salesId = seedCatalog(tenant, "sales", "Sales catalog",   now);
    var financeId = seedCatalog(tenant, "finance", "Finance catalog", now);
    DirectoryImpl.putIndex("sales", salesId);
    DirectoryImpl.putIndex("finance", financeId);

    seedNamespace(tenant, salesId, List.of("core"), "core", now);
    seedNamespace(tenant, salesId, List.of("staging","2025"), "staging/2025", now);
    seedNamespace(tenant, financeId, List.of("core"), "core", now);

    LOG.info("Seeded catalogs and namespaces for tenant " + tenant);
  }

  private String seedCatalog(String tenant, String displayName, String description, long now) {
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
      .setCreatedAtMs(now)
      .build();

    catalogs.putCatalog(cat);
    return id;
  }

  private void seedNamespace(String tenant, String catalogId, List<String> path, String display, long now) {
    String nsId = uuidFor(tenant + "/ns:" + displayPathKey(catalogId, path));

    var catRid = ResourceId.newBuilder()
      .setTenantId(tenant)
      .setId(catalogId)
      .setKind(ResourceKind.RK_CATALOG)
      .build();

    var nsRid = ResourceId.newBuilder()
      .setTenantId(tenant)
      .setId(nsId)
      .setKind(ResourceKind.RK_NAMESPACE)
      .build();

    Namespace ns = Namespace.newBuilder()
      .setResourceId(nsRid)
      .setDisplayName(display)
      .setDescription(display + " namespace")
      .setCreatedAtMs(now)
      .build();

    namespaces.put(ns, catRid);

    NamespaceRef ref = NamespaceRef.newBuilder()
    .setCatalogId(ResourceId.newBuilder()
        .setTenantId(tenant)
        .setId(catalogId)
        .setKind(ResourceKind.RK_CATALOG)
        .build())
    .addAllNamespacePath(path)
    .build();

    DirectoryImpl.putNamespaceIndex(ref, nsId);
  }

  private static String uuidFor(String seed) {
    return UUID.nameUUIDFromBytes(seed.getBytes()).toString();
  }

  private static String displayPathKey(String catalogId, List<String> path) {
    return catalogId + "/" + String.join("/", path);
  }
}