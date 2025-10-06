package ai.floedb.metacat.service.bootstrap.impl;

import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.directory.impl.DirectoryServiceImpl;

@ApplicationScoped
public class SeedRunner {
  private static final Logger LOG = Logger.getLogger(SeedRunner.class);

  @Inject CatalogRepository repo;

  void onStart(@jakarta.enterprise.event.Observes io.quarkus.runtime.StartupEvent ev) {
    String tenant = "t-0001";
    seedCatalog(tenant, "sales", "Sales catalog");
    seedCatalog(tenant, "finance", "Finance catalog");
    DirectoryServiceImpl.putNamespaceIndex("sales.core", UUID.nameUUIDFromBytes("t-0001/sales.core".getBytes()).toString());
    DirectoryServiceImpl.putNamespaceIndex("finance.core", UUID.nameUUIDFromBytes("t-0001/finance.core".getBytes()).toString());
    LOG.info("Seeded catalogs for tenant " + tenant);
  }

  private void seedCatalog(String tenant, String name, String desc) {
    String id = UUID.nameUUIDFromBytes((tenant+"/"+name).getBytes()).toString();
    var rid = ResourceId.newBuilder().setTenantId(tenant).setId(id).setKind(ResourceKind.RK_CATALOG).build();
    var cat = Catalog.newBuilder().setResourceId(rid).setDisplayName(name).setDescription(desc).setCreatedAtMs(System.currentTimeMillis()).build();
    repo.putCatalog(cat);
    DirectoryServiceImpl.putIndex(name, id);
  }
}