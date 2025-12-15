package ai.floedb.floecat.service.cdi;

import ai.floedb.floecat.metagraph.hint.EngineHintProvider;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.hint.SystemCatalogHintProvider;
import ai.floedb.floecat.systemcatalog.provider.ServiceLoaderSystemCatalogProvider;
import ai.floedb.floecat.systemcatalog.provider.SystemCatalogProvider;
import ai.floedb.floecat.systemcatalog.provider.SystemObjectScannerProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemDefinitionRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.util.List;

/* CDI producers for catalog-related components */
@ApplicationScoped
public class CatalogProducers {

  @Produces
  @ApplicationScoped
  public SystemDefinitionRegistry produceDefinitionRegistry(SystemCatalogProvider provider) {
    return new SystemDefinitionRegistry(provider);
  }

  @Produces
  @ApplicationScoped
  public SystemNodeRegistry produceBuiltinNodeRegistry(SystemDefinitionRegistry defs) {
    return new SystemNodeRegistry(defs);
  }

  @Produces
  @ApplicationScoped
  public EngineHintProvider produceSystemCatalogHintProvider(SystemNodeRegistry registry) {
    return new SystemCatalogHintProvider(registry);
  }

  @Produces
  @ApplicationScoped
  public ServiceLoaderSystemCatalogProvider produceSystemCatalogLoader() {
    return new ServiceLoaderSystemCatalogProvider();
  }

  @Produces
  @ApplicationScoped
  public List<SystemObjectScannerProvider> produceSystemObjectProviders(
      ServiceLoaderSystemCatalogProvider loader) {
    return loader.providers();
  }
}
