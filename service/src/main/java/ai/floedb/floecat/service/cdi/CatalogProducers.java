package ai.floedb.floecat.service.cdi;

import ai.floedb.floecat.catalog.builtin.graph.BuiltinNodeRegistry;
import ai.floedb.floecat.catalog.builtin.hint.BuiltinCatalogHintProvider;
import ai.floedb.floecat.catalog.builtin.provider.BuiltinCatalogProvider;
import ai.floedb.floecat.catalog.builtin.provider.ServiceLoaderBuiltinCatalogProvider;
import ai.floedb.floecat.catalog.builtin.registry.BuiltinDefinitionRegistry;
import ai.floedb.floecat.catalog.system_objects.provider.ServiceLoaderSystemObjectProvider;
import ai.floedb.floecat.catalog.system_objects.registry.SystemObjectRegistry;
import ai.floedb.floecat.catalog.system_objects.registry.SystemObjectResolver;
import ai.floedb.floecat.catalog.system_objects.spi.SystemObjectProvider;
import ai.floedb.floecat.metagraph.hint.EngineHintProvider;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.util.List;

/* CDI producers for catalog-related components */
@ApplicationScoped
public class CatalogProducers {

  @Produces
  @ApplicationScoped
  public BuiltinCatalogProvider produceBuiltinCatalogProvider() {
    return new ServiceLoaderBuiltinCatalogProvider();
  }

  @Produces
  @ApplicationScoped
  public BuiltinDefinitionRegistry produceDefinitionRegistry(BuiltinCatalogProvider provider) {
    return new BuiltinDefinitionRegistry(provider);
  }

  @Produces
  @ApplicationScoped
  public BuiltinNodeRegistry produceBuiltinNodeRegistry(BuiltinDefinitionRegistry defs) {
    return new BuiltinNodeRegistry(defs);
  }

  @Produces
  @ApplicationScoped
  public EngineHintProvider produceBuiltinCatalogHintProvider(BuiltinNodeRegistry registry) {
    return new BuiltinCatalogHintProvider(registry);
  }

  @Produces
  @ApplicationScoped
  public List<SystemObjectProvider> produceSystemObjectProviders() {
    return new ServiceLoaderSystemObjectProvider().providers();
  }

  @Produces
  @ApplicationScoped
  public SystemObjectRegistry produceSystemObjectRegistry(List<SystemObjectProvider> providers) {
    return new SystemObjectRegistry(providers);
  }

  @Produces
  @ApplicationScoped
  public SystemObjectResolver produceSystemObjectResolver() {
    return new SystemObjectResolver();
  }
}
