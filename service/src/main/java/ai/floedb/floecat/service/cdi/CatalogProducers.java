package ai.floedb.floecat.service.cdi;

import ai.floedb.floecat.catalog.builtin.graph.BuiltinNodeRegistry;
import ai.floedb.floecat.catalog.builtin.hint.BuiltinCatalogHintProvider;
import ai.floedb.floecat.catalog.builtin.provider.BuiltinCatalogProvider;
import ai.floedb.floecat.catalog.builtin.provider.ServiceLoaderBuiltinCatalogProvider;
import ai.floedb.floecat.catalog.builtin.registry.BuiltinDefinitionRegistry;
import ai.floedb.floecat.metagraph.hint.EngineHintProvider;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

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
}
