/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.cdi;

import ai.floedb.floecat.connector.common.resolver.LogicalSchemaMapper;
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
import java.util.stream.Stream;

/* CDI producers for catalog-related components */
@ApplicationScoped
public class CatalogProducers {

  @Produces
  @ApplicationScoped
  public LogicalSchemaMapper produceLogicalSchemaMapper() {
    return new LogicalSchemaMapper();
  }

  @Produces
  @ApplicationScoped
  public SystemDefinitionRegistry produceDefinitionRegistry(SystemCatalogProvider provider) {
    return new SystemDefinitionRegistry(provider);
  }

  @Produces
  @ApplicationScoped
  public SystemNodeRegistry produceBuiltinNodeRegistry(
      SystemDefinitionRegistry defs,
      SystemObjectScannerProvider internalProvider,
      List<SystemObjectScannerProvider> providers) {
    return new SystemNodeRegistry(defs, internalProvider, providers);
  }

  @Produces
  @ApplicationScoped
  public EngineHintProvider produceSystemCatalogHintProvider(
      SystemNodeRegistry registry, SystemDefinitionRegistry definitionRegistry) {
    return new SystemCatalogHintProvider(registry, definitionRegistry);
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
    return Stream.concat(Stream.of(loader.internalProvider()), loader.providers().stream())
        .toList();
  }

  @Produces
  @ApplicationScoped
  public SystemObjectScannerProvider produceInternalSystemObjectProvider(
      ServiceLoaderSystemCatalogProvider loader) {
    return loader.internalProvider();
  }
}
