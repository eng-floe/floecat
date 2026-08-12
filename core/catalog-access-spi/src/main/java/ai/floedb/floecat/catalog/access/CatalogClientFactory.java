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

package ai.floedb.floecat.catalog.access;

import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

/** Resolves catalog clients by protocol without depending on Connector resources or RPC types. */
public final class CatalogClientFactory {
  private final Map<CatalogProtocol, CatalogClientProvider> providers;

  public CatalogClientFactory(Collection<CatalogClientProvider> providers) {
    Objects.requireNonNull(providers, "providers");
    Map<CatalogProtocol, CatalogClientProvider> indexed = new EnumMap<>(CatalogProtocol.class);
    for (CatalogClientProvider provider : providers) {
      CatalogClientProvider previous = indexed.put(provider.protocol(), provider);
      if (previous != null) {
        throw new IllegalArgumentException(
            "Multiple CatalogClientProviders for protocol=" + provider.protocol());
      }
    }
    this.providers = Map.copyOf(indexed);
  }

  public static CatalogClientFactory load() {
    return new CatalogClientFactory(
        ServiceLoader.load(CatalogClientProvider.class, CatalogClientFactory.class.getClassLoader())
            .stream()
            .map(ServiceLoader.Provider::get)
            .toList());
  }

  public CatalogClient open(
      CatalogConnectionConfig config, ResolvedCatalogCredentials resolvedCredentials) {
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(resolvedCredentials, "resolvedCredentials");
    CatalogClientProvider provider = providers.get(config.protocol());
    if (provider == null) {
      throw new IllegalStateException("No CatalogClientProvider for protocol=" + config.protocol());
    }
    return provider.open(config, resolvedCredentials);
  }
}
