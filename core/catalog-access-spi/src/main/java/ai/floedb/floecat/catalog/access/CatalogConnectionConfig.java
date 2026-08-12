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

import java.net.URI;
import java.util.Map;
import java.util.Objects;

/** Immutable, non-secret configuration for opening an upstream catalog. */
public record CatalogConnectionConfig(
    CatalogProtocol protocol,
    URI endpoint,
    Map<String, String> properties,
    CatalogAuthentication authentication) {
  public CatalogConnectionConfig {
    protocol = Objects.requireNonNull(protocol, "protocol");
    endpoint = Objects.requireNonNull(endpoint, "endpoint");
    if (!endpoint.isAbsolute()) {
      throw new IllegalArgumentException("endpoint must be absolute");
    }
    if (endpoint.getRawUserInfo() != null) {
      throw new IllegalArgumentException("endpoint must not contain user-info");
    }
    NonSecretCatalogConfig.validateEndpoint(endpoint);
    properties = Map.copyOf(Objects.requireNonNull(properties, "properties"));
    NonSecretCatalogConfig.validateProperties(properties, "connection properties");
    authentication = Objects.requireNonNull(authentication, "authentication");
  }

  @Override
  public String toString() {
    return "CatalogConnectionConfig[protocol="
        + protocol
        + ", endpoint="
        + NonSecretCatalogConfig.safeEndpoint(endpoint)
        + ", propertyKeys="
        + NonSecretCatalogConfig.propertyKeys(properties)
        + ", authentication="
        + authentication
        + "]";
  }
}
