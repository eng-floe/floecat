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

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Short-lived, table-scoped credentials returned by an upstream catalog protocol. */
public record VendedStorageCredentials(
    Map<String, String> properties, String scopePrefix, Optional<Instant> expiresAt) {
  public VendedStorageCredentials {
    properties = Map.copyOf(Objects.requireNonNull(properties, "properties"));
    if (properties.isEmpty()) {
      throw new IllegalArgumentException("properties must not be empty");
    }
    scopePrefix = Objects.requireNonNull(scopePrefix, "scopePrefix");
    expiresAt = Objects.requireNonNull(expiresAt, "expiresAt");
  }

  @Override
  public String toString() {
    return "VendedStorageCredentials[propertyKeys="
        + NonSecretCatalogConfig.propertyKeys(properties)
        + ", scopePrefix=<redacted>"
        + ", expiresAt="
        + expiresAt
        + "]";
  }
}
