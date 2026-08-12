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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Provider-neutral table discovery metadata. This is not the captured Floecat table resource. */
public record CatalogTable(
    CatalogObjectName name,
    ExternalObjectIdentity identity,
    String format,
    Optional<String> metadataLocation,
    Optional<String> storageLocation,
    Map<String, String> properties) {
  public CatalogTable {
    name = Objects.requireNonNull(name, "name");
    identity = Objects.requireNonNull(identity, "identity");
    format = Objects.requireNonNull(format, "format").trim();
    if (format.isEmpty()) {
      throw new IllegalArgumentException("format must not be blank");
    }
    metadataLocation = Objects.requireNonNull(metadataLocation, "metadataLocation");
    storageLocation = Objects.requireNonNull(storageLocation, "storageLocation");
    properties = Map.copyOf(Objects.requireNonNull(properties, "properties"));
  }
}
