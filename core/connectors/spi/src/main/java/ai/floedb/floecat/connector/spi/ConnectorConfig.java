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

package ai.floedb.floecat.connector.spi;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public record ConnectorConfig(
    Kind kind, String displayName, String uri, Map<String, String> options, Auth auth) {

  public ConnectorConfig {
    Objects.requireNonNull(kind);
    Objects.requireNonNull(displayName);
    Objects.requireNonNull(uri);
    options = options == null ? Map.of() : Collections.unmodifiableMap(options);
    auth = auth == null ? new Auth("none", Map.of(), Map.of()) : auth;
  }

  /**
   * The table format a connector reads.
   *
   * <p>Deliberately not the catalog. Which catalog indexes those tables is a separate axis carried
   * in {@link #options()} -- {@code delta.source}, {@code iceberg.source} -- because one format is
   * reachable through several catalogs. A {@code UNITY} member used to sit here and named a
   * catalog, which made {@code DELTA} + {@code delta.source=unity} and {@code UNITY} two spellings
   * of one connector; every Delta-family check then had to remember both, and one of them did not.
   */
  public enum Kind {
    ICEBERG,
    DELTA,
    GLUE
  }

  public record Auth(String scheme, Map<String, String> props, Map<String, String> headerHints) {
    public Auth {
      scheme = Objects.requireNonNullElse(scheme, "none");
      props = props == null ? Map.of() : Map.copyOf(props);
      headerHints = headerHints == null ? Map.of() : Map.copyOf(headerHints);
    }
  }
}
