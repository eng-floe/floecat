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

/** In-memory resolved credentials. Its string representation never includes secret material. */
public final class ResolvedCatalogCredentials {
  private static final ResolvedCatalogCredentials NONE =
      new ResolvedCatalogCredentials(Map.of(), Map.of(), null);

  private final Map<String, String> properties;
  private final Map<String, String> headers;
  private final Instant expiresAt;

  public ResolvedCatalogCredentials(
      Map<String, String> properties, Map<String, String> headers, Instant expiresAt) {
    this.properties = Map.copyOf(Objects.requireNonNull(properties, "properties"));
    this.headers = Map.copyOf(Objects.requireNonNull(headers, "headers"));
    this.expiresAt = expiresAt;
  }

  public static ResolvedCatalogCredentials none() {
    return NONE;
  }

  public Map<String, String> properties() {
    return properties;
  }

  public Map<String, String> headers() {
    return headers;
  }

  public Optional<Instant> expiresAt() {
    return Optional.ofNullable(expiresAt);
  }

  public boolean isEmpty() {
    return properties.isEmpty() && headers.isEmpty();
  }

  @Override
  public String toString() {
    return "ResolvedCatalogCredentials[properties=<redacted:"
        + properties.size()
        + ">, headers=<redacted:"
        + headers.size()
        + ">, expiresAt="
        + expiresAt
        + "]";
  }
}
