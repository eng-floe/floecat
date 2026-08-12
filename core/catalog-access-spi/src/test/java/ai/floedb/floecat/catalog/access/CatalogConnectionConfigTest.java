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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CatalogConnectionConfigTest {
  @Test
  void rejectsSecretBearingConnectionAndAuthenticationProperties() {
    assertThrows(
        IllegalArgumentException.class,
        () -> config(URI.create("https://catalog.example/v1"), Map.of("dbPassword", "secret")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new CatalogAuthentication(
                CatalogAuthenticationScheme.OAUTH2, Map.of("client_secret", "secret")));
  }

  @Test
  void rejectsSecretBearingEndpointComponents() {
    assertThrows(
        IllegalArgumentException.class,
        () -> config(URI.create("https://catalog.example/v1?access_token=secret"), Map.of()));
    assertThrows(
        IllegalArgumentException.class,
        () -> config(URI.create("https://catalog.example/v1?X-Amz-Signature=secret"), Map.of()));
    assertThrows(
        IllegalArgumentException.class,
        () -> config(URI.create("https://catalog.example/v1#secret"), Map.of()));
  }

  @Test
  void stringRepresentationsNeverIncludePropertyOrQueryValues() {
    CatalogAuthentication authentication =
        new CatalogAuthentication(
            CatalogAuthenticationScheme.OAUTH2, Map.of("scope", "internal-scope"));
    CatalogConnectionConfig config =
        new CatalogConnectionConfig(
            CatalogProtocol.ICEBERG_REST,
            URI.create("https://catalog.example/v1?warehouse=private-warehouse"),
            Map.of("warehouse", "private-warehouse"),
            authentication);

    assertTrue(config.toString().contains("warehouse"));
    assertFalse(config.toString().contains("private-warehouse"));
    assertFalse(authentication.toString().contains("internal-scope"));
  }

  private static CatalogConnectionConfig config(URI endpoint, Map<String, String> properties) {
    return new CatalogConnectionConfig(
        CatalogProtocol.ICEBERG_REST,
        endpoint,
        properties,
        CatalogAuthentication.none());
  }
}
