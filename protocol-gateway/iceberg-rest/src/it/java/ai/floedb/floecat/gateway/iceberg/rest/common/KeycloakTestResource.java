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

package ai.floedb.floecat.gateway.iceberg.rest.common;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

public class KeycloakTestResource implements QuarkusTestResourceLifecycleManager {
  private static final String DEFAULT_ISSUER = "http://127.0.0.1:12221/realms/floecat";
  private static final String ISSUER_ENV = "FLOECAT_TEST_KEYCLOAK_ISSUER";

  @Override
  public Map<String, String> start() {
    String issuer = System.getenv(ISSUER_ENV);
    if (issuer == null || issuer.isBlank()) {
      issuer = DEFAULT_ISSUER;
    }
    String discovery = issuer.endsWith("/")
        ? issuer + ".well-known/openid-configuration"
        : issuer + "/.well-known/openid-configuration";
    try {
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(discovery))
              .timeout(Duration.ofSeconds(5))
              .GET()
              .build();
      HttpResponse<Void> response =
          HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.discarding());
      if (response.statusCode() / 100 != 2) {
        throw new IllegalStateException(
            "Keycloak discovery failed: " + response.statusCode() + " at " + discovery);
      }
    } catch (Exception ex) {
      throw new RuntimeException(
          "Keycloak is not available for OIDC gateway ITs. Start it with `make oidc-up` "
              + "or set " + ISSUER_ENV + " to a reachable issuer.",
          ex);
    }
    return Map.of();
  }

  @Override
  public void stop() {
    // no-op
  }
}
