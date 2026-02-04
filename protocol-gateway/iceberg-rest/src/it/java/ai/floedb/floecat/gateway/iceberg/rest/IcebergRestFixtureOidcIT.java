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

package ai.floedb.floecat.gateway.iceberg.rest;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.hasItem;

import ai.floedb.floecat.gateway.iceberg.rest.common.KeycloakTestResource;
import ai.floedb.floecat.gateway.iceberg.rest.common.RealServiceTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(OidcGatewayProfile.class)
@QuarkusTestResource(value = RealServiceTestResource.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = KeycloakTestResource.class, restrictToAnnotatedClass = true)
@EnabledIfSystemProperty(named = "floecat.test.oidc", matches = "true")
class IcebergRestFixtureOidcIT {

  private static final String CATALOG = "examples";
  private static RequestSpecification spec;

  @BeforeAll
  static void setUp() throws Exception {
    String token = fetchAccessToken();
    spec =
        new RequestSpecBuilder()
            .addHeader("authorization", "Bearer " + token)
            .setContentType(ContentType.JSON)
            .build();
  }

  @Test
  void listsNamespacesFromSeededService() {
    given()
        .spec(spec)
        .when()
        .get("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(200)
        .body("namespaces*.get(0)", hasItem("iceberg"));
  }

  private static String fetchAccessToken() throws IOException, InterruptedException {
    String body =
        "client_id=floecat-client&client_secret=floecat-secret&grant_type=client_credentials";
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:12221/realms/floecat/protocol/openid-connect/token"))
            .timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();

    HttpResponse<String> response =
        HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() / 100 != 2) {
      throw new IllegalStateException(
          "Keycloak token request failed: " + response.statusCode() + " " + response.body());
    }
    return extractJsonValue(response.body(), "access_token");
  }

  private static String extractJsonValue(String json, String key) {
    String needle = "\"" + key + "\":\"";
    int start = json.indexOf(needle);
    if (start < 0) {
      throw new IllegalStateException("Missing key " + key + " in response: " + json);
    }
    start += needle.length();
    int end = json.indexOf('"', start);
    if (end < 0) {
      throw new IllegalStateException("Malformed JSON response: " + json);
    }
    return json.substring(start, end);
  }
}
