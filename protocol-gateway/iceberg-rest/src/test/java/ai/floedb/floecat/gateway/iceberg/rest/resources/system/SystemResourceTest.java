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

package ai.floedb.floecat.gateway.iceberg.rest.resources.system;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;

import ai.floedb.floecat.gateway.iceberg.rest.resources.AbstractRestResourceTest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.RestResourceTestProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(RestResourceTestProfile.class)
class SystemResourceTest extends AbstractRestResourceTest {

  @Test
  void oauthTokensEndpointNotSupported() {
    given()
        .contentType("application/x-www-form-urlencoded")
        .formParam("grant_type", "client_credentials")
        .when()
        .post("/v1/oauth/tokens")
        .then()
        .statusCode(400)
        .body("error", equalTo("unsupported_grant_type"));
  }

  @Test
  void returnsConfigDto() {
    given()
        .when()
        .get("/v1/config?warehouse=examples")
        .then()
        .statusCode(200)
        .body("defaults.'catalog-name'", equalTo("examples"))
        .body(
            "endpoints",
            hasItems("POST /v1/{prefix}/tables/rename", "POST /v1/{prefix}/views/rename"));
  }
}
