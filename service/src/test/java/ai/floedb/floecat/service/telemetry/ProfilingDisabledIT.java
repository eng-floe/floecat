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

package ai.floedb.floecat.service.telemetry;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import java.net.URL;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(ProfilingDisabledProfile.class)
class ProfilingDisabledIT {
  @TestHTTPResource("/")
  URL baseUrl;

  @Test
  void disabledProfileReturns503() {
    given()
        .baseUri(baseUrl.toString())
        .contentType(ContentType.JSON)
        .body(
            "{\"trigger\":\"cli\",\"mode\":\"jfr\",\"scope\":\"manual\",\"requestedBy\":\"test\"}")
        .when()
        .post("/profiling/captures")
        .then()
        .statusCode(503)
        .body("error", equalTo("disabled"))
        .body("message", equalTo("profiling guard prevented capture"));
  }
}
