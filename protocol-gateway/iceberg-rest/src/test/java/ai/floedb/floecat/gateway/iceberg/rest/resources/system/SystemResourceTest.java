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
        .body("{\"grant_type\":\"client_credentials\"}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/oauth/tokens")
        .then()
        .statusCode(501)
        .body("error.type", equalTo("UnsupportedOperationException"));
  }

  @Test
  void returnsConfigDto() {
    given()
        .when()
        .get("/v1/config")
        .then()
        .statusCode(200)
        .body("defaults.'catalog-name'", equalTo("examples"))
        .body(
            "endpoints",
            hasItems("POST /v1/{prefix}/tables/rename", "POST /v1/{prefix}/views/rename"));
  }
}
