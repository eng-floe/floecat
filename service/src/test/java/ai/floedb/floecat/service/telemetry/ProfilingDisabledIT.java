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
        .body("message", equalTo("profiling disabled"));
  }
}
