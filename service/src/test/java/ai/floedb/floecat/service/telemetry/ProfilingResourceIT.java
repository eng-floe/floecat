package ai.floedb.floecat.service.telemetry;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import java.net.URL;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(ProfilingTestProfile.class)
class ProfilingResourceIT {

  @TestHTTPResource(value = "/metrics", management = true)
  URL metricsUrl;

  @TestHTTPResource("/")
  URL baseUrl;

  @Test
  void captureLifecycle() {
    String id = createCapture();

    given()
        .baseUri(baseUrl.toString())
        .when()
        .get("/profiling/captures/{id}", id)
        .then()
        .statusCode(200)
        .body("id", equalTo(id));

    given()
        .baseUri(baseUrl.toString())
        .when()
        .get("/profiling/captures/latest")
        .then()
        .statusCode(200)
        .body("id", equalTo(id));
  }

  @Test
  void artifactDownloadAndMetricsPopulated() throws InterruptedException {
    String id = createCapture();
    waitForCompletion(id);

    given()
        .baseUri(baseUrl.toString())
        .when()
        .get("/profiling/captures/{id}/artifact", id)
        .then()
        .statusCode(200)
        .header("Content-Disposition", equalTo("attachment; filename=profiling-" + id + ".jfr"));

    String metrics = given().when().get(metricsUrl).then().statusCode(200).extract().asString();
    Assertions.assertTrue(metrics.contains("floecat_profiling_captures_total"));
    Assertions.assertTrue(
        metrics.contains("result=\"started\"")
            || metrics.contains("result=\"completed\"")
            || metrics.contains("result=\"dropped\""));
  }

  private String createCapture() {
    Map<String, Object> meta =
        given()
            .baseUri(baseUrl.toString())
            .contentType(ContentType.JSON)
            .body(
                "{\"trigger\":\"cli\",\"mode\":\"jfr\",\"scope\":\"manual\",\"requestedBy\":\"test\"}")
            .when()
            .post("/profiling/captures")
            .then()
            .statusCode(202)
            .body("id", notNullValue())
            .extract()
            .as(Map.class);
    return meta.get("id").toString();
  }

  private void waitForCompletion(String id) throws InterruptedException {
    for (int i = 0; i < 10; i++) {
      Map<String, Object> meta =
          given()
              .baseUri(baseUrl.toString())
              .when()
              .get("/profiling/captures/{id}", id)
              .then()
              .statusCode(200)
              .extract()
              .as(Map.class);
      if ("completed".equals(meta.get("result"))) {
        return;
      }
      Thread.sleep(1000);
    }
    Assertions.fail("capture did not complete in time");
  }
}
