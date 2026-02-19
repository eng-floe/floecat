package ai.floedb.floecat.service.telemetry;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.net.URL;
import java.util.Map;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(ProfilingPolicyTestProfile.class)
class ProfilingPolicyIT {

  @TestHTTPResource(value = "/metrics", management = true)
  URL metricsUrl;

  @TestHTTPResource("/")
  URL baseUrl;

  @Test
  void policyCaptureRecordsMetadataAndMetrics() throws InterruptedException {
    Map<String, Object> meta = waitForPolicyCapture();
    assertEquals("policy", meta.get("trigger"));
    assertEquals("policy", meta.get("scope"));
    assertEquals("policy/gc_pressure", meta.get("requestedBy"));
    assertEquals("policy", meta.get("requestedByType"));
    assertEquals("gc_pressure", meta.get("policyName"));
    assertEquals("gc_live_bytes", meta.get("policySignal"));

    String metrics =
        given()
            .baseUri(metricsUrl.toString())
            .when()
            .get()
            .then()
            .statusCode(200)
            .extract()
            .asString();
    assertTrue(metrics.contains("floecat_profiling_captures_total"));
    assertTrue(metrics.contains("trigger=\"policy\""));
    assertTrue(metrics.contains("scope=\"policy\""));
    assertTrue(metrics.contains("policy=\"gc_pressure\""));
  }

  private Map<String, Object> waitForPolicyCapture() throws InterruptedException {
    for (int i = 0; i < 30; i++) {
      Map<String, Object> meta =
          given()
              .baseUri(baseUrl.toString())
              .when()
              .get("/profiling/captures/latest")
              .then()
              .statusCode(200)
              .extract()
              .as(Map.class);
      if ("policy".equals(meta.get("trigger"))) {
        return meta;
      }
      Thread.sleep(1000);
    }
    throw new AssertionError("policy capture never appeared");
  }
}
