package ai.floedb.metacat.connector.delta.uc.impl;

import ai.floedb.metacat.connector.spi.AuthProvider;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

public final class SqlStmtClient {
  private final HttpClient client;
  private final String host;
  private final AuthProvider auth;
  private final String warehouseId;
  private final int readMs;
  private static final ObjectMapper M = new ObjectMapper();

  public SqlStmtClient(String host, AuthProvider auth, String warehouseId, int readMs) {
    this.host = host.endsWith("/") ? host.substring(0, host.length() - 1) : host;
    this.auth = auth;
    this.warehouseId = warehouseId;
    this.readMs = readMs;
    this.client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
  }

  public JsonNode run(String sql) {
    try {
      String submitJson =
          M.createObjectNode().put("statement", sql).put("warehouse_id", warehouseId).toString();

      var submitBuilder =
          HttpRequest.newBuilder()
              .uri(URI.create(host + "/api/2.0/sql/statements"))
              .timeout(Duration.ofMillis(readMs))
              .header("Content-Type", "application/json")
              .header("Accept", "application/json");
      auth.applyHeaders(Map.of()).forEach(submitBuilder::header);

      var submitReq = submitBuilder.POST(HttpRequest.BodyPublishers.ofString(submitJson)).build();

      var submitResp = client.send(submitReq, HttpResponse.BodyHandlers.ofString());
      if (submitResp.statusCode() / 100 != 2) {
        throw new RuntimeException(
            "SQL submit failed: HTTP " + submitResp.statusCode() + " " + submitResp.body());
      }

      var submitNode = M.readTree(submitResp.body());
      String id = submitNode.path("statement_id").asText(null);
      if (id == null || id.isBlank()) {
        throw new IllegalStateException("No statement_id in submit response: " + submitResp.body());
      }

      long sleepMs = 300;
      while (true) {
        var pollBuilder =
            HttpRequest.newBuilder()
                .uri(URI.create(host + "/api/2.0/sql/statements/" + id))
                .timeout(Duration.ofMillis(readMs))
                .header("Accept", "application/json");
        auth.applyHeaders(Map.of()).forEach(pollBuilder::header);

        var pollReq = pollBuilder.GET().build();
        var pollResp = client.send(pollReq, HttpResponse.BodyHandlers.ofString());
        if (pollResp.statusCode() / 100 != 2) {
          throw new RuntimeException(
              "SQL poll failed: HTTP " + pollResp.statusCode() + " " + pollResp.body());
        }

        var pj = M.readTree(pollResp.body());
        String state = pj.path("status").path("state").asText("");

        if ("SUCCEEDED".equals(state) || "FINISHED".equals(state)) {
          return pj.path("result");
        }
        if ("FAILED".equals(state) || "CANCELED".equals(state)) {
          throw new RuntimeException("SQL failed: " + pj.toPrettyString());
        }

        long delay =
            pollResp
                .headers()
                .firstValue("Retry-After")
                .map(SqlStmtClient::parseRetryAfterMillis)
                .orElse(sleepMs);

        Thread.sleep(delay);
        sleepMs = Math.min(sleepMs * 2, 3000);
      }
    } catch (Exception e) {
      throw new RuntimeException("SQL execution error", e);
    }
  }

  private static long parseRetryAfterMillis(String v) {
    try {
      long seconds = Long.parseLong(v.trim());
      return seconds * 1000L;
    } catch (Exception ignore) {
      return 300L;
    }
  }
}
