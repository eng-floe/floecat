package ai.floedb.floecat.connector.delta.uc.impl;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.connector.spi.AuthProvider;
import ai.floedb.floecat.http.guards.HttpEndpointGuards;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/** Opt-in discovery check against the local account log catalog. */
class LocalAccountLogConnectorIT {
  @Test
  @EnabledIfSystemProperty(named = "floe.log.local.connector.it", matches = "true")
  void discoversAccountLogTables() throws Exception {
    Properties storage = properties(System.getProperty("floe.log.local.storage.info"));
    Properties unity = properties(System.getProperty("floe.log.local.unity.info"));
    String accountId = System.getProperty("floe.log.local.account-id", "local-log-ingest-it");
    String catalog = System.getProperty("floe.log.local.catalog", "floe_logs");
    String schema =
        "acct_"
            + java.util.HexFormat.of()
                .formatHex(accountId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    String namespace = catalog + "." + schema;
    String token = unity.getProperty("unity.bearer.token");

    Map<String, String> options = new HashMap<>();
    options.put("delta.source", "unity");
    options.put("s3.endpoint", storage.getProperty("minio.endpoint"));
    options.put("s3.access-key-id", storage.getProperty("minio.access.key"));
    options.put("s3.secret-access-key", storage.getProperty("minio.secret.key"));
    options.put("s3.path-style-access", "true");
    options.put("s3.region", "us-east-1");
    options.put(
        "unity.temporary-table-vend-path", "/api/2.1/unity-catalog/temporary-table-credentials");
    AuthProvider auth =
        new AuthProvider() {
          @Override
          public String scheme() {
            return "oauth2";
          }

          @Override
          public Map<String, String> apply(Map<String, String> props) {
            return props;
          }

          @Override
          public Map<String, String> applyHeaders(Map<String, String> headers) {
            return Map.of("Authorization", "Bearer " + token);
          }
        };

    String property = HttpEndpointGuards.ALLOW_LOOPBACK_PROPERTY;
    String previous = System.setProperty(property, "true");
    try (var connector =
        DeltaConnectorFactory.create(
            unity.getProperty("unity.endpoint"), options, auth, Map.of())) {
      assertTrue(connector.listNamespaces().contains(namespace));
      assertTrue(connector.listTables(namespace).contains("query_log"));
      assertTrue(connector.listTables(namespace).contains("session_log"));
      assertTrue(connector.listTables(namespace).contains("audit_log"));
      assertNotNull(connector.describe(namespace, "query_log"));
    } finally {
      if (previous == null) System.clearProperty(property);
      else System.setProperty(property, previous);
    }
  }

  private static Properties properties(String file) throws Exception {
    Properties result = new Properties();
    try (var input = Files.newInputStream(Path.of(file))) {
      result.load(input);
    }
    return result;
  }
}
