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

package ai.floedb.floecat.connector.common.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

class CredentialResolverSupportTest {
  private HttpServer server;

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  void tokenExchangeRfc8693ExchangesToken() throws Exception {
    AtomicReference<CapturedRequest> captured = new AtomicReference<>();
    server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext(
        "/token",
        exchange -> {
          String body =
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
          captured.set(new CapturedRequest(body, exchange.getRequestHeaders()));
          byte[] response =
              "{\"access_token\":\"exchanged\",\"token_type\":\"Bearer\",\"expires_in\":3600}"
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();

    String endpoint = "http://localhost:" + server.getAddress().getPort() + "/token";
    var exchange =
        AuthCredentials.TokenExchange.newBuilder()
            .setEndpoint(endpoint)
            .setSubjectRef("subject-token")
            .setSubjectTokenType("urn:ietf:params:oauth:token-type:jwt")
            .setRequestedTokenType("urn:ietf:params:oauth:token-type:access_token")
            .setAudience("example-audience")
            .setScope("scope-a scope-b")
            .putProperties("custom", "value")
            .putHeaders("X-Test", "yes")
            .build();

    var rfc = AuthCredentials.Rfc8693TokenExchange.newBuilder().setBase(exchange).build();

    var creds = AuthCredentials.newBuilder().setRfc8693TokenExchange(rfc).build();
    var base =
        new ConnectorConfig(
            ConnectorConfig.Kind.DELTA,
            "name",
            "uri",
            Map.of(),
            new ConnectorConfig.Auth("oauth2", Map.of(), Map.of(), "", null));

    ConnectorConfig applied = CredentialResolverSupport.apply(base, creds);

    assertEquals("exchanged", applied.auth().props().get("token"));

    CapturedRequest req = captured.get();
    assertNotNull(req);
    Map<String, String> form = parseForm(req.body);
    assertEquals("urn:ietf:params:oauth:grant-type:token-exchange", form.get("grant_type"));
    assertEquals("subject-token", form.get("subject_token"));
    assertEquals("urn:ietf:params:oauth:token-type:jwt", form.get("subject_token_type"));
    assertEquals("urn:ietf:params:oauth:token-type:access_token", form.get("requested_token_type"));
    assertEquals("example-audience", form.get("audience"));
    assertEquals("scope-a scope-b", form.get("scope"));
    assertEquals("value", form.get("custom"));
    assertEquals("yes", req.headers.getFirst("X-Test"));
  }

  @Test
  void tokenExchangeAzureOboExchangesToken() throws Exception {
    AtomicReference<CapturedRequest> captured = new AtomicReference<>();
    server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext(
        "/token",
        exchange -> {
          String body =
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
          captured.set(new CapturedRequest(body, exchange.getRequestHeaders()));
          byte[] response =
              "{\"access_token\":\"azure-token\",\"token_type\":\"Bearer\",\"expires_in\":3600}"
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();

    String endpoint = "http://localhost:" + server.getAddress().getPort() + "/token";
    var base =
        AuthCredentials.TokenExchange.newBuilder()
            .setEndpoint(endpoint)
            .setSubjectRef("subject-token")
            .setScope("https://graph.microsoft.com/.default")
            .putProperties("oauth.client_id", "client-1")
            .putProperties("oauth.client_secret", "secret-1")
            .build();
    var azure = AuthCredentials.AzureTokenExchange.newBuilder().setBase(base).build();
    var creds = AuthCredentials.newBuilder().setAzureTokenExchange(azure).build();
    var cfg =
        new ConnectorConfig(
            ConnectorConfig.Kind.DELTA,
            "name",
            "uri",
            Map.of(),
            new ConnectorConfig.Auth("oauth2", Map.of(), Map.of(), "", null));

    ConnectorConfig applied = CredentialResolverSupport.apply(cfg, creds);
    assertEquals("azure-token", applied.auth().props().get("token"));

    CapturedRequest req = captured.get();
    Map<String, String> form = parseForm(req.body);
    assertEquals("urn:ietf:params:oauth:grant-type:jwt-bearer", form.get("grant_type"));
    assertEquals("on_behalf_of", form.get("requested_token_use"));
    assertEquals("subject-token", form.get("assertion"));
    assertEquals("https://graph.microsoft.com/.default", form.get("scope"));
    String expectedBasic =
        "Basic "
            + Base64.getEncoder()
                .encodeToString("client-1:secret-1".getBytes(StandardCharsets.UTF_8));
    assertEquals(expectedBasic, req.headers.getFirst("Authorization"));
  }

  @Test
  void tokenExchangeGcpDwdExchangesToken() throws Exception {
    AtomicReference<CapturedRequest> captured = new AtomicReference<>();
    server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext(
        "/token",
        exchange -> {
          String body =
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
          captured.set(new CapturedRequest(body, exchange.getRequestHeaders()));
          byte[] response =
              "{\"access_token\":\"gcp-token\",\"token_type\":\"Bearer\",\"expires_in\":3600}"
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();

    String endpoint = "http://localhost:" + server.getAddress().getPort() + "/token";
    String pem = generatePkcs8Pem();

    var base =
        AuthCredentials.TokenExchange.newBuilder()
            .setEndpoint(endpoint)
            .setScope("https://www.googleapis.com/auth/cloud-platform")
            .putProperties("gcp.service_account_email", "svc@example.com")
            .putProperties("gcp.delegated_user", "user@example.com")
            .putProperties("gcp.service_account_private_key_pem", pem)
            .build();
    var gcp = AuthCredentials.GcpTokenExchange.newBuilder().setBase(base).build();
    var creds = AuthCredentials.newBuilder().setGcpTokenExchange(gcp).build();
    var cfg =
        new ConnectorConfig(
            ConnectorConfig.Kind.DELTA,
            "name",
            "uri",
            Map.of(),
            new ConnectorConfig.Auth("oauth2", Map.of(), Map.of(), "", null));

    ConnectorConfig applied = CredentialResolverSupport.apply(cfg, creds);
    assertEquals("gcp-token", applied.auth().props().get("token"));

    CapturedRequest req = captured.get();
    Map<String, String> form = parseForm(req.body);
    assertEquals("urn:ietf:params:oauth:grant-type:jwt-bearer", form.get("grant_type"));
    assertEquals("https://www.googleapis.com/auth/cloud-platform", form.get("scope"));
    String assertion = form.get("assertion");
    assertNotNull(assertion);
  }

  @Test
  void buildAssumeRoleRequestUsesDefaults() {
    var ar =
        AuthCredentials.AwsAssumeRole.newBuilder()
            .setRoleArn("arn:aws:iam::123456789012:role/demo")
            .setExternalId("ext-1")
            .setDurationSeconds(900)
            .build();

    AssumeRoleRequest req = CredentialResolverSupport.buildAssumeRoleRequest(ar);

    assertEquals("arn:aws:iam::123456789012:role/demo", req.roleArn());
    assertEquals("floecat-assume-role", req.roleSessionName());
    assertEquals("ext-1", req.externalId());
    assertEquals(900, req.durationSeconds());
  }

  @Test
  void buildAssumeRoleWithWebIdentityRequestUsesDefaults() {
    var web =
        AuthCredentials.AwsWebIdentity.newBuilder()
            .setRoleArn("arn:aws:iam::123456789012:role/web")
            .setProviderId("provider")
            .setDurationSeconds(3600)
            .build();
    Map<String, String> props = Map.of("aws.web_identity_token", "token-value");

    AssumeRoleWithWebIdentityRequest req =
        CredentialResolverSupport.buildAssumeRoleWithWebIdentityRequest(web, props);

    assertEquals("arn:aws:iam::123456789012:role/web", req.roleArn());
    assertEquals("floecat-web-identity", req.roleSessionName());
    assertEquals("provider", req.providerId());
    assertEquals("token-value", req.webIdentityToken());
    assertEquals(3600, req.durationSeconds());
  }

  @Test
  void assumeRoleUsesStsClient() {
    StsClient sts = mock(StsClient.class);
    Credentials creds =
        Credentials.builder()
            .accessKeyId("AKIA")
            .secretAccessKey("secret")
            .sessionToken("token")
            .build();
    when(sts.assumeRole(any(AssumeRoleRequest.class)))
        .thenReturn(AssumeRoleResponse.builder().credentials(creds).build());

    Credentials resolved =
        CredentialResolverSupport.assumeRole(
            AssumeRoleRequest.builder()
                .roleArn("arn:aws:iam::123456789012:role/demo")
                .roleSessionName("demo")
                .build(),
            sts);

    assertEquals("AKIA", resolved.accessKeyId());
    assertEquals("secret", resolved.secretAccessKey());
    assertEquals("token", resolved.sessionToken());
  }

  @Test
  void assumeRoleWithWebIdentityUsesStsClient() {
    StsClient sts = mock(StsClient.class);
    Credentials creds =
        Credentials.builder()
            .accessKeyId("AKIA-WEB")
            .secretAccessKey("secret-web")
            .sessionToken("token-web")
            .build();
    when(sts.assumeRoleWithWebIdentity(any(AssumeRoleWithWebIdentityRequest.class)))
        .thenReturn(AssumeRoleWithWebIdentityResponse.builder().credentials(creds).build());

    Credentials resolved =
        CredentialResolverSupport.assumeRoleWithWebIdentity(
            AssumeRoleWithWebIdentityRequest.builder()
                .roleArn("arn:aws:iam::123456789012:role/web")
                .roleSessionName("demo")
                .webIdentityToken("token-value")
                .build(),
            sts);

    assertEquals("AKIA-WEB", resolved.accessKeyId());
    assertEquals("secret-web", resolved.secretAccessKey());
    assertEquals("token-web", resolved.sessionToken());
  }

  private static Map<String, String> parseForm(String body) {
    Map<String, String> out = new HashMap<>();
    if (body == null || body.isBlank()) {
      return out;
    }
    for (String pair : body.split("&")) {
      int idx = pair.indexOf('=');
      if (idx < 0) {
        continue;
      }
      String key = java.net.URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8);
      String val = java.net.URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8);
      out.put(key, val);
    }
    return out;
  }

  private static String generatePkcs8Pem() {
    try {
      var generator = java.security.KeyPairGenerator.getInstance("RSA");
      generator.initialize(2048);
      var privateKey = generator.generateKeyPair().getPrivate();
      var encoded = privateKey.getEncoded();
      var base64 =
          Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.UTF_8)).encodeToString(encoded);
      return "-----BEGIN PRIVATE KEY-----\n" + base64 + "\n-----END PRIVATE KEY-----\n";
    } catch (Exception e) {
      throw new RuntimeException("Failed to generate PKCS#8 key", e);
    }
  }

  private record CapturedRequest(String body, Headers headers) {}
}
