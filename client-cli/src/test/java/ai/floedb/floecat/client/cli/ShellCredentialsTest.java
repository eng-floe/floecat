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

package ai.floedb.floecat.client.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.connector.rpc.AuthCredentials;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ShellCredentialsTest {
  private Shell shell;

  @BeforeEach
  void setUp() {
    shell = new Shell();
  }

  private AuthCredentials build(
      String type, Map<String, String> props, Map<String, String> headers) {
    return AuthCredentialParser.buildCredentials(type, props, headers);
  }

  private static Map<String, String> map(String... kvs) {
    Map<String, String> out = new LinkedHashMap<>();
    for (int i = 0; i + 1 < kvs.length; i += 2) {
      out.put(kvs[i], kvs[i + 1]);
    }
    return out;
  }

  @Test
  void bearerCredentials() {
    var props = map("token", "t1", "extra", "v1");
    AuthCredentials creds = build("bearer", props, Map.of());
    assertEquals(AuthCredentials.CredentialCase.BEARER, creds.getCredentialCase());
    assertEquals("t1", creds.getBearer().getToken());
    assertEquals("v1", creds.getPropertiesMap().get("extra"));
  }

  @Test
  void clientCredentials() {
    var props =
        map(
            "endpoint",
            "https://example/token",
            "client_id",
            "cid",
            "client_secret",
            "csecret",
            "extra",
            "v2");
    AuthCredentials creds = build("client", props, Map.of());
    assertEquals(AuthCredentials.CredentialCase.CLIENT, creds.getCredentialCase());
    assertEquals("https://example/token", creds.getClient().getEndpoint());
    assertEquals("cid", creds.getClient().getClientId());
    assertEquals("csecret", creds.getClient().getClientSecret());
    assertEquals("v2", creds.getPropertiesMap().get("extra"));
  }

  @Test
  void cliCredentials() {
    var props =
        map(
            "provider",
            "databricks",
            "cache_path",
            "/tmp/cache.json",
            "client_id",
            "cli-id",
            "scope",
            "all-apis",
            "extra",
            "v3");
    AuthCredentials creds = build("cli", props, Map.of());
    assertEquals(AuthCredentials.CredentialCase.CLI, creds.getCredentialCase());
    assertEquals("databricks", creds.getCli().getProvider());
    assertEquals("/tmp/cache.json", creds.getPropertiesMap().get("cache_path"));
    assertEquals("cli-id", creds.getPropertiesMap().get("client_id"));
    assertEquals("all-apis", creds.getPropertiesMap().get("scope"));
    assertEquals("v3", creds.getPropertiesMap().get("extra"));
  }

  @Test
  void cliAwsProfileCredentials() {
    var props =
        map(
            "provider",
            "aws",
            "profile_name",
            "dev",
            "cache_path",
            "/tmp/aws-config",
            "extra",
            "v3b");
    AuthCredentials creds = build("cli", props, Map.of());
    assertEquals(AuthCredentials.CredentialCase.CLI, creds.getCredentialCase());
    assertEquals("aws", creds.getCli().getProvider());
    assertEquals("dev", creds.getPropertiesMap().get("profile_name"));
    assertEquals("/tmp/aws-config", creds.getPropertiesMap().get("cache_path"));
    assertEquals("v3b", creds.getPropertiesMap().get("extra"));
  }

  @Test
  void tokenExchangeCredentials() {
    var props =
        map(
            "endpoint",
            "https://example/token",
            "subject_token_type",
            "urn:subject",
            "requested_token_type",
            "urn:requested",
            "audience",
            "aud",
            "scope",
            "s1",
            "client_id",
            "cid",
            "client_secret",
            "csecret",
            "extra",
            "v4");
    var headers = map("X-Test", "yes");
    AuthCredentials creds = build("token-exchange", props, headers);
    assertEquals(AuthCredentials.CredentialCase.RFC8693_TOKEN_EXCHANGE, creds.getCredentialCase());
    var base = creds.getRfc8693TokenExchange().getBase();
    assertEquals("https://example/token", base.getEndpoint());
    assertEquals("urn:subject", base.getSubjectTokenType());
    assertEquals("urn:requested", base.getRequestedTokenType());
    assertEquals("aud", base.getAudience());
    assertEquals("s1", base.getScope());
    assertEquals("cid", base.getClientId());
    assertEquals("csecret", base.getClientSecret());
    assertEquals("v4", creds.getPropertiesMap().get("extra"));
    assertEquals("yes", creds.getHeadersMap().get("X-Test"));
  }

  @Test
  void entraExchangeCredentials() {
    var props =
        map(
            "endpoint",
            "https://entra/token",
            "subject_token_type",
            "urn:subject",
            "requested_token_type",
            "urn:requested",
            "audience",
            "aud",
            "scope",
            "s2",
            "client_id",
            "cid",
            "client_secret",
            "csecret");
    AuthCredentials creds = build("token-exchange-entra", props, Map.of());
    assertEquals(AuthCredentials.CredentialCase.AZURE_TOKEN_EXCHANGE, creds.getCredentialCase());
    var base = creds.getAzureTokenExchange().getBase();
    assertEquals("https://entra/token", base.getEndpoint());
    assertEquals("urn:subject", base.getSubjectTokenType());
    assertEquals("urn:requested", base.getRequestedTokenType());
    assertEquals("aud", base.getAudience());
    assertEquals("s2", base.getScope());
    assertEquals("cid", base.getClientId());
    assertEquals("csecret", base.getClientSecret());
  }

  @Test
  void gcpExchangeCredentials() {
    var props =
        map(
            "endpoint",
            "https://gcp/token",
            "scope",
            "s3",
            "gcp.service_account_email",
            "svc@example.com",
            "gcp.delegated_user",
            "user@example.com",
            "gcp.service_account_private_key_pem",
            "pem",
            "gcp.service_account_private_key_id",
            "kid",
            "jwt_lifetime_seconds",
            "900");
    AuthCredentials creds = build("token-exchange-gcp", props, Map.of());
    assertEquals(AuthCredentials.CredentialCase.GCP_TOKEN_EXCHANGE, creds.getCredentialCase());
    var base = creds.getGcpTokenExchange().getBase();
    assertEquals("https://gcp/token", base.getEndpoint());
    assertEquals("s3", base.getScope());
    var gcp = creds.getGcpTokenExchange();
    assertEquals("svc@example.com", gcp.getServiceAccountEmail());
    assertEquals("user@example.com", gcp.getDelegatedUser());
    assertEquals("pem", gcp.getServiceAccountPrivateKeyPem());
    assertEquals("kid", gcp.getServiceAccountPrivateKeyId());
  }

  @Test
  void awsCredentials() {
    var props =
        map(
            "access_key_id",
            "AKIA",
            "secret_access_key",
            "secret",
            "session_token",
            "sess",
            "extra",
            "v5");
    AuthCredentials creds = build("aws", props, Map.of());
    assertEquals(AuthCredentials.CredentialCase.AWS, creds.getCredentialCase());
    assertEquals("AKIA", creds.getAws().getAccessKeyId());
    assertEquals("secret", creds.getAws().getSecretAccessKey());
    assertEquals("sess", creds.getAws().getSessionToken());
    assertEquals("v5", creds.getPropertiesMap().get("extra"));
  }

  @Test
  void awsWebIdentityCredentials() {
    var props =
        map(
            "role_arn",
            "arn:aws:iam::123:role/demo",
            "role_session_name",
            "sess",
            "provider_id",
            "provider",
            "duration_seconds",
            "3600",
            "aws.web_identity_token",
            "token",
            "extra",
            "v6");
    AuthCredentials creds = build("aws-web-identity", props, Map.of());
    assertEquals(AuthCredentials.CredentialCase.AWS_WEB_IDENTITY, creds.getCredentialCase());
    var web = creds.getAwsWebIdentity();
    assertEquals("arn:aws:iam::123:role/demo", web.getRoleArn());
    assertEquals("sess", web.getRoleSessionName());
    assertEquals("provider", web.getProviderId());
    assertEquals(3600, web.getDurationSeconds());
    assertEquals("token", creds.getPropertiesMap().get("aws.web_identity_token"));
    assertEquals("v6", creds.getPropertiesMap().get("extra"));
  }

  @Test
  void awsAssumeRoleCredentials() {
    var props =
        map(
            "role_arn",
            "arn:aws:iam::123:role/demo",
            "role_session_name",
            "sess",
            "external_id",
            "ext",
            "duration_seconds",
            "900",
            "extra",
            "v7");
    AuthCredentials creds = build("aws-assume-role", props, Map.of());
    assertEquals(AuthCredentials.CredentialCase.AWS_ASSUME_ROLE, creds.getCredentialCase());
    var ar = creds.getAwsAssumeRole();
    assertEquals("arn:aws:iam::123:role/demo", ar.getRoleArn());
    assertEquals("sess", ar.getRoleSessionName());
    assertEquals("ext", ar.getExternalId());
    assertEquals(900, ar.getDurationSeconds());
    assertEquals("v7", creds.getPropertiesMap().get("extra"));
  }

  @Test
  void missingRequiredFieldsThrow() {
    var props = map("client_id", "cid");
    assertThrows(IllegalArgumentException.class, () -> build("client", props, Map.of()));
  }
}
