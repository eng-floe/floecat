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

import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

public final class CredentialResolverSupport {
  private static final ObjectMapper M = new ObjectMapper();
  private static final String DEFAULT_SUBJECT_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:jwt";
  private static final String AZURE_OBO_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer";
  private static final String AZURE_REQUESTED_TOKEN_USE = "on_behalf_of";
  private static final String GOOGLE_JWT_BEARER_GRANT_TYPE =
      "urn:ietf:params:oauth:grant-type:jwt-bearer";
  private static final String DEFAULT_GCP_TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token";
  private static final int DEFAULT_GCP_JWT_LIFETIME_SECONDS = 3600;

  private static final int TOKEN_EXCHANGE_TIMEOUT_MS = 15000;
  private static final HttpClient HTTP =
      HttpClient.newBuilder()
          .connectTimeout(java.time.Duration.ofMillis(TOKEN_EXCHANGE_TIMEOUT_MS))
          .build();
  private static final Set<String> RFC8693_RESERVED =
      Set.of(
          "grant_type",
          "subject_token",
          "subject_token_type",
          "actor_token",
          "actor_token_type",
          "requested_token_type",
          "audience",
          "scope",
          "resource");
  private static final Set<String> AZURE_OBO_RESERVED =
      Set.of(
          "grant_type",
          "requested_token_use",
          "assertion",
          "scope",
          "client_id",
          "client_secret",
          "oauth.client_id",
          "oauth.client_secret");
  private static final Set<String> GCP_DWD_RESERVED =
      Set.of(
          "grant_type",
          "assertion",
          // These are config keys we read from properties (or explicit proto fields, if added)
          "gcp.service_account_email",
          "gcp.delegated_user",
          "gcp.service_account_private_key_pem",
          "gcp.service_account_private_key_id",
          "gcp.token_endpoint",
          "gcp.jwt_lifetime_seconds");

  private CredentialResolverSupport() {}

  public static ConnectorConfig apply(ConnectorConfig base, AuthCredentials credential) {
    if (credential == null) {
      return base;
    }

    Map<String, String> options = new HashMap<>(base.options());
    Map<String, String> authProps = new HashMap<>(base.auth().props());
    Map<String, String> headerHints = new HashMap<>(base.auth().headerHints());

    switch (credential.getCredentialCase()) {
      case BEARER -> putIfNotBlank(authProps, "token", credential.getBearer().getToken());
      case CLIENT -> {
        var client = credential.getClient();
        putIfNotBlank(authProps, "oauth.client_id", client.getClientId());
        putIfNotBlank(authProps, "oauth.client_secret", client.getClientSecret());
      }
      case AWS -> {
        var aws = credential.getAws();
        putIfNotBlank(options, "s3.access-key-id", aws.getAccessKeyId());
        putIfNotBlank(options, "s3.secret-access-key", aws.getSecretAccessKey());
        putIfNotBlank(options, "s3.session-token", aws.getSessionToken());
      }
      case AWS_WEB_IDENTITY -> {
        var web = credential.getAwsWebIdentity();
        Credentials creds = assumeRoleWithWebIdentity(web, credential.getPropertiesMap());
        applyAwsCredentials(options, creds);
      }
      case AWS_ASSUME_ROLE -> {
        var ar = credential.getAwsAssumeRole();
        Credentials creds = assumeRole(ar);
        applyAwsCredentials(options, creds);
      }
      case RFC8693_TOKEN_EXCHANGE -> {
        String token = exchangeRfc8693(requireBase(credential.getRfc8693TokenExchange()));
        putIfNotBlank(authProps, "token", token);
      }
      case AZURE_TOKEN_EXCHANGE -> {
        String token = exchangeAzureObo(requireBase(credential.getAzureTokenExchange()));
        putIfNotBlank(authProps, "token", token);
      }
      case GCP_TOKEN_EXCHANGE -> {
        String token = exchangeGoogleDwd(credential.getGcpTokenExchange());
        putIfNotBlank(authProps, "token", token);
      }
      case CREDENTIAL_NOT_SET -> {}
    }

    if (!credential.getPropertiesMap().isEmpty()) {
      authProps.putAll(credential.getPropertiesMap());
    }

    if (!credential.getHeadersMap().isEmpty()) {
      headerHints.putAll(credential.getHeadersMap());
    }

    var auth =
        new ConnectorConfig.Auth(
            base.auth().scheme(),
            authProps,
            headerHints,
            base.auth().secretRef(),
            base.auth().credentials());

    return new ConnectorConfig(base.kind(), base.displayName(), base.uri(), options, auth);
  }

  private static void putIfNotBlank(Map<String, String> target, String key, String value) {
    if (value != null && !value.isBlank()) {
      target.put(key, value);
    }
  }

  private static Credentials assumeRoleWithWebIdentity(
      AuthCredentials.AwsWebIdentity web, Map<String, String> properties) {
    var req = buildAssumeRoleWithWebIdentityRequest(web, properties);

    try (var sts = StsClient.builder().build()) {
      return assumeRoleWithWebIdentity(req, sts);
    }
  }

  static AssumeRoleWithWebIdentityRequest buildAssumeRoleWithWebIdentityRequest(
      AuthCredentials.AwsWebIdentity web, Map<String, String> properties) {
    String roleArn = requireNonBlank(web.getRoleArn(), "aws_web_identity.role_arn");
    String sessionName = defaultIfBlank(web.getRoleSessionName(), "floecat-web-identity");
    String token =
        requireNonBlank(
            properties != null ? properties.get("aws.web_identity_token") : null,
            "properties.aws.web_identity_token");

    return AssumeRoleWithWebIdentityRequest.builder()
        .roleArn(roleArn)
        .roleSessionName(sessionName)
        .webIdentityToken(token)
        .providerId(blankToNull(web.getProviderId()))
        .durationSeconds(web.getDurationSeconds() > 0 ? web.getDurationSeconds() : null)
        .build();
  }

  private static String requireNonBlank(String value, String label) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Missing required " + label);
    }
    return value;
  }

  private static String defaultIfBlank(String value, String fallback) {
    return (value == null || value.isBlank()) ? fallback : value;
  }

  private static String blankToNull(String value) {
    return (value == null || value.isBlank()) ? null : value;
  }

  static Credentials assumeRoleWithWebIdentity(
      AssumeRoleWithWebIdentityRequest req, StsClient sts) {
    AssumeRoleWithWebIdentityResponse resp = sts.assumeRoleWithWebIdentity(req);
    return resp.credentials();
  }

  private static void applyAwsCredentials(Map<String, String> options, Credentials creds) {
    if (creds == null) {
      throw new IllegalStateException("AWS STS did not return credentials");
    }
    putIfNotBlank(options, "s3.access-key-id", creds.accessKeyId());
    putIfNotBlank(options, "s3.secret-access-key", creds.secretAccessKey());
    putIfNotBlank(options, "s3.session-token", creds.sessionToken());
  }

  private static Credentials assumeRole(AuthCredentials.AwsAssumeRole ar) {
    var req = buildAssumeRoleRequest(ar);

    try (var sts =
        StsClient.builder().credentialsProvider(DefaultCredentialsProvider.create()).build()) {
      return assumeRole(req, sts);
    }
  }

  static AssumeRoleRequest buildAssumeRoleRequest(AuthCredentials.AwsAssumeRole ar) {
    String roleArn = requireNonBlank(ar.getRoleArn(), "aws_assume_role.role_arn");
    String sessionName = defaultIfBlank(ar.getRoleSessionName(), "floecat-assume-role");

    return AssumeRoleRequest.builder()
        .roleArn(roleArn)
        .roleSessionName(sessionName)
        .externalId(blankToNull(ar.getExternalId()))
        .durationSeconds(ar.getDurationSeconds() > 0 ? ar.getDurationSeconds() : null)
        .build();
  }

  static Credentials assumeRole(AssumeRoleRequest req, StsClient sts) {
    AssumeRoleResponse resp = sts.assumeRole(req);
    return resp.credentials();
  }

  private static AuthCredentials.TokenExchange requireBase(
      AuthCredentials.Rfc8693TokenExchange exchange) {
    if (exchange == null || !exchange.hasBase()) {
      throw new IllegalArgumentException("rfc8693_token_exchange.base required");
    }
    return exchange.getBase();
  }

  private static AuthCredentials.TokenExchange requireBase(
      AuthCredentials.AzureTokenExchange exchange) {
    if (exchange == null || !exchange.hasBase()) {
      throw new IllegalArgumentException("azure_token_exchange.base required");
    }
    return exchange.getBase();
  }

  private static String exchangeRfc8693(AuthCredentials.TokenExchange exchange) {
    String endpoint = requireNonBlank(exchange.getEndpoint(), "token_exchange.endpoint");
    String subjectToken = requireNonBlank(exchange.getSubjectRef(), "token_exchange.subject_ref");
    String subjectTokenType =
        defaultIfBlank(exchange.getSubjectTokenType(), DEFAULT_SUBJECT_TOKEN_TYPE);

    Map<String, String> params = new LinkedHashMap<>();
    params.put("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange");
    params.put("subject_token", subjectToken);
    params.put("subject_token_type", subjectTokenType);
    putIfNotBlank(params, "requested_token_type", exchange.getRequestedTokenType());
    putIfNotBlank(params, "audience", exchange.getAudience());
    putIfNotBlank(params, "scope", exchange.getScope());

    return sendTokenExchangeRequest(
        endpoint, params, exchange.getPropertiesMap(), exchange.getHeadersMap(), RFC8693_RESERVED);
  }

  private static String exchangeAzureObo(AuthCredentials.TokenExchange exchange) {
    String endpoint = requireNonBlank(exchange.getEndpoint(), "token_exchange.endpoint");
    String subjectToken = requireNonBlank(exchange.getSubjectRef(), "token_exchange.subject_ref");
    String scopeValue = requireNonBlank(exchange.getScope(), "token_exchange.scope");

    String clientId =
        firstNonBlank(
            exchange.getPropertiesMap().get("oauth.client_id"),
            exchange.getPropertiesMap().get("client_id"));
    String clientSecret =
        firstNonBlank(
            exchange.getPropertiesMap().get("oauth.client_secret"),
            exchange.getPropertiesMap().get("client_secret"));

    Map<String, String> params = new LinkedHashMap<>();
    params.put("grant_type", AZURE_OBO_GRANT_TYPE);
    params.put("requested_token_use", AZURE_REQUESTED_TOKEN_USE);
    params.put("assertion", subjectToken);
    params.put("scope", scopeValue);

    Map<String, String> headers = new LinkedHashMap<>(exchange.getHeadersMap());
    boolean hasAuthHeader =
        headers.keySet().stream().anyMatch(k -> "authorization".equalsIgnoreCase(k));
    if (!hasAuthHeader && !isBlank(clientId) && !isBlank(clientSecret)) {
      String basic =
          Base64.getEncoder()
              .encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));
      headers.put("Authorization", "Basic " + basic);
    } else if (!hasAuthHeader) {
      if (!isBlank(clientId)) {
        params.put("client_id", clientId);
      }
      if (!isBlank(clientSecret)) {
        params.put("client_secret", clientSecret);
      }
    }

    return sendTokenExchangeRequest(
        endpoint, params, exchange.getPropertiesMap(), headers, AZURE_OBO_RESERVED);
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  private static String firstNonBlank(String a, String b) {
    if (a != null && !a.isBlank()) {
      return a;
    }
    if (b != null && !b.isBlank()) {
      return b;
    }
    return null;
  }

  private static String sendTokenExchangeRequest(
      String endpoint,
      Map<String, String> params,
      Map<String, String> extraParams,
      Map<String, String> headers,
      Set<String> reservedKeys) {
    if (extraParams != null && !extraParams.isEmpty()) {
      for (var entry : extraParams.entrySet()) {
        if (entry.getValue() == null) {
          continue;
        }
        if (reservedKeys.contains(entry.getKey())) {
          continue;
        }
        params.putIfAbsent(entry.getKey(), entry.getValue());
      }
    }

    String body = formEncode(params);
    HttpRequest.Builder req =
        HttpRequest.newBuilder(URI.create(endpoint))
            .timeout(java.time.Duration.ofMillis(TOKEN_EXCHANGE_TIMEOUT_MS))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Accept", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body));

    if (headers != null && !headers.isEmpty()) {
      for (var entry : headers.entrySet()) {
        req.header(entry.getKey(), entry.getValue());
      }
    }

    HttpResponse<String> resp;
    try {
      resp = HTTP.send(req.build(), HttpResponse.BodyHandlers.ofString());
    } catch (Exception e) {
      throw new RuntimeException("Token exchange request failed", e);
    }

    if (resp.statusCode() / 100 != 2) {
      throw new RuntimeException(
          "Token exchange failed: HTTP " + resp.statusCode() + " " + resp.body());
    }

    String token = extractAccessToken(resp.body());
    if (token == null || token.isBlank()) {
      throw new IllegalStateException("No access_token in token exchange response");
    }
    return token;
  }

  private static String extractAccessToken(String json) {
    try {
      JsonNode root = M.readTree(json);
      return root.path("access_token").asText(null);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse token exchange response", e);
    }
  }

  private static String formEncode(Map<String, String> params) {
    StringBuilder out = new StringBuilder();
    for (var entry : params.entrySet()) {
      if (entry.getValue() == null || entry.getValue().isBlank()) {
        continue;
      }
      if (!out.isEmpty()) {
        out.append('&');
      }
      out.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8));
      out.append('=');
      out.append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
    }
    return out.toString();
  }

  private static String exchangeGoogleDwd(AuthCredentials.GcpTokenExchange exchange) {
    if (exchange == null || !exchange.hasBase()) {
      throw new IllegalArgumentException("gcp_token_exchange.base required");
    }
    var base = exchange.getBase();
    String scopeValue = requireNonBlank(base.getScope(), "gcp_token_exchange.base.scope");

    String endpoint =
        exchange.hasTokenEndpoint()
            ? exchange.getTokenEndpoint()
            : firstNonBlank(base.getEndpoint(), DEFAULT_GCP_TOKEN_ENDPOINT);

    String serviceAccountEmail =
        exchange.hasServiceAccountEmail()
            ? exchange.getServiceAccountEmail()
            : base.getPropertiesMap().get("gcp.service_account_email");
    String delegatedUser =
        exchange.hasDelegatedUser()
            ? exchange.getDelegatedUser()
            : base.getPropertiesMap().get("gcp.delegated_user");
    String privateKeyPem =
        exchange.hasServiceAccountPrivateKeyPem()
            ? exchange.getServiceAccountPrivateKeyPem()
            : base.getPropertiesMap().get("gcp.service_account_private_key_pem");
    String privateKeyId =
        exchange.hasServiceAccountPrivateKeyId()
            ? exchange.getServiceAccountPrivateKeyId()
            : base.getPropertiesMap().get("gcp.service_account_private_key_id");

    String issuer = requireNonBlank(serviceAccountEmail, "gcp.service_account_email");
    String subject = requireNonBlank(delegatedUser, "gcp.delegated_user");
    String privateKeyValue = requireNonBlank(privateKeyPem, "gcp.service_account_private_key_pem");

    int lifetimeSeconds =
        exchange.hasJwtLifetimeSeconds() && exchange.getJwtLifetimeSeconds() > 0
            ? exchange.getJwtLifetimeSeconds()
            : parseLifetimeSeconds(base.getPropertiesMap().get("gcp.jwt_lifetime_seconds"));
    if (lifetimeSeconds <= 0) {
      lifetimeSeconds = DEFAULT_GCP_JWT_LIFETIME_SECONDS;
    }

    String assertion =
        buildGcpAssertion(
            issuer, subject, endpoint, scopeValue, privateKeyValue, privateKeyId, lifetimeSeconds);

    Map<String, String> params = new LinkedHashMap<>();
    params.put("grant_type", GOOGLE_JWT_BEARER_GRANT_TYPE);
    params.put("assertion", assertion);
    params.put("scope", scopeValue);

    return sendTokenExchangeRequest(
        endpoint, params, base.getPropertiesMap(), base.getHeadersMap(), GCP_DWD_RESERVED);
  }

  private static int parseLifetimeSeconds(String raw) {
    if (raw == null || raw.isBlank()) {
      return 0;
    }
    try {
      return Integer.parseInt(raw.trim());
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  private static String buildGcpAssertion(
      String issuer,
      String subject,
      String audience,
      String scope,
      String privateKeyPem,
      String privateKeyId,
      int lifetimeSeconds) {
    long now = Instant.now().getEpochSecond();
    long exp = now + Math.max(1, Math.min(lifetimeSeconds, DEFAULT_GCP_JWT_LIFETIME_SECONDS));

    byte[] headerBytes;
    byte[] payloadBytes;
    try {
      Map<String, Object> header = new LinkedHashMap<>();
      header.put("alg", "RS256");
      header.put("typ", "JWT");
      if (privateKeyId != null && !privateKeyId.isBlank()) {
        header.put("kid", privateKeyId);
      }
      Map<String, Object> payload = new LinkedHashMap<>();
      payload.put("iss", issuer);
      payload.put("sub", subject);
      payload.put("aud", audience);
      payload.put("scope", scope);
      payload.put("iat", now);
      payload.put("exp", exp);
      headerBytes = M.writeValueAsBytes(header);
      payloadBytes = M.writeValueAsBytes(payload);
    } catch (Exception e) {
      throw new RuntimeException("Failed to build GCP JWT assertion", e);
    }

    String encodedHeader = base64Url(headerBytes);
    String encodedPayload = base64Url(payloadBytes);
    String signingInput = encodedHeader + "." + encodedPayload;

    byte[] signature = signJwt(signingInput.getBytes(StandardCharsets.UTF_8), privateKeyPem);
    return signingInput + "." + base64Url(signature);
  }

  private static byte[] signJwt(byte[] signingInput, String privateKeyPem) {
    try {
      PrivateKey key = parsePkcs8PrivateKey(privateKeyPem);
      Signature signature = Signature.getInstance("SHA256withRSA");
      signature.initSign(key);
      signature.update(signingInput);
      return signature.sign();
    } catch (Exception e) {
      throw new RuntimeException("Failed to sign GCP JWT assertion", e);
    }
  }

  private static PrivateKey parsePkcs8PrivateKey(String pem) {
    try {
      String normalized =
          pem.replace("-----BEGIN PRIVATE KEY-----", "")
              .replace("-----END PRIVATE KEY-----", "")
              .replaceAll("\\s+", "");
      byte[] der = Base64.getMimeDecoder().decode(normalized);
      PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(der);
      return KeyFactory.getInstance("RSA").generatePrivate(spec);
    } catch (Exception e) {
      throw new RuntimeException("Invalid PKCS#8 private key", e);
    }
  }

  private static String base64Url(byte[] in) {
    return Base64.getUrlEncoder().withoutPadding().encodeToString(in);
  }
}
