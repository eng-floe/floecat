/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.catalog.unity;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.catalog.access.CatalogAuthenticationScheme;
import ai.floedb.floecat.catalog.access.CatalogClient;
import ai.floedb.floecat.catalog.access.CatalogClientProvider;
import ai.floedb.floecat.catalog.access.CatalogConnectionConfig;
import ai.floedb.floecat.catalog.access.CatalogProtocol;
import ai.floedb.floecat.catalog.access.ResolvedCatalogCredentials;
import ai.floedb.floecat.client.unity.HttpUnityCatalogClient;
import ai.floedb.floecat.client.unity.UnityCatalogAuthentication;
import ai.floedb.floecat.client.unity.UnityCatalogClient;
import ai.floedb.floecat.http.guards.HttpEndpointGuards;
import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Opens Unity Catalog integrations without creating or reading Connector resources. */
public final class UnityCatalogClientProvider implements CatalogClientProvider {
  static final String CONNECT_TIMEOUT_MS = "http.connect.ms";
  static final String READ_TIMEOUT_MS = "http.read.ms";
  static final String VEND_PATH = "unity.temporary-table-vend-path";
  static final String TOKEN_URI = "oauth2-server-uri";
  static final String OAUTH_SCOPE = "scope";
  static final String TOKEN = "token";
  static final String CREDENTIAL = "credential";
  private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(30);

  @FunctionalInterface
  interface ClientFactory {
    UnityCatalogClient create(
        URI endpoint,
        Duration connectTimeout,
        Duration readTimeout,
        UnityCatalogAuthentication authentication,
        String vendPath);
  }

  private final ClientFactory clientFactory;

  public UnityCatalogClientProvider() {
    this(HttpUnityCatalogClient::new);
  }

  UnityCatalogClientProvider(ClientFactory clientFactory) {
    this.clientFactory = Objects.requireNonNull(clientFactory, "clientFactory");
  }

  @Override
  public CatalogProtocol protocol() {
    return CatalogProtocol.UNITY_CATALOG;
  }

  @Override
  public CatalogClient open(
      CatalogConnectionConfig config, ResolvedCatalogCredentials resolvedCredentials) {
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(resolvedCredentials, "resolvedCredentials");
    if (config.protocol() != CatalogProtocol.UNITY_CATALOG) {
      throw new IllegalArgumentException(
          "Unity Catalog provider cannot open protocol=" + config.protocol());
    }
    if (config.authentication().scheme() != CatalogAuthenticationScheme.OAUTH2) {
      // UNSUPPORTED, not INVALID_CONFIGURATION: this is "the SPI does not implement that auth
      // mode", which is a deterministic description of the Integration rather than of one attempt
      // -- exactly what SourceCatalogCredentialVendor's classification reserves the code for, and
      // what makes it a refusal rather than something a retry could clear.
      // CatalogIntegrationAccess.resolve already raises UNSUPPORTED for the other AWS auth modes,
      // so this matches its neighbours. Validation still steps over it per table.
      throw new CatalogAccessException(
          CatalogAccessException.Code.UNSUPPORTED,
          "Unity Catalog requires OAuth2 or bearer authentication");
    }

    Map<String, String> properties = config.properties();
    Duration connectTimeout = duration(properties, CONNECT_TIMEOUT_MS, DEFAULT_CONNECT_TIMEOUT);
    Duration readTimeout = duration(properties, READ_TIMEOUT_MS, DEFAULT_READ_TIMEOUT);
    String vendPath =
        properties.getOrDefault(
            VEND_PATH, HttpUnityCatalogClient.DATABRICKS_TEMPORARY_TABLE_CREDENTIALS_PATH);

    AutoCloseable authenticationOwner = null;
    UnityCatalogAuthentication authentication;
    String token = nonBlank(resolvedCredentials.properties().get(TOKEN));
    String credential = nonBlank(resolvedCredentials.properties().get(CREDENTIAL));
    if (token != null && credential != null) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "Unity Catalog authentication must supply token or client credentials, not both");
    }
    if (token != null) {
      authentication = bearer(token, resolvedCredentials.headers());
    } else if (credential != null) {
      UnityOAuthTokenProvider tokens =
          new UnityOAuthTokenProvider(
              tokenUri(config), credential, config.authentication().properties().get(OAUTH_SCOPE));
      authenticationOwner = tokens;
      authentication = bearer(tokens, resolvedCredentials.headers());
    } else {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "Unity Catalog authentication credentials are not configured");
    }

    UnityCatalogClient unity = null;
    try {
      unity =
          clientFactory.create(
              config.endpoint(), connectTimeout, readTimeout, authentication, vendPath);
      return new UnityCatalogAccessClient(
          unity, authenticationOwner, UnityStorageAccessValidator.s3(), routing(properties));
    } catch (RuntimeException | Error failure) {
      closeQuietly(unity);
      closeQuietly(authenticationOwner);
      throw failure;
    }
  }

  private static UnityCatalogAuthentication bearer(
      String token, Map<String, String> resolvedHeaders) {
    return () -> headers(token, resolvedHeaders);
  }

  private static UnityCatalogAuthentication bearer(
      UnityOAuthTokenProvider tokens, Map<String, String> resolvedHeaders) {
    return () -> headers(tokens.accessToken(), resolvedHeaders);
  }

  private static Map<String, String> headers(String token, Map<String, String> resolvedHeaders) {
    LinkedHashMap<String, String> headers = new LinkedHashMap<>(resolvedHeaders);
    if (headers.keySet().stream().anyMatch("authorization"::equalsIgnoreCase)) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "Unity Catalog Authorization header is controlled by authentication");
    }
    headers.put("Authorization", "Bearer " + token);
    return Map.copyOf(headers);
  }

  private static URI tokenUri(CatalogConnectionConfig config) {
    String configured = nonBlank(config.authentication().properties().get(TOKEN_URI));
    return configured == null
        ? config.endpoint().resolve("/oidc/v1/token")
        : URI.create(configured);
  }

  private static Duration duration(
      Map<String, String> properties, String name, Duration defaultValue) {
    String raw = nonBlank(properties.get(name));
    if (raw == null) {
      return defaultValue;
    }
    try {
      long millis = Long.parseLong(raw);
      if (millis <= 0) {
        throw new NumberFormatException();
      }
      return Duration.ofMillis(millis);
    } catch (NumberFormatException failure) {
      throw new IllegalArgumentException(name + " must be a positive integer", failure);
    }
  }

  /**
   * The S3 routing an operator can set on the integration.
   *
   * <p>No {@code s3.access-point}: nothing addresses one. {@code UnityStorageAccessValidator}
   * deliberately probes the bucket named in the object URI, and {@code
   * SourceCatalogCredentialVendor} strips the key before a credential leaves the service, so an
   * operator who set it saw no effect and no error. Plumbing a key no consumer honours is how one
   * starts being honoured inconsistently.
   *
   * <p>A vended access point is different and still reported: Unity returning one on the
   * credentials response is a diagnostic for a later 403, which {@code noteIgnoredAccessPoint}
   * logs.
   */
  private static Map<String, String> routing(Map<String, String> properties) {
    LinkedHashMap<String, String> routing = new LinkedHashMap<>();
    for (String key : new String[] {"s3.region", "s3.endpoint", "s3.path-style-access"}) {
      String value = nonBlank(properties.get(key));
      if (value != null) {
        routing.put(key, value);
      }
    }
    requireUsableEndpoint(routing.get("s3.endpoint"));
    return Map.copyOf(routing);
  }

  /**
   * Rejects an {@code s3.endpoint} that names somewhere the service should not be made to reach.
   *
   * <p>The policy and its reasoning live in {@link
   * HttpEndpointGuards#requireUsableStorageEndpoint}, which the Delta Sharing provider holds its
   * own storage endpoint to for the same reason: both publish a vend carrying a session token.
   */
  private static void requireUsableEndpoint(String endpoint) {
    try {
      HttpEndpointGuards.requireUsableStorageEndpoint(endpoint, "Unity Catalog s3.endpoint");
    } catch (IllegalArgumentException refused) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION, refused.getMessage(), refused);
    }
  }

  private static String nonBlank(String value) {
    return value == null || value.isBlank() ? null : value.trim();
  }

  private static void closeQuietly(AutoCloseable closeable) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    } catch (Exception ignored) {
    }
  }
}
