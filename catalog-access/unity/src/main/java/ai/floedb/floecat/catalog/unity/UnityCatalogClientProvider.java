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
   * <p>Checked when the client is opened, before anything connects. The value is tenant-supplied
   * and reaches {@code endpointOverride}, so validation would otherwise issue signed S3 requests
   * wherever it pointed, and the same value travels on as client-safe routing to reconcile and
   * query workers. The catalog and OAuth endpoints in this same integration are already held to
   * this policy; this one was not.
   *
   * <p>HTTPS unless a deployment says otherwise. The earlier rule allowed cleartext outright, on
   * the reasoning that an S3 request carries a SigV4 signature rather than a bearer secret -- which
   * stopped being true here: this provider refuses to publish a vend without {@code
   * s3.session-token}, so every signed request carries that token in {@code X-Amz-Security-Token},
   * and a session token is replayable against the table's prefix by anyone who sees it for as long
   * as it lives. The exposure is not confined to the validation probe either, because {@code
   * s3.endpoint} travels on to query workers as client-safe routing.
   *
   * <p>The escape hatch stays, because an S3-compatible endpoint on a private network commonly is
   * HTTP -- MinIO and LocalStack both -- but it is now a deployment saying so rather than a
   * default: {@value #ALLOW_CLEARTEXT_S3_PROPERTY}, or the environment variable {@value
   * #ALLOW_CLEARTEXT_S3_ENV}. The address-class rule still applies on top of either scheme: it is
   * the one that refuses {@code 169.254.169.254}.
   */
  static final String ALLOW_CLEARTEXT_S3_PROPERTY = "floecat.security.allow-cleartext-s3-endpoints";

  static final String ALLOW_CLEARTEXT_S3_ENV = "FLOECAT_SECURITY_ALLOW_CLEARTEXT_S3_ENDPOINTS";

  private static boolean allowCleartextS3Endpoints() {
    return Boolean.parseBoolean(
        System.getProperty(
            ALLOW_CLEARTEXT_S3_PROPERTY,
            System.getenv().getOrDefault(ALLOW_CLEARTEXT_S3_ENV, "false")));
  }

  private static void requireUsableEndpoint(String endpoint) {
    if (endpoint == null) {
      return;
    }
    URI uri;
    try {
      uri = URI.create(endpoint);
    } catch (IllegalArgumentException malformed) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "Unity Catalog s3.endpoint is not a valid URI",
          malformed);
    }
    String scheme = uri.getScheme();
    if (!uri.isAbsolute()
        || uri.getHost() == null
        || !("https".equalsIgnoreCase(scheme) || "http".equalsIgnoreCase(scheme))
        || uri.getRawUserInfo() != null
        || uri.getRawQuery() != null
        || uri.getRawFragment() != null) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "Unity Catalog s3.endpoint must be an absolute http or https URI with no userinfo,"
              + " query or fragment");
    }
    if ("http".equalsIgnoreCase(scheme) && !allowCleartextS3Endpoints()) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "Unity Catalog s3.endpoint must use HTTPS: a vended credential carries a session token,"
              + " which travels in a header and is replayable. Set "
              + ALLOW_CLEARTEXT_S3_ENV
              + "=true to allow cleartext on a trusted network");
    }
    try {
      HttpUnityCatalogClient.assertEndpointAddressAllowed(uri);
    } catch (IllegalArgumentException refused) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "Unity Catalog s3.endpoint names an address class that is not allowed",
          refused);
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
