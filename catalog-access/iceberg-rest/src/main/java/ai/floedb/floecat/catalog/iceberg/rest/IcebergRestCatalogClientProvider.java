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

package ai.floedb.floecat.catalog.iceberg.rest;

import ai.floedb.floecat.catalog.access.CatalogClient;
import ai.floedb.floecat.catalog.access.CatalogClientProvider;
import ai.floedb.floecat.catalog.access.CatalogConnectionConfig;
import ai.floedb.floecat.catalog.access.CatalogProtocol;
import ai.floedb.floecat.catalog.access.ResolvedCatalogCredentials;
import ai.floedb.floecat.catalog.iceberg.rest.auth.AwsCredentialScope;
import ai.floedb.floecat.catalog.iceberg.rest.auth.CatalogSigV4AuthManager;
import ai.floedb.floecat.catalog.iceberg.rest.auth.RefreshingAwsCredentialsRegistry;
import ai.floedb.floecat.catalog.iceberg.rest.auth.RegistryBackedAwsCredentialsProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.RESTUtil;

/** Opens Iceberg REST catalogs without any dependency on Connector resources or RPC contracts. */
public final class IcebergRestCatalogClientProvider implements CatalogClientProvider {
  private static final String ACCESS_DELEGATION_HEADER = "X-Iceberg-Access-Delegation";
  private static final String ACCESS_DELEGATION_HEADER_PROPERTY =
      "header." + ACCESS_DELEGATION_HEADER;
  private static final String VENDED_CREDENTIALS = "vended-credentials";
  static final String DEFAULT_S3_FILE_IO = "org.apache.iceberg.aws.s3.S3FileIO";
  private static final AwsCredentialKeys CATALOG_AWS_KEYS =
      new AwsCredentialKeys(
          RefreshingAwsCredentialsRegistry.CATALOG_PROVIDER_ID,
          "rest",
          "AWS SigV4 requires catalog access/secret keys or a renewable catalog provider");
  private static final AwsCredentialKeys STORAGE_AWS_KEYS =
      new AwsCredentialKeys(
          RefreshingAwsCredentialsRegistry.STORAGE_PROVIDER_ID,
          "s3",
          "AWS storage credentials require both s3.access-key-id and s3.secret-access-key");
  private static final Set<String> OAUTH_CREDENTIAL_PROPERTIES = Set.of("token", "credential");
  private static final Set<String> AWS_CREDENTIAL_PROPERTIES =
      Stream.of(CATALOG_AWS_KEYS, STORAGE_AWS_KEYS)
          .flatMap(keys -> keys.propertyNames().stream())
          .collect(Collectors.toUnmodifiableSet());
  private static final Set<String> CONTROLLED_CONNECTION_PROPERTIES =
      Stream.concat(
              Stream.of("token", "credential", "rest.auth.type", "rest.sigv4-enabled"),
              AWS_CREDENTIAL_PROPERTIES.stream())
          .collect(Collectors.toUnmodifiableSet());

  @Override
  public CatalogProtocol protocol() {
    return CatalogProtocol.ICEBERG_REST;
  }

  @Override
  public CatalogClient open(
      CatalogConnectionConfig config, ResolvedCatalogCredentials resolvedCredentials) {
    if (config.protocol() != CatalogProtocol.ICEBERG_REST) {
      throw new IllegalArgumentException(
          "Iceberg REST provider cannot open protocol=" + config.protocol());
    }

    Map<String, String> properties = catalogProperties(config, resolvedCredentials);
    RESTSessionCatalog sessionCatalog =
        IcebergRestCatalogErrors.call(
            "client initialization", () -> createSessionCatalog(properties));
    try {
      SessionCatalog.SessionContext context = SessionCatalog.SessionContext.createEmpty();
      Catalog catalog = sessionCatalog.asCatalog(context);
      ViewCatalog viewCatalog = sessionCatalog.asViewCatalog(context);
      if (!(catalog instanceof SupportsNamespaces namespaces)) {
        throw new IllegalStateException("Iceberg REST catalog does not support namespaces");
      }
      return new IcebergRestCatalogClient(
          catalog,
          namespaces,
          viewCatalog,
          () -> closeCatalog(sessionCatalog),
          IcebergRestCatalogClient.storageRoutingProperties(properties));
    } catch (RuntimeException | Error e) {
      closeCatalog(sessionCatalog);
      if (e instanceof RuntimeException runtimeException) {
        throw IcebergRestCatalogErrors.translate("client initialization", runtimeException);
      }
      throw e;
    }
  }

  static Map<String, String> catalogProperties(
      CatalogConnectionConfig config, ResolvedCatalogCredentials resolvedCredentials) {
    String endpointScheme = config.endpoint().getScheme();
    if (!"http".equalsIgnoreCase(endpointScheme) && !"https".equalsIgnoreCase(endpointScheme)) {
      throw new IllegalArgumentException(
          "Iceberg REST endpoint must use http or https: scheme=" + endpointScheme);
    }
    rejectResolvedOnlyProperties(config.properties());
    rejectResolvedOnlyProperties(config.authentication().properties());
    Map<String, String> properties = new HashMap<>(config.properties());

    properties.putAll(config.authentication().properties());
    properties.put(CatalogProperties.URI, config.endpoint().toString());
    resolvedCredentials
        .headers()
        .forEach(
            (name, value) -> {
              if (ACCESS_DELEGATION_HEADER.equalsIgnoreCase(name)) {
                throw new IllegalArgumentException(
                    ACCESS_DELEGATION_HEADER + " is controlled by the Iceberg REST provider");
              }
              properties.put("header." + name, value);
            });
    properties.put(ACCESS_DELEGATION_HEADER_PROPERTY, VENDED_CREDENTIALS);

    switch (config.authentication().scheme()) {
      case NONE -> {
        if (!resolvedCredentials.isEmpty()) {
          throw new IllegalArgumentException(
              "Authentication scheme none does not accept resolved credentials");
        }
      }
      case OAUTH2 -> {
        Set<String> unsupportedCredentials =
            resolvedCredentials.properties().keySet().stream()
                .filter(name -> !OAUTH_CREDENTIAL_PROPERTIES.contains(name))
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        if (!unsupportedCredentials.isEmpty()) {
          throw new IllegalArgumentException(
              "Unsupported OAuth2 credential properties: " + unsupportedCredentials);
        }
        String token = resolvedCredentials.properties().get("token");
        String credential = resolvedCredentials.properties().get("credential");
        if ((token == null || token.isBlank()) && (credential == null || credential.isBlank())) {
          throw new IllegalArgumentException(
              "OAuth2 requires resolved token or credential material");
        }
        properties.put("rest.auth.type", "oauth2");
        if (token != null && !token.isBlank()) {
          properties.put("token", token);
        }
        if (credential != null && !credential.isBlank()) {
          properties.put("credential", credential);
        }
      }
      case AWS_SIGV4 -> applyAwsSigV4(properties, resolvedCredentials.properties());
    }
    return Map.copyOf(properties);
  }

  private static void applyAwsSigV4(
      Map<String, String> properties, Map<String, String> credentialProperties) {
    Set<String> unsupportedCredentials =
        credentialProperties.keySet().stream()
            .filter(name -> !AWS_CREDENTIAL_PROPERTIES.contains(name))
            .collect(java.util.stream.Collectors.toUnmodifiableSet());
    if (!unsupportedCredentials.isEmpty()) {
      throw new IllegalArgumentException(
          "Unsupported AWS SigV4 credential properties: " + unsupportedCredentials);
    }

    AwsCredentialInput catalogCredentials =
        AwsCredentialInput.from(credentialProperties, CATALOG_AWS_KEYS);
    catalogCredentials.validate(true);
    AwsCredentialInput storageCredentials =
        AwsCredentialInput.from(credentialProperties, STORAGE_AWS_KEYS);
    storageCredentials.validate(false);

    properties.put("rest.auth.type", CatalogSigV4AuthManager.class.getName());
    copyIfAbsent(properties, "rest.signing-name", "signing-name");
    copyFirstIfAbsent(
        properties, "rest.signing-region", "signing-region", "s3.region", "client.region");

    if (catalogCredentials.renewable()) {
      properties.put(
          RefreshingAwsCredentialsRegistry.CATALOG_PROVIDER_ID, catalogCredentials.providerId());
    } else {
      catalogCredentials.copyStaticTo(properties);
    }

    if (storageCredentials.renewable()) {
      properties.put(
          RefreshingAwsCredentialsRegistry.STORAGE_PROVIDER_ID, storageCredentials.providerId());
      properties.put(
          "client.credentials-provider", RegistryBackedAwsCredentialsProvider.class.getName());
      properties.put(
          "client.credentials-provider.floecat-provider-id", storageCredentials.providerId());
      properties.put(
          "client.credentials-provider.floecat-credential-scope",
          AwsCredentialScope.STORAGE.name());
    } else {
      storageCredentials.copyStaticTo(properties);
    }
    if (storageCredentials.configured()) {
      properties.putIfAbsent("io-impl", DEFAULT_S3_FILE_IO);
    }
    normalizeAwsRegion(properties);
  }

  private static void rejectResolvedOnlyProperties(Map<String, String> properties) {
    for (String name : properties.keySet()) {
      if (name.startsWith("client.credentials-provider")) {
        throw new IllegalArgumentException(
            "client.credentials-provider is controlled by resolved credentials");
      }
      if (name.regionMatches(true, 0, "header.", 0, "header.".length())) {
        throw new IllegalArgumentException(
            "HTTP headers are controlled by resolved credentials, not connection properties");
      }
      if (CONTROLLED_CONNECTION_PROPERTIES.contains(name)) {
        throw new IllegalArgumentException(
            name
                + " is controlled by authentication and resolved credentials, not connection"
                + " properties");
      }
    }
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  private static void copyIfAbsent(Map<String, String> properties, String target, String source) {
    if (!properties.containsKey(target) && properties.containsKey(source)) {
      properties.put(target, properties.get(source));
    }
  }

  private static void copyFirstIfAbsent(
      Map<String, String> properties, String target, String... sources) {
    if (properties.containsKey(target)) {
      return;
    }
    for (String source : sources) {
      if (properties.containsKey(source)) {
        properties.put(target, properties.get(source));
        return;
      }
    }
  }

  private static void normalizeAwsRegion(Map<String, String> properties) {
    String region = properties.get("client.region");
    if (isBlank(region)) {
      region = properties.get("s3.region");
    }
    if (isBlank(region)) {
      return;
    }
    properties.putIfAbsent("client.region", region);
    properties.putIfAbsent("s3.region", region);
  }

  private static RESTSessionCatalog createSessionCatalog(Map<String, String> properties) {
    RESTSessionCatalog catalog =
        new RESTSessionCatalog(
            config ->
                HTTPClient.builder(config)
                    .uri(config.get(CatalogProperties.URI))
                    .withHeaders(RESTUtil.configHeaders(config))
                    .build(),
            null);
    try {
      catalog.initialize("floecat-catalog-access", properties);
      return catalog;
    } catch (RuntimeException | Error e) {
      closeCatalog(catalog);
      throw e;
    }
  }

  private static void closeCatalog(RESTSessionCatalog catalog) {
    try {
      catalog.close();
    } catch (Exception ignored) {
    }
  }

  private record AwsCredentialKeys(
      String providerIdProperty, String propertyPrefix, String incompleteMessage) {
    private String accessKeyProperty() {
      return propertyPrefix + ".access-key-id";
    }

    private String secretKeyProperty() {
      return propertyPrefix + ".secret-access-key";
    }

    private String sessionTokenProperty() {
      return propertyPrefix + ".session-token";
    }

    private Set<String> propertyNames() {
      return Set.of(
          providerIdProperty, accessKeyProperty(), secretKeyProperty(), sessionTokenProperty());
    }
  }

  private record AwsCredentialInput(
      AwsCredentialKeys keys,
      String providerId,
      String accessKey,
      String secretKey,
      String sessionToken) {
    private static AwsCredentialInput from(Map<String, String> properties, AwsCredentialKeys keys) {
      return new AwsCredentialInput(
          keys,
          properties.get(keys.providerIdProperty()),
          properties.get(keys.accessKeyProperty()),
          properties.get(keys.secretKeyProperty()),
          properties.get(keys.sessionTokenProperty()));
    }

    private void validate(boolean required) {
      boolean hasProvider = !isBlank(providerId);
      boolean hasAccessKey = !isBlank(accessKey);
      boolean hasSecretKey = !isBlank(secretKey);
      boolean hasSessionToken = !isBlank(sessionToken);
      boolean hasStaticCredential = hasAccessKey || hasSecretKey || hasSessionToken;

      if (hasProvider && hasStaticCredential) {
        throw new IllegalArgumentException(
            "AWS credentials must use either a renewable provider or static credentials, not both");
      }
      if (hasProvider) {
        return;
      }
      if ((required || hasStaticCredential) && (!hasAccessKey || !hasSecretKey)) {
        throw new IllegalArgumentException(keys.incompleteMessage());
      }
    }

    private boolean renewable() {
      return !isBlank(providerId);
    }

    private boolean configured() {
      return renewable() || (!isBlank(accessKey) && !isBlank(secretKey));
    }

    private void copyStaticTo(Map<String, String> properties) {
      putIfPresent(properties, keys.accessKeyProperty(), accessKey);
      putIfPresent(properties, keys.secretKeyProperty(), secretKey);
      putIfPresent(properties, keys.sessionTokenProperty(), sessionToken);
    }

    private static void putIfPresent(Map<String, String> properties, String name, String value) {
      if (!isBlank(value)) {
        properties.put(name, value);
      }
    }
  }
}
