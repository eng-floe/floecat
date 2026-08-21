/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.integration;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.catalog.access.CatalogAuthenticationScheme;
import ai.floedb.floecat.catalog.access.CatalogClient;
import ai.floedb.floecat.catalog.access.CatalogClientFactory;
import ai.floedb.floecat.catalog.access.CatalogConnectionConfig;
import ai.floedb.floecat.catalog.access.CatalogProtocol;
import ai.floedb.floecat.catalog.access.ResolvedCatalogCredentials;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationCredentials;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

/** Resolves one persisted Catalog Integration into a short-lived catalog-access client. */
@ApplicationScoped
public class CatalogIntegrationAccess {
  @FunctionalInterface
  interface ClientOpener {
    CatalogClient open(
        CatalogConnectionConfig config, ResolvedCatalogCredentials resolvedCredentials);
  }

  @Inject CatalogIntegrationCredentialStore credentialStore;

  // Package-private so unit tests can install a provider without using ServiceLoader.
  ClientOpener clientOpener = CatalogClientFactory.load()::open;

  public CatalogClient open(CatalogIntegration integration) {
    try {
      var resolved = resolve(integration);
      return clientOpener.open(resolved.config(), resolved.credentials());
    } catch (CatalogAccessException failure) {
      throw failure;
    } catch (IllegalArgumentException failure) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "Catalog Integration configuration is invalid",
          failure);
    } catch (UnsupportedOperationException failure) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.UNSUPPORTED,
          "Catalog Integration configuration is not supported",
          failure);
    } catch (IllegalStateException failure) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INTERNAL,
          "Catalog Integration credentials or provider state is invalid",
          failure);
    }
  }

  ResolvedAccess resolve(CatalogIntegration integration) {
    CatalogProtocol protocol =
        switch (integration.getType()) {
          case CIT_ICEBERG_REST -> CatalogProtocol.ICEBERG_REST;
          case CIT_UNITY -> CatalogProtocol.UNITY_CATALOG;
          case CIT_UNSPECIFIED, UNRECOGNIZED ->
              throw new CatalogAccessException(
                  CatalogAccessException.Code.INVALID_CONFIGURATION,
                  "Catalog Integration type is not configured");
        };

    var persisted = integration.getAuthentication();
    var stored = credentialStore.resolve(integration);
    Map<String, String> authenticationProperties = new LinkedHashMap<>();
    Map<String, String> credentialProperties = new LinkedHashMap<>();
    CatalogAuthenticationScheme scheme;

    switch (persisted.getConfigurationCase()) {
      case OAUTH_CLIENT_CREDENTIALS -> {
        scheme = CatalogAuthenticationScheme.OAUTH2;
        var oauth = persisted.getOauthClientCredentials();
        if (oauth.hasTokenUri()) {
          authenticationProperties.put("oauth2-server-uri", oauth.getTokenUri());
        }
        if (!oauth.getScopesList().isEmpty()) {
          authenticationProperties.put("scope", String.join(" ", oauth.getScopesList()));
        }
        String secret =
            requireStored(stored, CatalogIntegrationCredentials.CredentialCase.OAUTH_CLIENT_SECRET)
                .getOauthClientSecret()
                .getValue();
        credentialProperties.put("credential", oauth.getClientId() + ":" + secret);
      }
      case BEARER -> {
        scheme = CatalogAuthenticationScheme.OAUTH2;
        String token =
            requireStored(stored, CatalogIntegrationCredentials.CredentialCase.BEARER_TOKEN)
                .getBearerToken()
                .getValue();
        credentialProperties.put("token", token);
      }
      case AWS_SIGV4 -> {
        scheme = CatalogAuthenticationScheme.AWS_SIGV4;
        var sigv4 = persisted.getAwsSigv4();
        authenticationProperties.put("signing-region", sigv4.getRegion());
        if (sigv4.hasSigningName()) {
          authenticationProperties.put("signing-name", sigv4.getSigningName());
        }
        if (sigv4.getCredentialsCase()
            != ai.floedb.floecat.integration.rpc.AwsSigV4Authentication.CredentialsCase
                .AWS_ACCESS_KEY) {
          throw new CatalogAccessException(
              CatalogAccessException.Code.UNSUPPORTED,
              "Ambient and assumed AWS Catalog Integration credentials are not supported");
        }
        var secret =
            requireStored(stored, CatalogIntegrationCredentials.CredentialCase.AWS_ACCESS_KEY)
                .getAwsAccessKey();
        credentialProperties.put("rest.access-key-id", sigv4.getAwsAccessKey().getAccessKeyId());
        credentialProperties.put("rest.secret-access-key", secret.getSecretAccessKey());
        if (secret.hasSessionToken()) {
          credentialProperties.put("rest.session-token", secret.getSessionToken());
        }
      }
      case AWS_ASSUME_ROLE, AWS_ACCESS_KEY ->
          throw new CatalogAccessException(
              CatalogAccessException.Code.UNSUPPORTED,
              "Catalog Integration authentication must be OAuth, bearer, or explicit AWS SigV4");
      case CONFIGURATION_NOT_SET ->
          throw new CatalogAccessException(
              CatalogAccessException.Code.INVALID_CONFIGURATION,
              "Catalog Integration authentication is not configured");
      default ->
          throw new CatalogAccessException(
              CatalogAccessException.Code.INVALID_CONFIGURATION,
              "Catalog Integration authentication is not recognized");
    }

    var config =
        new CatalogConnectionConfig(
            protocol,
            URI.create(integration.getCatalogUri()),
            integration.getPropertiesMap(),
            new ai.floedb.floecat.catalog.access.CatalogAuthentication(
                scheme, Map.copyOf(authenticationProperties)));
    return new ResolvedAccess(
        config, new ResolvedCatalogCredentials(Map.copyOf(credentialProperties), Map.of(), null));
  }

  private static CatalogIntegrationCredentials requireStored(
      java.util.Optional<CatalogIntegrationCredentials> stored,
      CatalogIntegrationCredentials.CredentialCase expected) {
    var credentials =
        stored.orElseThrow(
            () ->
                new CatalogAccessException(
                    CatalogAccessException.Code.INVALID_CONFIGURATION,
                    "Catalog Integration credentials are not configured"));
    if (credentials.getCredentialCase() != expected) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "Catalog Integration credentials do not match authentication configuration");
    }
    return credentials;
  }

  record ResolvedAccess(CatalogConnectionConfig config, ResolvedCatalogCredentials credentials) {}
}
