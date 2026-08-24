/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.integration;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.integration.rpc.CatalogAuthentication;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationCredentials;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;

@ApplicationScoped
public class CatalogIntegrationCredentialStore {
  static final String SECRET_TYPE = "catalog-integrations";

  @Inject SecretsManager secretsManager;

  public void store(
      ResourceId integrationId,
      long credentialGeneration,
      CatalogIntegrationCredentials credentials) {
    if (credentials.getCredentialCase()
        == CatalogIntegrationCredentials.CredentialCase.CREDENTIAL_NOT_SET) {
      return;
    }
    byte[] payload = credentials.toByteArray();
    String reference = reference(integrationId, credentialGeneration);
    if (secretsManager.get(integrationId.getAccountId(), SECRET_TYPE, reference).isPresent()) {
      return;
    }
    boolean stored;
    try {
      stored =
          secretsManager.putIfAbsent(
              integrationId.getAccountId(), SECRET_TYPE, reference, payload);
    } catch (RuntimeException failure) {
      if (secretsManager.get(integrationId.getAccountId(), SECRET_TYPE, reference).isEmpty()) {
        throw failure;
      }
      return;
    }
    if (!stored
        && secretsManager.get(integrationId.getAccountId(), SECRET_TYPE, reference).isEmpty()) {
      throw new BaseResourceRepository.AbortRetryableException(
          "Catalog integration credential generation is not yet available");
    }
  }

  public long storeRotation(
      ResourceId integrationId, long minimumGeneration, CatalogIntegrationCredentials credentials) {
    if (credentials.getCredentialCase()
        == CatalogIntegrationCredentials.CredentialCase.CREDENTIAL_NOT_SET) {
      return 0L;
    }
    byte[] payload = credentials.toByteArray();
    long generation = minimumGeneration;
    while (generation > 0L) {
      String reference = reference(integrationId, generation);
      var existing = secretsManager.get(integrationId.getAccountId(), SECRET_TYPE, reference);
      if (existing.isPresent()) {
        generation++;
        continue;
      }
      try {
        if (secretsManager.putIfAbsent(
            integrationId.getAccountId(), SECRET_TYPE, reference, payload)) {
          return generation;
        }
        generation++;
      } catch (RuntimeException failure) {
        existing = secretsManager.get(integrationId.getAccountId(), SECRET_TYPE, reference);
        if (existing.isEmpty()) throw failure;
        generation++;
      }
    }
    throw new IllegalStateException("Catalog integration credential generation exhausted");
  }

  public Optional<CatalogIntegrationCredentials> resolve(CatalogIntegration integration) {
    if (!hasStoredCredentials(integration)) return Optional.empty();
    String reference =
        reference(
            integration.getResourceId(), integration.getAuthentication().getCredentialGeneration());
    return secretsManager
        .get(integration.getResourceId().getAccountId(), SECRET_TYPE, reference)
        .map(CatalogIntegrationCredentialStore::parse)
        .map(credentials -> requireCompatible(integration.getAuthentication(), credentials));
  }

  public void deleteImmediately(ResourceId integrationId, long credentialGeneration) {
    secretsManager.deleteImmediately(
        integrationId.getAccountId(), SECRET_TYPE, reference(integrationId, credentialGeneration));
  }

  static String reference(ResourceId integrationId, long credentialGeneration) {
    if (credentialGeneration <= 0L) {
      throw new IllegalArgumentException("credential generation must be positive");
    }
    return integrationId.getId() + ".credentials." + Long.toUnsignedString(credentialGeneration);
  }

  static boolean hasStoredCredentials(CatalogIntegration integration) {
    return integration.hasAuthentication()
        && integration.getAuthentication().getCredentialsConfigured()
        && usesStoredCredentials(integration.getAuthentication());
  }

  private static boolean usesStoredCredentials(CatalogAuthentication authentication) {
    return switch (authentication.getConfigurationCase()) {
      case OAUTH_CLIENT_CREDENTIALS, BEARER, AWS_ACCESS_KEY -> true;
      case AWS_SIGV4 ->
          authentication.getAwsSigv4().getCredentialsCase()
              == ai.floedb.floecat.integration.rpc.AwsSigV4Authentication.CredentialsCase
                  .AWS_ACCESS_KEY;
      case AWS_ASSUME_ROLE, CONFIGURATION_NOT_SET -> false;
    };
  }

  private static CatalogIntegrationCredentials parse(byte[] payload) {
    try {
      return CatalogIntegrationCredentials.parseFrom(payload);
    } catch (Exception failure) {
      throw new IllegalStateException("Failed to parse catalog integration credentials", failure);
    }
  }

  private static CatalogIntegrationCredentials requireCompatible(
      CatalogAuthentication authentication, CatalogIntegrationCredentials credentials) {
    CatalogIntegrationCredentials.CredentialCase expected =
        switch (authentication.getConfigurationCase()) {
          case OAUTH_CLIENT_CREDENTIALS ->
              CatalogIntegrationCredentials.CredentialCase.OAUTH_CLIENT_SECRET;
          case BEARER -> CatalogIntegrationCredentials.CredentialCase.BEARER_TOKEN;
          case AWS_ACCESS_KEY -> CatalogIntegrationCredentials.CredentialCase.AWS_ACCESS_KEY;
          case AWS_SIGV4 ->
              authentication.getAwsSigv4().getCredentialsCase()
                      == ai.floedb.floecat.integration.rpc.AwsSigV4Authentication.CredentialsCase
                          .AWS_ACCESS_KEY
                  ? CatalogIntegrationCredentials.CredentialCase.AWS_ACCESS_KEY
                  : CatalogIntegrationCredentials.CredentialCase.CREDENTIAL_NOT_SET;
          case AWS_ASSUME_ROLE, CONFIGURATION_NOT_SET ->
              CatalogIntegrationCredentials.CredentialCase.CREDENTIAL_NOT_SET;
        };
    if (credentials.getCredentialCase() != expected) {
      throw new IllegalStateException(
          "Stored catalog integration credentials do not match authentication configuration");
    }
    return credentials;
  }
}
