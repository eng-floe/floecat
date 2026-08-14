/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.catalog.access.CatalogAuthenticationScheme;
import ai.floedb.floecat.catalog.access.CatalogProtocol;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.integration.rpc.AwsAccessKeyAuthentication;
import ai.floedb.floecat.integration.rpc.AwsAccessKeySecret;
import ai.floedb.floecat.integration.rpc.AwsDefaultAuthentication;
import ai.floedb.floecat.integration.rpc.AwsSigV4Authentication;
import ai.floedb.floecat.integration.rpc.BearerAuthentication;
import ai.floedb.floecat.integration.rpc.CatalogAuthentication;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationCredentials;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationType;
import ai.floedb.floecat.integration.rpc.OAuthClientCredentialsAuthentication;
import ai.floedb.floecat.integration.rpc.SecretValue;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalogIntegrationAccessTest {
  private CatalogIntegrationAccess access;
  private CatalogIntegrationCredentialStore credentials;

  @BeforeEach
  void setUp() {
    access = new CatalogIntegrationAccess();
    credentials = mock(CatalogIntegrationCredentialStore.class);
    access.credentialStore = credentials;
  }

  @Test
  void resolvesOAuthWithoutPuttingSecretsInPersistableConfiguration() {
    var authentication =
        CatalogAuthentication.newBuilder()
            .setOauthClientCredentials(
                OAuthClientCredentialsAuthentication.newBuilder()
                    .setClientId("client")
                    .setTokenUri("https://identity.example/token")
                    .addScopes("catalog")
                    .addScopes("read"))
            .setCredentialsConfigured(true)
            .setCredentialGeneration(1L)
            .build();
    CatalogIntegration integration = integration(authentication);
    when(credentials.resolve(integration))
        .thenReturn(
            Optional.of(
                CatalogIntegrationCredentials.newBuilder()
                    .setOauthClientSecret(SecretValue.newBuilder().setValue("secret"))
                    .build()));

    var resolved = access.resolve(integration);

    assertEquals(CatalogProtocol.ICEBERG_REST, resolved.config().protocol());
    assertEquals(CatalogAuthenticationScheme.OAUTH2, resolved.config().authentication().scheme());
    assertEquals(
        "https://identity.example/token",
        resolved.config().authentication().properties().get("oauth2-server-uri"));
    assertEquals("catalog read", resolved.config().authentication().properties().get("scope"));
    assertEquals("client:secret", resolved.credentials().properties().get("credential"));
  }

  @Test
  void resolvesBearerAndExplicitSigV4SecretsOnlyIntoRuntimeCredentials() {
    var bearer =
        CatalogAuthentication.newBuilder()
            .setBearer(BearerAuthentication.getDefaultInstance())
            .setCredentialsConfigured(true)
            .setCredentialGeneration(2L)
            .build();
    CatalogIntegration bearerIntegration = integration(bearer);
    when(credentials.resolve(bearerIntegration))
        .thenReturn(
            Optional.of(
                CatalogIntegrationCredentials.newBuilder()
                    .setBearerToken(SecretValue.newBuilder().setValue("token"))
                    .build()));
    assertEquals(
        "token", access.resolve(bearerIntegration).credentials().properties().get("token"));

    var sigv4 =
        CatalogAuthentication.newBuilder()
            .setAwsSigv4(
                AwsSigV4Authentication.newBuilder()
                    .setAwsAccessKey(
                        AwsAccessKeyAuthentication.newBuilder().setAccessKeyId("access"))
                    .setRegion("us-east-1"))
            .setCredentialsConfigured(true)
            .setCredentialGeneration(3L)
            .build();
    CatalogIntegration sigv4Integration = integration(sigv4);
    when(credentials.resolve(sigv4Integration))
        .thenReturn(
            Optional.of(
                CatalogIntegrationCredentials.newBuilder()
                    .setAwsAccessKey(
                        AwsAccessKeySecret.newBuilder()
                            .setSecretAccessKey("secret")
                            .setSessionToken("session"))
                    .build()));
    var resolved = access.resolve(sigv4Integration);
    assertEquals(
        CatalogAuthenticationScheme.AWS_SIGV4, resolved.config().authentication().scheme());
    assertEquals("access", resolved.credentials().properties().get("rest.access-key-id"));
    assertEquals("secret", resolved.credentials().properties().get("rest.secret-access-key"));
    assertEquals("session", resolved.credentials().properties().get("rest.session-token"));
  }

  @Test
  void rejectsAmbientSigV4WithoutConnectorFallback() {
    var authentication =
        CatalogAuthentication.newBuilder()
            .setAwsSigv4(
                AwsSigV4Authentication.newBuilder()
                    .setAwsDefault(AwsDefaultAuthentication.getDefaultInstance())
                    .setRegion("us-east-1"))
            .build();

    CatalogAccessException error =
        assertThrows(
            CatalogAccessException.class, () -> access.resolve(integration(authentication)));

    assertEquals(CatalogAccessException.Code.UNSUPPORTED, error.code());
  }

  @Test
  void reportsUnreadableStoredCredentialsAsInternalState() {
    var authentication =
        CatalogAuthentication.newBuilder()
            .setBearer(BearerAuthentication.getDefaultInstance())
            .setCredentialsConfigured(true)
            .setCredentialGeneration(1L)
            .build();
    CatalogIntegration integration = integration(authentication);
    when(credentials.resolve(integration)).thenThrow(new IllegalStateException("corrupt blob"));

    CatalogAccessException error =
        assertThrows(CatalogAccessException.class, () -> access.open(integration));

    assertEquals(CatalogAccessException.Code.INTERNAL, error.code());
  }

  private static CatalogIntegration integration(CatalogAuthentication authentication) {
    return CatalogIntegration.newBuilder()
        .setResourceId(
            ResourceId.newBuilder()
                .setAccountId("acct")
                .setId("integration")
                .setKind(ResourceKind.RK_CATALOG_INTEGRATION))
        .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
        .setCatalogUri("https://catalog.example/v1")
        .setAuthentication(authentication)
        .build();
  }
}
