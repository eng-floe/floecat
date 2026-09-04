/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.catalog.access.CatalogAuthenticationScheme;
import ai.floedb.floecat.catalog.access.CatalogClientFactory;
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
import java.util.List;
import java.util.Map;
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
    access.defaultRegion = "eu-west-1";
  }

  /**
   * A provider module cannot see the deployment's configuration, so the region has to arrive in the
   * connection properties. Without it the Unity storage validator substituted us-east-1 of its own
   * and reported storage failure for an ordinary bucket elsewhere, while the read path -- which
   * does consult this property -- worked, making s3.region effectively required.
   */
  @Test
  void fillsInTheDeploymentRegionWhenAUnityIntegrationStatesNone() {
    var integration = unityIntegration(Map.of());

    var resolved = access.resolve(integration);

    assertEquals("eu-west-1", resolved.config().properties().get("s3.region"));
  }

  /**
   * Unity only. The Iceberg REST provider reads the same map and turns s3.region into
   * client.region, so defaulting it there would pin a region on an integration that deliberately
   * set none and was relying on the AWS SDK's own resolution chain.
   */
  @Test
  void leavesAnIcebergRestIntegrationToTheSdkResolutionChain() {
    var integration = bearerIntegration();

    var resolved = access.resolve(integration);

    assertNull(resolved.config().properties().get("s3.region"));
  }

  /** An integration that states a region keeps it: the default is a fallback, not an override. */
  @Test
  void doesNotOverrideARegionTheIntegrationStates() {
    var integration = unityIntegration(Map.of("s3.region", "ap-south-1"));

    var resolved = access.resolve(integration);

    assertEquals("ap-south-1", resolved.config().properties().get("s3.region"));
  }

  /**
   * However the operator spelled it. Testing s3.region alone was worse than doing nothing: the
   * injected default does not stay in the validator, and routingProperties reads the vended map
   * before the connector's aliases -- so a region written as aws.region, region or client.region
   * was silently replaced by the deployment's, and the bucket read against the wrong one.
   */
  @Test
  void carriesARegionStatedUnderAnyAliasRatherThanDefaultingOverIt() {
    for (String alias : List.of("s3.region", "region", "client.region", "aws.region")) {
      var integration = unityIntegration(Map.of(alias, "ap-south-1"));

      var resolved = access.resolve(integration);

      assertEquals(
          "ap-south-1",
          resolved.config().properties().get("s3.region"),
          alias + " must decide the region, not the deployment default");
    }
  }

  /** A blank alias states nothing, so the deployment default still applies. */
  @Test
  void aBlankRegionAliasIsNotAStatedRegion() {
    var integration = unityIntegration(Map.of("aws.region", "   "));

    var resolved = access.resolve(integration);

    assertEquals("eu-west-1", resolved.config().properties().get("s3.region"));
  }

  private CatalogIntegration bearerIntegration() {
    return bearerIntegration(Map.of());
  }

  private CatalogIntegration unityIntegration(Map<String, String> properties) {
    return bearerIntegration(properties, CatalogIntegrationType.CIT_UNITY);
  }

  private CatalogIntegration bearerIntegration(Map<String, String> properties) {
    return bearerIntegration(properties, CatalogIntegrationType.CIT_ICEBERG_REST);
  }

  private CatalogIntegration bearerIntegration(
      Map<String, String> properties, CatalogIntegrationType type) {
    var authentication =
        CatalogAuthentication.newBuilder()
            .setBearer(BearerAuthentication.getDefaultInstance())
            .setCredentialsConfigured(true)
            .setCredentialGeneration(1L)
            .build();
    // Properties before the stub: resolve() is stubbed for this exact message, so mutating the
    // integration afterwards would leave the stub matching a different one.
    CatalogIntegration integration =
        integration(authentication).toBuilder().setType(type).putAllProperties(properties).build();
    when(credentials.resolve(integration))
        .thenReturn(
            Optional.of(
                CatalogIntegrationCredentials.newBuilder()
                    .setBearerToken(SecretValue.newBuilder().setValue("token"))
                    .build()));
    return integration;
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
    CatalogIntegration integration =
        integration(authentication).toBuilder()
            .putProperties("warehouse", "polaris-catalog")
            .putProperties("s3.region", "us-east-1")
            .build();
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
    assertEquals(
        java.util.Map.of("warehouse", "polaris-catalog", "s3.region", "us-east-1"),
        resolved.config().properties());
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
  void aRecordedGenerationThatIsMomentarilyUnreadableIsRetryable() {
    // The window CatalogIntegrationCredentialCleanup opens while a secret generation is superseded.
    // It closes on the next attempt, so a caller that gives up here permanently fails work that
    // would have succeeded.
    var authentication =
        CatalogAuthentication.newBuilder()
            .setBearer(BearerAuthentication.getDefaultInstance())
            .setCredentialsConfigured(true)
            .setCredentialGeneration(1L)
            .build();
    CatalogIntegration integration = integration(authentication);
    when(credentials.resolve(integration)).thenReturn(java.util.Optional.empty());

    CatalogAccessException error =
        assertThrows(CatalogAccessException.class, () -> access.open(integration));

    assertEquals(CatalogAccessException.Code.CREDENTIAL_UNAVAILABLE, error.code());
  }

  @Test
  void permanentCredentialLossKeepsReportingWhileASupersedeWindowGoesQuiet() {
    // The distinction the WARN exists to make. Reporting a pair once gave both conditions the same
    // shape -- one line, then DEBUG forever, which production does not enable -- so an operator
    // could not tell a superseded generation from a secret that is gone. What separates them is
    // that loss keeps failing at one generation while a supersede window moves to the next.
    java.time.Instant t0 = java.time.Instant.parse("2026-09-04T12:00:00Z");

    assertTrue(access.shouldReportCredentialGap("integration-1@7", t0));
    // The flood this damps: a vend per file group, all inside the interval.
    assertFalse(access.shouldReportCredentialGap("integration-1@7", t0.plusSeconds(1)));
    assertFalse(access.shouldReportCredentialGap("integration-1@7", t0.plusSeconds(119)));

    // Still the same generation after the interval, which is what permanent loss looks like.
    assertTrue(access.shouldReportCredentialGap("integration-1@7", t0.plusSeconds(120)));
    assertFalse(access.shouldReportCredentialGap("integration-1@7", t0.plusSeconds(121)));

    // A supersede window instead moves the generation on, so it reports once and stops rather than
    // repeating at the interval.
    assertTrue(access.shouldReportCredentialGap("integration-1@8", t0.plusSeconds(121)));
  }

  @Test
  void credentialsThatWereNeverConfiguredAreNotRetryable() {
    // Same empty Optional, opposite answer, and the record is what separates them: this one says
    // no credentials were ever attached, so nothing is coming. Reporting "come back" makes a
    // reconcile job spend its whole budget and report exhaustion instead of the cause, and tells
    // an operator validating the integration that it is temporarily unavailable, indefinitely.
    var authentication =
        CatalogAuthentication.newBuilder()
            .setBearer(BearerAuthentication.getDefaultInstance())
            .setCredentialsConfigured(false)
            .build();
    CatalogIntegration integration = integration(authentication);
    when(credentials.resolve(integration)).thenReturn(java.util.Optional.empty());

    CatalogAccessException error =
        assertThrows(CatalogAccessException.class, () -> access.open(integration));

    assertEquals(CatalogAccessException.Code.INVALID_CONFIGURATION, error.code());
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

  @Test
  void reportsMissingUnityProviderAsUnsupported() {
    var authentication =
        CatalogAuthentication.newBuilder()
            .setBearer(BearerAuthentication.getDefaultInstance())
            .setCredentialsConfigured(true)
            .setCredentialGeneration(1L)
            .build();
    CatalogIntegration integration =
        integration(authentication).toBuilder().setType(CatalogIntegrationType.CIT_UNITY).build();
    when(credentials.resolve(integration))
        .thenReturn(
            Optional.of(
                CatalogIntegrationCredentials.newBuilder()
                    .setBearerToken(SecretValue.newBuilder().setValue("token"))
                    .build()));
    access.clientOpener = new CatalogClientFactory(List.of())::open;

    CatalogAccessException error =
        assertThrows(CatalogAccessException.class, () -> access.open(integration));

    assertEquals(CatalogAccessException.Code.UNSUPPORTED, error.code());
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
