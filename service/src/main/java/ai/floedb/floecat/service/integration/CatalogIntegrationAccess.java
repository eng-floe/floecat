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
  private static final org.jboss.logging.Logger LOG =
      org.jboss.logging.Logger.getLogger(CatalogIntegrationAccess.class);

  @FunctionalInterface
  interface ClientOpener {
    CatalogClient open(
        CatalogConnectionConfig config, ResolvedCatalogCredentials resolvedCredentials);
  }

  /**
   * Integration-and-generation pairs already reported at WARN.
   *
   * <p>This vend runs once per file group on reconcile and once per scan session on query, so an
   * integration whose secret is genuinely gone would otherwise write a WARN per group per attempt
   * per table -- the flood {@code catalogIntegrationFailureStatus} drops to DEBUG for its own
   * retryable answers, and for the same reason.
   *
   * <p>Damped by interval rather than reported once, which is what makes the generation readable as
   * a signal. Reporting a pair once would give permanent loss and a supersede window the same
   * shape: one WARN and then silence, with the repeats at DEBUG, which production does not enable.
   * Re-reporting past {@link #CREDENTIAL_GAP_REPORT_INTERVAL} leaves a supersede window writing the
   * line about once before its generation moves on, while genuine loss keeps writing it at that
   * interval against the same generation. The interval is far longer than the per-file-group vend
   * rate this exists to damp, so the flood is still bounded.
   *
   * <p>Bounded and access-synchronized: the key is tenant-supplied, so an unbounded map would be a
   * slow leak, and eviction only costs a repeated WARN rather than correctness.
   */
  private static final int MAX_REPORTED_CREDENTIAL_GAPS = 64;

  private static final java.time.Duration CREDENTIAL_GAP_REPORT_INTERVAL =
      java.time.Duration.ofMinutes(2);

  private final java.util.Map<String, java.time.Instant> reportedCredentialGaps =
      java.util.Collections.synchronizedMap(
          new java.util.LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(
                java.util.Map.Entry<String, java.time.Instant> eldest) {
              return size() > MAX_REPORTED_CREDENTIAL_GAPS;
            }
          });

  // Package-visible so a test can advance past CREDENTIAL_GAP_REPORT_INTERVAL without sleeping.
  java.time.Clock clock = java.time.Clock.systemUTC();

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
            requireStored(
                    integration,
                    stored,
                    CatalogIntegrationCredentials.CredentialCase.OAUTH_CLIENT_SECRET)
                .getOauthClientSecret()
                .getValue();
        credentialProperties.put("credential", oauth.getClientId() + ":" + secret);
      }
      case BEARER -> {
        scheme = CatalogAuthenticationScheme.OAUTH2;
        String token =
            requireStored(
                    integration, stored, CatalogIntegrationCredentials.CredentialCase.BEARER_TOKEN)
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
            requireStored(
                    integration,
                    stored,
                    CatalogIntegrationCredentials.CredentialCase.AWS_ACCESS_KEY)
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

  private CatalogIntegrationCredentials requireStored(
      CatalogIntegration integration,
      java.util.Optional<CatalogIntegrationCredentials> stored,
      CatalogIntegrationCredentials.CredentialCase expected) {
    var credentials = stored.orElseThrow(() -> credentialsAbsent(integration));
    if (credentials.getCredentialCase() != expected) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "Catalog Integration credentials do not match authentication configuration");
    }
    return credentials;
  }

  /**
   * Why {@code resolve} came back empty, which decides whether a caller should retry.
   *
   * <p>Two structurally different conditions reach the same empty Optional, and callers act on the
   * difference. The record saying no credentials were ever attached is permanent until someone
   * configures them, so a retry only hides the cause behind an exhausted budget. A generation the
   * record does carry but the store cannot read is the window {@code
   * CatalogIntegrationCredentialCleanup} opens while a secret is superseded, and it closes on the
   * next attempt.
   *
   * <p>A rotation whose secret write was lost after the generation was recorded would land in the
   * second branch and retry forever, which is the one case this split does not separate. It is not
   * reachable as written -- {@code CatalogIntegrationsImpl} stores the secret before it persists
   * the generation -- so distinguishing it would mean guarding against an ordering the code does
   * not have.
   */
  private CatalogAccessException credentialsAbsent(CatalogIntegration integration) {
    if (!CatalogIntegrationCredentialStore.hasStoredCredentials(integration)) {
      return new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "Catalog Integration credentials are not configured");
    }
    // Logged because the classification cannot tell the two apart. hasStoredCredentials reads the
    // record, not the store, so every empty resolve against a configured record is retryable -- and
    // an empty resolve means the store holds no entry, which a superseded generation and a secret
    // deleted out of band, lost in a restore, or left behind by a backend migration all produce.
    // The retryable answer is right for the first and wrong forever for the rest.
    //
    // The generation is what separates them in the log: a supersede window writes this line about
    // once and stops as the generation moves on, while permanent loss keeps writing it at
    // CREDENTIAL_GAP_REPORT_INTERVAL against the same generation. That is visible without reading
    // the secret store, which is the part an operator cannot do. Bounding the retryable
    // classification to a window after the record's last update would separate them in the
    // classification too, at the cost of carrying that state.
    String integrationId = integration.getResourceId().getId();
    long generation = integration.getAuthentication().getCredentialGeneration();
    String gap = integrationId + "@" + generation;
    if (shouldReportCredentialGap(gap, clock.instant())) {
      LOG.warnf(
          "Catalog Integration %s has credentials configured at generation %d that the store cannot"
              + " resolve; retrying assumes a superseded generation, which repeats if the secret is"
              + " gone for good",
          integrationId, generation);
    } else {
      LOG.debugf(
          "Catalog Integration %s still cannot resolve generation %d", integrationId, generation);
    }
    return new CatalogAccessException(
        CatalogAccessException.Code.CREDENTIAL_UNAVAILABLE,
        "Catalog Integration credentials are not currently resolvable");
  }

  /**
   * Whether this integration-and-generation pair is due a WARN, recording it when it is.
   *
   * <p>Package-visible so the interval is asserted without capturing log output or sleeping through
   * it. Read and write share one mutex so a burst of concurrent vends reports once rather than once
   * per thread; {@code synchronizedMap} locks on the map it returned, which is this reference.
   */
  boolean shouldReportCredentialGap(String gap, java.time.Instant now) {
    synchronized (reportedCredentialGaps) {
      java.time.Instant reported = reportedCredentialGaps.get(gap);
      if (reported != null && reported.isAfter(now.minus(CREDENTIAL_GAP_REPORT_INTERVAL))) {
        return false;
      }
      reportedCredentialGaps.put(gap, now);
      return true;
    }
  }

  record ResolvedAccess(CatalogConnectionConfig config, ResolvedCatalogCredentials credentials) {}
}
