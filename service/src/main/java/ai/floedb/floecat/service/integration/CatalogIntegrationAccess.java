/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.integration;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.catalog.access.CatalogAuthenticationScheme;
import ai.floedb.floecat.catalog.access.CatalogCapabilities;
import ai.floedb.floecat.catalog.access.CatalogClient;
import ai.floedb.floecat.catalog.access.CatalogClientFactory;
import ai.floedb.floecat.catalog.access.CatalogConnectionConfig;
import ai.floedb.floecat.catalog.access.CatalogObjectName;
import ai.floedb.floecat.catalog.access.CatalogProtocol;
import ai.floedb.floecat.catalog.access.CatalogTable;
import ai.floedb.floecat.catalog.access.CatalogView;
import ai.floedb.floecat.catalog.access.NamespacePath;
import ai.floedb.floecat.catalog.access.ResolvedCatalogCredentials;
import ai.floedb.floecat.catalog.access.VendedStorageCredentials;
import ai.floedb.floecat.catalog.iceberg.rest.auth.AwsCredentialScope;
import ai.floedb.floecat.catalog.iceberg.rest.auth.AwsCredentialValue;
import ai.floedb.floecat.catalog.iceberg.rest.auth.RefreshingAwsCredentialsRegistry;
import ai.floedb.floecat.catalog.iceberg.rest.auth.TerminalCredentialRefreshException;
import ai.floedb.floecat.integration.rpc.AwsSigV4Authentication;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationCredentials;
import ai.floedb.floecat.service.account.impl.AccountAwsExternalIdProvider;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sts.model.MalformedPolicyDocumentException;
import software.amazon.awssdk.services.sts.model.PackedPolicyTooLargeException;
import software.amazon.awssdk.services.sts.model.RegionDisabledException;
import software.amazon.awssdk.services.sts.model.StsException;

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

  @FunctionalInterface
  interface AwsCredentialSourceResolver {
    ResolvedAwsCredentials resolve(String accountId, AwsSigV4Authentication authentication);
  }

  @FunctionalInterface
  interface AwsCredentialRegistrar {
    AwsCredentialRegistration register(Supplier<ResolvedAwsCredentials> resolver);
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
  @Inject CatalogIntegrationAwsCredentialPolicy awsCredentialPolicy;

  /**
   * The region a provider should assume when the integration names none.
   *
   * <p>The same property {@code SourceCatalogCredentialVendor} falls back to, and defaulted here
   * because a provider module cannot see the deployment's configuration. Without it the Unity
   * storage validator substituted {@code us-east-1} of its own: cross-region access is off by
   * default, so validation reported storage failure for an ordinary bucket elsewhere while the read
   * path -- which does consult this property -- worked. That made {@code s3.region} effectively
   * required on every non-{@code us-east-1} integration, which is not what the docs say.
   */
  @org.eclipse.microprofile.config.inject.ConfigProperty(
      name = "floecat.storage.aws.region",
      defaultValue = "us-east-1")
  String defaultRegion;

  // Package-private so unit tests can install a provider without using ServiceLoader.
  ClientOpener clientOpener = CatalogClientFactory.load()::open;

  @Inject CatalogIntegrationAwsCredentialResolver integrationAwsCredentialResolver;
  AwsCredentialSourceResolver awsCredentialSourceResolver =
      (accountId, authentication) ->
          integrationAwsCredentialResolver.resolve(accountId, authentication);
  AwsCredentialRegistrar awsCredentialRegistrar = CatalogIntegrationAccess::registerAwsCredentials;

  public CatalogClient open(CatalogIntegration integration) {
    ResolvedAccess resolved = null;
    try {
      resolved = resolve(integration);
      CatalogClient client = clientOpener.open(resolved.config(), resolved.credentials());
      return resolved.registration() == null
          ? client
          : new RegisteredCatalogClient(client, resolved.registration());
    } catch (CatalogAccessException failure) {
      throw failure;
    } catch (StsException failure) {
      throw translateStsFailure(failure);
    } catch (CatalogIntegrationAwsCredentialResolver.MissingAwsCredentialsException failure) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "AWS credentials are not available for the Catalog Integration",
          failure);
    } catch (CatalogIntegrationAwsCredentialResolver.CredentialWaitTimeoutException failure) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.TIMEOUT,
          "Timed out waiting for AWS Catalog Integration credentials",
          failure);
    } catch (CatalogIntegrationAwsCredentialResolver.CredentialSourceTimeoutException failure) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.TIMEOUT,
          "Timed out resolving AWS source credentials",
          failure);
    } catch (CatalogIntegrationAwsCredentialResolver.ExternalIdNotEnforcedException failure) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.CREDENTIAL_CONFIGURATION_INVALID,
          "The AWS role does not enforce the Floecat-issued external ID",
          failure);
    } catch (CatalogIntegrationAwsCredentialResolver.ExternalIdProbeException failure) {
      throw translateStsFailure((StsException) failure.getCause());
    } catch (AccountAwsExternalIdProvider.AccountMissingException failure) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.CREDENTIAL_CONFIGURATION_INVALID,
          "The Floecat account for this Catalog Integration does not exist",
          failure);
    } catch (SdkClientException failure) {
      if (resolved == null) throw translateSdkClientFailure(failure);
      throw translateClientInitializationFailure(failure);
    } catch (java.util.concurrent.CancellationException failure) {
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
    } finally {
      if (resolved != null
          && resolved.registration() != null
          && resolved.registration().unclaimed()) {
        resolved.registration().close();
      }
    }
  }

  ResolvedAccess resolve(CatalogIntegration integration) {
    CatalogProtocol protocol =
        switch (integration.getType()) {
          case CIT_ICEBERG_REST -> CatalogProtocol.ICEBERG_REST;
          case CIT_UNITY -> CatalogProtocol.UNITY_CATALOG;
          case CIT_DELTA_SHARING -> CatalogProtocol.DELTA_SHARING;
          case CIT_UNSPECIFIED, UNRECOGNIZED ->
              throw new CatalogAccessException(
                  CatalogAccessException.Code.INVALID_CONFIGURATION,
                  "Catalog Integration type is not configured");
        };

    var persisted = integration.getAuthentication();
    var stored = credentialStore.resolve(integration);
    Map<String, String> authenticationProperties = new LinkedHashMap<>();
    Map<String, String> credentialProperties = new LinkedHashMap<>();
    AwsCredentialRegistration registration = null;
    Supplier<ResolvedAwsCredentials> renewableAwsCredentials = null;
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
        switch (sigv4.getCredentialsCase()) {
          case AWS_DEFAULT, AWS_ASSUME_ROLE -> {
            awsCredentialPolicy.requireAllowed(sigv4);
            renewableAwsCredentials =
                () ->
                    awsCredentialSourceResolver.resolve(
                        integration.getResourceId().getAccountId(), sigv4);
          }
          case AWS_ACCESS_KEY -> {
            var secret =
                requireStored(
                        integration,
                        stored,
                        CatalogIntegrationCredentials.CredentialCase.AWS_ACCESS_KEY)
                    .getAwsAccessKey();
            credentialProperties.put(
                "rest.access-key-id", sigv4.getAwsAccessKey().getAccessKeyId());
            credentialProperties.put("rest.secret-access-key", secret.getSecretAccessKey());
            if (secret.hasSessionToken()) {
              credentialProperties.put("rest.session-token", secret.getSessionToken());
            }
          }
          case CREDENTIALS_NOT_SET ->
              throw new CatalogAccessException(
                  CatalogAccessException.Code.INVALID_CONFIGURATION,
                  "AWS SigV4 credential source is not configured");
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

    URI endpoint = URI.create(integration.getCatalogUri());
    Map<String, String> connectionProperties = withDefaultRegion(integration, protocol);
    var config =
        new CatalogConnectionConfig(
            protocol,
            endpoint,
            connectionProperties,
            new ai.floedb.floecat.catalog.access.CatalogAuthentication(
                scheme, Map.copyOf(authenticationProperties)));
    if (renewableAwsCredentials != null) {
      registration = awsCredentialRegistrar.register(renewableAwsCredentials);
      credentialProperties.putAll(registration.properties());
    }
    return new ResolvedAccess(
        config,
        new ResolvedCatalogCredentials(Map.copyOf(credentialProperties), Map.of(), null),
        registration);
  }

  private static CatalogAccessException translateStsFailure(StsException failure) {
    int status = failure.statusCode();
    CatalogAccessException.Code code =
        isStsThrottling(failure)
            ? CatalogAccessException.Code.UNAVAILABLE
            : status == 401
                ? CatalogAccessException.Code.UNAUTHENTICATED
                : status == 403
                    ? CatalogAccessException.Code.PERMISSION_DENIED
                    : status == 429 || status >= 500
                        ? CatalogAccessException.Code.UNAVAILABLE
                        : CatalogAccessException.Code.INVALID_CONFIGURATION;
    return new CatalogAccessException(
        code, "AWS STS could not resolve Catalog Integration credentials", failure);
  }

  static boolean isStsThrottling(StsException failure) {
    if (failure.isThrottlingException()) return true;
    String errorCode =
        failure.awsErrorDetails() == null ? null : failure.awsErrorDetails().errorCode();
    return errorCode != null && errorCode.toLowerCase(java.util.Locale.ROOT).contains("throttl");
  }

  private static CatalogAccessException translateSdkClientFailure(SdkClientException failure) {
    return new CatalogAccessException(
        CatalogAccessException.Code.UNAVAILABLE,
        "AWS credential resolution is temporarily unavailable",
        failure);
  }

  private static CatalogAccessException translateClientInitializationFailure(
      SdkClientException failure) {
    CatalogAccessException.Code code =
        hasTransientIoCause(failure)
            ? CatalogAccessException.Code.UNAVAILABLE
            : CatalogAccessException.Code.INVALID_CONFIGURATION;
    return new CatalogAccessException(code, "Catalog client initialization failed", failure);
  }

  private static boolean hasTransientIoCause(Throwable failure) {
    for (Throwable current = failure; current != null; current = current.getCause()) {
      if (current instanceof java.net.UnknownHostException) return false;
      if (current instanceof java.net.SocketTimeoutException
          || current instanceof java.net.ConnectException
          || current instanceof java.net.SocketException
          || current instanceof java.io.InterruptedIOException
          || current instanceof java.util.concurrent.TimeoutException) return true;
    }
    return false;
  }

  private static RuntimeException translateAwsFailure(RuntimeException failure) {
    for (Throwable current = failure; current != null; current = current.getCause()) {
      if (current instanceof AccountAwsExternalIdProvider.AccountMissingException) {
        return new CatalogAccessException(
            CatalogAccessException.Code.CREDENTIAL_CONFIGURATION_INVALID,
            "The Floecat account for this Catalog Integration no longer exists",
            failure);
      }
    }
    for (Throwable current = failure; current != null; current = current.getCause()) {
      if (current instanceof TerminalCredentialRefreshException) {
        return new CatalogAccessException(
            CatalogAccessException.Code.CREDENTIAL_UNAVAILABLE,
            "AWS credentials for the Catalog Integration can no longer be refreshed",
            failure);
      }
      if (current instanceof StsException sts) return translateRefreshStsFailure(sts, failure);
      if (current
          instanceof CatalogIntegrationAwsCredentialResolver.MissingAwsCredentialsException) {
        return new CatalogAccessException(
            CatalogAccessException.Code.CREDENTIAL_UNAVAILABLE,
            "AWS credentials for the Catalog Integration are temporarily unavailable",
            failure);
      }
      if (current
          instanceof CatalogIntegrationAwsCredentialResolver.CredentialWaitTimeoutException) {
        return new CatalogAccessException(
            CatalogAccessException.Code.TIMEOUT,
            "Timed out waiting for AWS Catalog Integration credentials",
            failure);
      }
      if (current
          instanceof CatalogIntegrationAwsCredentialResolver.CredentialSourceTimeoutException) {
        return new CatalogAccessException(
            CatalogAccessException.Code.TIMEOUT,
            "Timed out resolving AWS source credentials",
            failure);
      }
      if (current
          instanceof CatalogIntegrationAwsCredentialResolver.ExternalIdNotEnforcedException) {
        return new CatalogAccessException(
            CatalogAccessException.Code.CREDENTIAL_CONFIGURATION_INVALID,
            "The AWS role no longer enforces the Floecat-issued external ID",
            failure);
      }
    }
    return failure;
  }

  private static AwsCredentialRegistration registerAwsCredentials(
      Supplier<ResolvedAwsCredentials> resolver) {
    ResolvedAwsCredentials initial = resolver.get();
    var registration =
        RefreshingAwsCredentialsRegistry.register(
            toProviderCredentials(initial),
            () -> {
              try {
                return toProviderCredentials(resolver.get());
              } catch (RuntimeException failure) {
                throw terminalRefreshFailure(failure);
              }
            });
    Map<String, String> properties = new LinkedHashMap<>();
    properties.putAll(
        RefreshingAwsCredentialsRegistry.propertiesFor(registration, AwsCredentialScope.CATALOG));
    return new AwsCredentialRegistration(Map.copyOf(properties), registration);
  }

  static RuntimeException terminalRefreshFailure(RuntimeException failure) {
    if (failure instanceof TerminalCredentialRefreshException) return failure;
    for (Throwable current = failure; current != null; current = current.getCause()) {
      if (current instanceof AccountAwsExternalIdProvider.AccountMissingException) {
        return new TerminalCredentialRefreshException(
            "The Floecat account for this Catalog Integration no longer exists", failure);
      }
      if (current
          instanceof CatalogIntegrationAwsCredentialResolver.ExternalIdNotEnforcedException) {
        return new TerminalCredentialRefreshException(
            "The AWS role no longer enforces the Floecat-issued external ID", failure);
      }
      if (current instanceof StsException sts && isTerminalStsRefreshFailure(sts)) {
        return new TerminalCredentialRefreshException(
            "AWS STS rejected credential refresh", failure);
      }
    }
    return failure;
  }

  private static boolean isTerminalStsRefreshFailure(StsException failure) {
    if (failure instanceof MalformedPolicyDocumentException
        || failure instanceof PackedPolicyTooLargeException
        || failure instanceof RegionDisabledException) return true;
    if (isStsThrottling(failure) || failure.awsErrorDetails() == null) return false;
    String errorCode = failure.awsErrorDetails().errorCode();
    if (errorCode == null) return false;
    return switch (errorCode) {
      case "AccessDenied", "AccessDeniedException" -> true;
      default -> false;
    };
  }

  private static CatalogAccessException translateRefreshStsFailure(
      StsException sts, RuntimeException failure) {
    if (isStsThrottling(sts) || sts.statusCode() == 429 || sts.statusCode() >= 500) {
      return new CatalogAccessException(
          CatalogAccessException.Code.UNAVAILABLE,
          "AWS STS credential refresh is temporarily unavailable",
          failure);
    }
    return new CatalogAccessException(
        CatalogAccessException.Code.CREDENTIAL_UNAVAILABLE,
        "AWS credentials for the Catalog Integration could not be refreshed",
        failure);
  }

  private static AwsCredentialValue toProviderCredentials(ResolvedAwsCredentials credentials) {
    return new AwsCredentialValue(
        credentials.accessKeyId(),
        credentials.secretAccessKey(),
        credentials.sessionToken(),
        credentials.expiresAt());
  }

  /**
   * Where an operator may have spelled the region. Mirrors {@code
   * SourceCatalogCredentialVendor.REGION_ALIAS_KEYS}, which is what reads them on the vend path;
   * kept here rather than shared because that class is in another package, and duplicated
   * deliberately rather than approximated -- checking fewer spellings here is precisely the defect
   * this list exists to prevent.
   */
  private static final List<String> REGION_ALIAS_KEYS =
      List.of("s3.region", "region", "client.region", "aws.region");

  /**
   * Protocols whose provider probes storage itself and so needs a resolved {@code s3.region}.
   *
   * <p>Both run the same Delta log probe, which falls back to {@code us-east-1} when the region is
   * absent. A share whose table lives elsewhere then has its validation read answered with
   * PermanentRedirect while the read path, which consults the endpoint, succeeds -- an Integration
   * permanently reporting a storage-access failure that does not exist in practice.
   */
  private static final java.util.Set<CatalogProtocol> RESOLVES_STORAGE_REGION =
      java.util.EnumSet.of(CatalogProtocol.UNITY_CATALOG, CatalogProtocol.DELTA_SHARING);

  /**
   * The integration's properties with {@code s3.region} resolved.
   *
   * <p>Only for {@link #RESOLVES_STORAGE_REGION}. The Iceberg REST provider reads the same map and
   * turns {@code s3.region} into {@code client.region}, so defaulting it there would pin a region
   * on an integration that deliberately set none and was relying on the AWS SDK's own resolution
   * chain -- replacing a provider-managed default with this deployment's. The Delta log probe those
   * two protocols share has no such chain: without a region it assumed {@code us-east-1} and
   * disagreed with the read path.
   *
   * <p>Resolved across every spelling, not just {@code s3.region}. The provider reads only that key
   * and the validation probe builds its S3 client from it, so a region written another way has to
   * be carried across -- and the deployment default is only correct when the operator stated none
   * at all.
   *
   * <p>Testing {@code s3.region} alone was worse than doing nothing. What is injected here does not
   * stay in the validator: the provider copies {@code s3.region} into its routing, the vend merges
   * that routing into the credential properties, and {@code
   * SourceCatalogCredentialVendor.routingProperties} reads the vended map before the connector's
   * aliases. So an operator who wrote {@code aws.region}, {@code region} or {@code client.region}
   * had the deployment default silently substituted for it -- their bucket in one region read
   * against another and answered PermanentRedirect -- which is the exact outcome that alias list
   * exists to prevent.
   */
  private Map<String, String> withDefaultRegion(
      CatalogIntegration integration, CatalogProtocol protocol) {
    Map<String, String> properties = integration.getPropertiesMap();
    if (!RESOLVES_STORAGE_REGION.contains(protocol)) {
      return properties;
    }
    String stated = null;
    for (String key : REGION_ALIAS_KEYS) {
      String value = properties.get(key);
      if (value != null && !value.isBlank()) {
        stated = value.trim();
        break;
      }
    }
    String region = stated != null ? stated : (defaultRegion == null ? null : defaultRegion.trim());
    if (region == null || region.isBlank() || region.equals(properties.get("s3.region"))) {
      return properties;
    }
    LinkedHashMap<String, String> defaulted = new LinkedHashMap<>(properties);
    defaulted.put("s3.region", region);
    return Map.copyOf(defaulted);
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

  record ResolvedAccess(
      CatalogConnectionConfig config,
      ResolvedCatalogCredentials credentials,
      AwsCredentialRegistration registration) {}

  static final class AwsCredentialRegistration implements AutoCloseable {
    private final Map<String, String> properties;
    private final AutoCloseable delegate;
    private boolean claimed;
    private boolean closed;

    AwsCredentialRegistration(Map<String, String> properties, AutoCloseable delegate) {
      this.properties = Map.copyOf(properties);
      this.delegate = delegate;
    }

    Map<String, String> properties() {
      return properties;
    }

    synchronized void claim() {
      if (closed || claimed) {
        throw new IllegalStateException("AWS credential registration is unavailable");
      }
      claimed = true;
    }

    synchronized boolean unclaimed() {
      return !claimed && !closed;
    }

    @Override
    public synchronized void close() {
      if (closed) return;
      closed = true;
      try {
        delegate.close();
      } catch (RuntimeException failure) {
        throw failure;
      } catch (Exception failure) {
        throw new IllegalStateException("Failed closing AWS credential registration", failure);
      }
    }
  }

  private static final class RegisteredCatalogClient implements CatalogClient {
    private final CatalogClient delegate;
    private final AwsCredentialRegistration registration;

    private RegisteredCatalogClient(
        CatalogClient delegate, AwsCredentialRegistration registration) {
      this.delegate = delegate;
      this.registration = registration;
      registration.claim();
    }

    @Override
    public CatalogCapabilities capabilities() {
      return callWithAwsTranslation(delegate::capabilities);
    }

    @Override
    public void validate() {
      runWithAwsTranslation(delegate::validate);
    }

    @Override
    public List<NamespacePath> listNamespaces(NamespacePath parent) {
      return callWithAwsTranslation(() -> delegate.listNamespaces(parent));
    }

    @Override
    public List<CatalogObjectName> listTables(NamespacePath namespace) {
      return callWithAwsTranslation(() -> delegate.listTables(namespace));
    }

    @Override
    public CatalogTable loadTable(CatalogObjectName table) {
      return callWithAwsTranslation(() -> delegate.loadTable(table));
    }

    @Override
    public List<CatalogObjectName> listViews(NamespacePath namespace) {
      return callWithAwsTranslation(() -> delegate.listViews(namespace));
    }

    @Override
    public CatalogView loadView(CatalogObjectName view) {
      return callWithAwsTranslation(() -> delegate.loadView(view));
    }

    @Override
    public Optional<VendedStorageCredentials> vendStorageCredentials(CatalogObjectName table) {
      return callWithAwsTranslation(() -> delegate.vendStorageCredentials(table));
    }

    @Override
    public void validateStorageAccess(
        CatalogObjectName table, VendedStorageCredentials vendedStorageCredentials) {
      runWithAwsTranslation(() -> delegate.validateStorageAccess(table, vendedStorageCredentials));
    }

    @Override
    public void close() {
      try {
        delegate.close();
      } finally {
        registration.close();
      }
    }

    private static <T> T callWithAwsTranslation(Supplier<T> action) {
      try {
        return action.get();
      } catch (RuntimeException failure) {
        throw translateAwsFailure(failure);
      }
    }

    private static void runWithAwsTranslation(Runnable action) {
      callWithAwsTranslation(
          () -> {
            action.run();
            return null;
          });
    }
  }
}
