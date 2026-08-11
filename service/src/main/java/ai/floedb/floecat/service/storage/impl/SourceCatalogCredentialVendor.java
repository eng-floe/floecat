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

package ai.floedb.floecat.service.storage.impl;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.connector.common.auth.CredentialResolverSupport;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorConfigMapper;
import ai.floedb.floecat.connector.spi.ConnectorFactory;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.connector.spi.IcebergAccessDelegation;
import ai.floedb.floecat.service.credentials.AuthResolutionContexts;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.VendedStorageCredential;
import com.google.protobuf.util.Timestamps;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Asks a table's own source catalog to vend storage credentials.
 *
 * <p>An Iceberg REST catalog with access delegation issues its own short-lived, table-scoped
 * credentials, which makes a separately configured storage authority unnecessary. Both paths that
 * need storage credentials for a table share this: the reconcile vend RPC ({@link
 * StorageAuthorityServiceImpl}) and the query scan path ({@link
 * ServerSideFileIoPropertiesResolver}). Keeping it in one place is what stops capture and query
 * disagreeing about whether a table is readable -- a table captured through delegation has no
 * authority to fall back to, so a query path that only knew about authorities could never read back
 * what capture had just written.
 *
 * <p>Callers own admission: this class assumes the caller has already authorized access to the
 * table and decided that no storage authority covers the location. It returns {@code null} for
 * every "this catalog cannot or will not vend" condition so the caller can fall back, and throws
 * only when the catalog actively refused or answered unusably.
 */
@ApplicationScoped
public class SourceCatalogCredentialVendor {
  private static final org.jboss.logging.Logger LOG =
      org.jboss.logging.Logger.getLogger(SourceCatalogCredentialVendor.class);

  /**
   * Non-secret S3 routing keys a vended credential carries, safe to expose in client_safe_config.
   */
  private static final List<String> VENDED_ROUTING_KEYS =
      List.of("s3.region", "s3.endpoint", "s3.path-style-access");

  /** Region aliases, matching what {@code StorageAuthorityResolver.putRegionConfig} writes. */
  private static final List<String> REGION_KEYS = List.of("s3.region", "region", "client.region");

  @Inject ConnectorRepository connectorRepo;
  @Inject CredentialResolver credentialResolver;

  @ConfigProperty(name = "floecat.storage.aws.region", defaultValue = "us-east-1")
  String defaultRegion;

  /**
   * Vends credentials for {@code table} from the catalog it was captured from, or returns {@code
   * null} when that catalog does not delegate and the caller should fall back.
   *
   * @param table a persisted table, carrying the upstream connector reference
   * @param responseLocationPrefix prefix to stamp on the returned credential
   */
  ResolveStorageAuthorityResponse vendForTable(Table table, String responseLocationPrefix) {
    if (table == null) {
      return null;
    }
    UpstreamRef upstream = table.getUpstream();
    String tableId = table.getResourceId().getId();
    if (!upstream.hasConnectorId() || upstream.getTableDisplayName().isBlank()) {
      LOG.infof(
          "source-catalog vending skipped: table %s has no upstream connector reference", tableId);
      return null;
    }
    Connector connector = connectorRepo.getById(upstream.getConnectorId()).orElse(null);
    if (connector == null) {
      LOG.infof(
          "source-catalog vending skipped: upstream connector %s of table %s not found",
          upstream.getConnectorId().getId(), tableId);
      return null;
    }

    // Only a connector that actually asked the catalog to vend can produce vended credentials.
    // Presence of s3.* on a table's FileIO is not a reliable signal: a Glue or S3 Tables connector
    // authenticated with static aws credentials writes s3.access-key-id into the same property map
    // for its own storage access, and those catalogs do not delegate at all. Gate on the declared
    // delegation intent so their own credentials are never mistaken for vended ones -- they keep
    // falling back to a storage authority. This also skips a needless connector build and OAuth
    // exchange for the non-delegating case.
    if (!connectorDeclaresVendedDelegation(connector)) {
      LOG.infof(
          "source-catalog vending skipped: connector %s did not declare vended-credentials"
              + " delegation",
          connector.getResourceId().getId());
      return null;
    }

    ConnectorConfig resolvedConfig = resolveConnectorCredentials(connector);
    String configuredAccessKey = resolvedConfig.options().get("s3.access-key-id");

    String namespaceFq = String.join(".", upstream.getNamespacePathList());
    Optional<FloecatConnector.VendedStorageCredentials> vended;
    try (FloecatConnector source = ConnectorFactory.create(resolvedConfig)) {
      vended = source.vendStorageCredentials(namespaceFq, upstream.getTableDisplayName());
    } catch (StatusRuntimeException e) {
      throw e;
    } catch (RuntimeException e) {
      // A catalog that refuses us is a permanent condition: bad credentials, a revoked grant, a
      // principal without TABLE_READ_DATA. Letting it escape as INTERNAL makes the reconciler treat
      // it as transient and retry the job forever, so classify it terminally. Anything that is not
      // recognisably an authorization refusal stays retryable.
      throw catalogFailureStatus(e, connector, namespaceFq, upstream.getTableDisplayName());
    }
    if (vended.isEmpty() || vended.get().isEmpty()) {
      LOG.infof(
          "source-catalog vending skipped: connector %s returned no credentials for %s.%s"
              + " (catalog does not delegate)",
          connector.getResourceId().getId(), namespaceFq, upstream.getTableDisplayName());
      return null;
    }

    // Delegation was declared, but a catalog that silently ignored the header leaves only the
    // connector's own configured credentials on the FileIO. If what came back is exactly what we
    // configured the connector with, nothing was vended -- fall back rather than pass the
    // connector's static credentials down the vend path as if they were catalog-scoped.
    String vendedAccessKey = vended.get().properties().get("s3.access-key-id");
    if (configuredAccessKey != null && configuredAccessKey.equals(vendedAccessKey)) {
      LOG.infof(
          "source-catalog vending skipped: connector %s returned its own configured credentials"
              + " for %s.%s (catalog did not delegate)",
          connector.getResourceId().getId(), namespaceFq, upstream.getTableDisplayName());
      return null;
    }

    requireRefreshableCredentials(vended.get(), namespaceFq, upstream.getTableDisplayName());

    // A delegating catalog vends credentials, not routing. Polaris returns the session triple and
    // no region at all, which left every consumer to supply its own: the reconcile worker has a
    // default and survived, the query scan engine has none and fails the whole scan with "region
    // is missing" after planning has already succeeded. Region is resolved here so both paths see
    // the same answer, and written under every alias the authority path emits -- consumers read
    // different ones, and an authority-backed response has always carried all three.
    Map<String, String> routing =
        routingProperties(vended.get().properties(), resolvedConfig.options());

    LinkedHashMap<String, String> storageConfig = new LinkedHashMap<>();
    storageConfig.put("type", "s3");
    storageConfig.putAll(vended.get().properties());
    storageConfig.putAll(routing);
    VendedStorageCredential.Builder credential =
        VendedStorageCredential.newBuilder()
            .setPrefix(responseLocationPrefix == null ? "" : responseLocationPrefix)
            .putAllConfig(Map.copyOf(storageConfig));
    Instant expiresAt = vended.get().expiresAt();
    if (expiresAt != null) {
      credential.setExpiresAt(Timestamps.fromMillis(expiresAt.toEpochMilli()));
    }
    LOG.infof(
        "vended storage credentials from source catalog connector=%s table=%s.%s expiresAt=%s",
        connector.getResourceId().getId(), namespaceFq, upstream.getTableDisplayName(), expiresAt);
    // Non-secret routing properties must also travel in client_safe_config. The reconcile worker's
    // execution-bound (refreshable) merge path applies client_safe_config plus the refreshed
    // credential triple and never reads storage_credentials[0].config, so region and endpoint
    // placed only there are silently dropped on the path that matters -- leaving the worker to
    // guess the endpoint for any non-default region or custom-endpoint bucket. Authority-backed
    // responses carry routing in client-safe config for exactly this reason.
    return ResolveStorageAuthorityResponse.newBuilder()
        .putAllClientSafeConfig(routing)
        .addStorageCredentials(credential)
        .build();
  }

  static Map<String, String> clientSafeRoutingProperties(Map<String, String> props) {
    LinkedHashMap<String, String> routing = new LinkedHashMap<>();
    for (String key : VENDED_ROUTING_KEYS) {
      String value = props.get(key);
      if (value != null && !value.isBlank()) {
        routing.put(key, value);
      }
    }
    return routing;
  }

  /**
   * Non-secret routing to advertise alongside vended credentials.
   *
   * <p>Endpoint and path-style come only from what the catalog vended or how the connector was
   * configured. They are deliberately <em>not</em> defaulted from floecat's own storage settings:
   * that endpoint points at floecat's blob store -- LocalStack in dev -- and injecting it would
   * redirect reads of a real S3 warehouse to the wrong service. A missing endpoint correctly means
   * "standard AWS S3".
   *
   * <p>Region is different: it has no safe absent value, and the catalog does not supply one. It
   * falls back to the connector's own configuration and then to the deployment's configured region,
   * mirroring {@code resolveSnapshotCompatStorageSettings}, which synthesizes exactly these
   * settings when no authority exists. A wrong region announces itself immediately as an S3
   * redirect; an absent one fails mid-scan with nothing pointing at the cause.
   */
  Map<String, String> routingProperties(
      Map<String, String> vendedProps, Map<String, String> connectorOptions) {
    LinkedHashMap<String, String> routing =
        new LinkedHashMap<>(clientSafeRoutingProperties(vendedProps));
    String region =
        firstNonBlank(
            firstNonBlank(REGION_KEYS.stream().map(vendedProps::get).toArray(String[]::new)),
            firstNonBlank(REGION_KEYS.stream().map(connectorOptions::get).toArray(String[]::new)),
            defaultRegion);
    if (region != null) {
      // Same three keys putRegionConfig writes for an authority-backed response.
      REGION_KEYS.forEach(key -> routing.put(key, region));
    }
    return Map.copyOf(routing);
  }

  private static String firstNonBlank(String... values) {
    if (values == null) {
      return null;
    }
    for (String value : values) {
      if (value != null && !value.isBlank()) {
        return value.trim();
      }
    }
    return null;
  }

  /**
   * Whether the connector asked its catalog to vend credentials.
   *
   * <p>Delegates to the shared parser so this gate and the reconciler's -- which decides whether to
   * absorb the resulting missing-authority error -- cannot drift apart.
   */
  static boolean connectorDeclaresVendedDelegation(Connector connector) {
    return IcebergAccessDelegation.declaresVendedCredentials(
        ConnectorConfigMapper.fromProto(connector));
  }

  /**
   * Refuses vended credentials that carry no usable expiry.
   *
   * <p>The reconcile worker registers a refresh provider only when it can see one -- its {@code
   * is_refreshable()} is exactly {@code expires_at.is_some()} -- and without it embeds the
   * credentials statically and never re-vends, so they expire mid-read with no recovery. Client
   * usage carries the same invariant. Failing at vend time makes that visible here instead of as an
   * opaque 403 partway through a file group.
   */
  static void requireRefreshableCredentials(
      FloecatConnector.VendedStorageCredentials vended, String namespaceFq, String tableName) {
    Map<String, String> props = vended.properties();
    List<String> missing = new java.util.ArrayList<>();
    for (String key : List.of("s3.access-key-id", "s3.secret-access-key", "s3.session-token")) {
      String value = props.get(key);
      if (value == null || value.isBlank()) {
        missing.add(key);
      }
    }
    if (vended.expiresAt() == null) {
      missing.add("s3.session-token-expires-at-ms");
    }
    if (missing.isEmpty()) {
      return;
    }
    // The whole tuple, not just the expiry. An access key and secret with an expiry but no session
    // token satisfies isExecutionBoundStorageCredential yet fails isRefreshableExecutionCredential,
    // so the reconciler embeds it statically and never renews -- recreating exactly the defect the
    // expiry check was added to close.
    //
    // Structured and terminal: a catalog that omits a field will keep omitting it, and a bare
    // FAILED_PRECONDITION is classified retryable, so the job would loop forever rather than fail
    // mid-read.
    throw ai.floedb.floecat.storage.errors.SourceCatalogVendingGrpcStatus
        .vendedCredentialsNotRefreshable(
            "source catalog vended unusable storage credentials for "
                + namespaceFq
                + "."
                + tableName
                + "; missing "
                + String.join(", ", missing));
  }

  /**
   * Classifies a source-catalog failure so the reconciler can stop retrying what will never
   * succeed.
   *
   * <p>Only recognisable authentication and authorization refusals become terminal. Anything else
   * -- a connection reset, a 5xx, a timeout -- is genuinely transient and keeps INTERNAL so the
   * existing retry behaviour still applies.
   */
  private static StatusRuntimeException catalogFailureStatus(
      RuntimeException cause, Connector connector, String namespaceFq, String tableName) {
    String detail =
        String.format(
            "source catalog %s refused credentials for %s.%s: %s",
            connector.getResourceId().getId(), namespaceFq, tableName, cause);

    // Typed exceptions only. Substring-matching the cause chain for 401/403/"access denied" gets
    // the risk backwards: a transient failure whose text merely contains one of those tokens -- a
    // gateway page echoing "Access Denied", an S3 denial during IAM propagation lag, a URL with 403
    // in it -- would be classified terminal and stop the reconciler retrying a job that would have
    // recovered. Iceberg's REST client raises NotAuthorizedException for 401 and ForbiddenException
    // for 403, so classification uses those and nothing else.
    for (Throwable c = cause; c != null; c = c.getCause()) {
      if (c instanceof org.apache.iceberg.exceptions.NotAuthorizedException) {
        return io.grpc.Status.UNAUTHENTICATED
            .withDescription(detail)
            .withCause(cause)
            .asRuntimeException();
      }
      if (c instanceof org.apache.iceberg.exceptions.ForbiddenException) {
        return io.grpc.Status.PERMISSION_DENIED
            .withDescription(detail)
            .withCause(cause)
            .asRuntimeException();
      }
      if (c.getCause() == c) {
        break;
      }
    }
    // Unrecognised stays retryable: an over-eager terminal permanently fails a job, an over-eager
    // retry only costs time.
    return io.grpc.Status.INTERNAL.withDescription(detail).withCause(cause).asRuntimeException();
  }

  /** Mirrors the resolution the reconciler uses so a connector authenticates identically here. */
  private ConnectorConfig resolveConnectorCredentials(Connector connector) {
    ConnectorConfig base = ConnectorConfigMapper.fromProto(connector);
    AuthConfig auth = connector.getAuth();
    if (auth.hasCredentials()
        && auth.getCredentials().getCredentialCase()
            != AuthCredentials.CredentialCase.CREDENTIAL_NOT_SET) {
      return CredentialResolverSupport.apply(base, auth.getCredentials());
    }
    if (!connector.hasResourceId()
        || auth.getScheme().isBlank()
        || "none".equalsIgnoreCase(auth.getScheme())) {
      return base;
    }
    // Carry the inbound request context, not an empty one. Token-exchange schemes
    // (RFC8693/AZURE/GCP) need the caller's subject token to mint connector credentials; with an
    // empty context CredentialResolverSupport cannot resolve them, so a delegating catalog on any
    // of those auth modes would fail to build before it could vend. The storage RPC has the
    // inbound authorization/session headers available here, same as the normal connector path.
    return credentialResolver
        .resolve(connector.getResourceId().getAccountId(), connector.getResourceId().getId())
        .map(
            c ->
                CredentialResolverSupport.apply(
                    base, c, AuthResolutionContexts.fromInboundContext()))
        .orElse(base);
  }
}
