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

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.catalog.access.CatalogCapability;
import ai.floedb.floecat.catalog.access.CatalogClient;
import ai.floedb.floecat.catalog.access.CatalogObjectName;
import ai.floedb.floecat.catalog.access.NamespacePath;
import ai.floedb.floecat.catalog.access.StorageLocations;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.common.auth.CredentialResolverSupport;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorConfigMapper;
import ai.floedb.floecat.connector.spi.ConnectorFactory;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.connector.spi.LogSafeText;
import ai.floedb.floecat.connector.spi.SourceCatalogAccessException;
import ai.floedb.floecat.connector.spi.SourceCatalogVending;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationType;
import ai.floedb.floecat.service.credentials.AuthResolutionContexts;
import ai.floedb.floecat.service.integration.CatalogIntegrationAccess;
import ai.floedb.floecat.service.integration.CatalogUpstreamBudget;
import ai.floedb.floecat.service.repo.impl.CatalogIntegrationRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.security.RolePermissions;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.storage.errors.SourceCatalogVendingGrpcStatus;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.VendedStorageCredential;
import com.google.protobuf.util.Timestamps;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Stream;
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
 * table and decided that no storage authority covers the location.
 *
 * <p>The two upstream kinds answer differently, and deliberately. A legacy Connector opts in to
 * vending -- {@code IcebergAccessDelegation.declaresVendedCredentials} is the switch -- so a
 * connector that does not, or cannot, is answered with {@code null} and the caller falls back to
 * the storage authority the operator configured instead. A Catalog Integration has no such switch
 * and no such alternative: nothing on the record names an authority, and {@code
 * ValidateCatalogIntegration} reports an Integration whose provider cannot vend as invalid rather
 * than as configured some other way. Reaching for an authority beside an Integration is the
 * split-brain this feature exists to remove, so every "cannot vend" condition on that path is a
 * refusal naming the cause, not a fall-back.
 *
 * <p>Nothing was lost by that. This vend is reached only once no authority covers the location, so
 * a {@code null} from the Integration path landed on {@code
 * StorageAuthorityResolver.buildResponse(null, ...)}, which raises no-matching-authority -- the one
 * consumer that absorbs that error gates on a {@code ConnectorConfig}. The fall-back could only
 * ever relabel a specific cause as "you configured no storage authority".
 *
 * <p>What it throws is classified, because the reconcile path acts on it: only a condition a retry
 * cannot change -- an authorization refusal, a vanished upstream table, an incomplete credential
 * tuple -- travels as a terminal refusal reason. Anything a later attempt could clear stays
 * retryable, since a wrongly terminal answer permanently fails a capture job while a wrongly
 * retried one costs time.
 */
@ApplicationScoped
public class SourceCatalogCredentialVendor {
  private static final org.jboss.logging.Logger LOG =
      org.jboss.logging.Logger.getLogger(SourceCatalogCredentialVendor.class);

  /** Cap on the gRPC status description built from a catalog failure. See catalogFailureStatus. */
  private static final int MAX_STATUS_DESCRIPTION = 512;

  /**
   * A catalog-supplied table identifier, flattened and bounded for a status or a log line.
   *
   * <p>Both halves come off the persisted {@code UpstreamRef}, so neither has a length or character
   * bound, and a refusal carries this text off the node twice: once in the {@code Error} detail's
   * {@code detail} parameter, and again as the grpc-message {@code LocalizeErrorsInterceptor}
   * renders from it -- percent-encoded trailer metadata competing with HTTP/2 header limits.
   * Flattening matters for the log line too: a name containing a newline would forge one.
   */
  private static String boundedTable(String namespaceFq, String tableName) {
    return LogSafeText.bounded(namespaceFq + "." + tableName, MAX_LOGGED_PREFIX_CHARS);
  }

  /**
   * Bound on the refusal detail written to the server log.
   *
   * <p>Far above {@link #MAX_STATUS_DESCRIPTION} on purpose: the status carries a summary and the
   * log carries the diagnostic, so this exists to cap a flood rather than to summarize.
   */
  private static final int MAX_LOGGED_DETAIL_CHARS = 8_192;

  /** Bound on a catalog-supplied prefix in a log line; long enough to be recognizable. */
  private static final int MAX_LOGGED_PREFIX_CHARS = 256;

  /**
   * Non-secret S3 routing keys a vended credential carries, safe to expose in client_safe_config.
   */
  private static final List<String> VENDED_ROUTING_KEYS =
      List.of("s3.region", "s3.endpoint", "s3.path-style-access");

  /** Region aliases, matching what {@code StorageAuthorityResolver.putRegionConfig} writes. */
  private static final List<String> REGION_KEYS = List.of("s3.region", "region", "client.region");

  /**
   * Where a connector's own region may be spelled. A superset of {@link #REGION_KEYS}: {@code
   * aws.region} is documented for Delta and Iceberg connector options ({@code
   * DeltaConnectorFactory} reads it) but is not one of the keys written back, so it is read-only
   * here.
   */
  private static final List<String> REGION_ALIAS_KEYS =
      Stream.concat(REGION_KEYS.stream(), Stream.of("aws.region")).toList();

  /**
   * How far a vended expiry may sit in the past before it stops looking like a race.
   *
   * <p>Wide enough to absorb the two things that legitimately produce a just-expired credential --
   * transit time between the catalog issuing it and us reading it, and ordinary NTP-scale
   * disagreement between floecat's clock and the catalog's -- and far narrower than any credential
   * lifetime worth vending, so a timestamp that is genuinely stale still reads as stale. See {@link
   * #requireLiveExpiry}, which is the only reader.
   */
  private static final Duration EXPIRY_SKEW_TOLERANCE = Duration.ofSeconds(60);

  /**
   * What the caller will do with the credentials. Selects how strictly they are validated: only the
   * reconcile path renews them, so only it requires a renewable session tuple.
   */
  enum CredentialUse {
    /** Execution-bound capture. A refresh provider is registered, so renewal must be possible. */
    RECONCILE,
    /** Query read-back. Handed to the scan engine's FileIO for immediate reads; never renewed. */
    QUERY
  }

  /**
   * Which vend path produced a credential, where the two differ in what shapes are legitimate.
   *
   * <p>Separate from {@link CredentialUse}, which says what the caller will do with the credential.
   * This says where it came from, and only one rule turns on it today: whether a key pair with no
   * session token and no expiry is acceptable for capture. It retires with the connector path.
   */
  enum VendSource {
    /** A connector's own catalog. Unity vends a long-lived key pair for some external locations. */
    CONNECTOR,
    /** A catalog integration, whose contract is delegated temporary credentials. */
    CATALOG_INTEGRATION
  }

  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject ConnectorRepository connectorRepo;
  @Inject CredentialResolver credentialResolver;
  Function<ConnectorConfig, FloecatConnector> connectorFactory = ConnectorFactory::create;
  @Inject CatalogIntegrationRepository catalogIntegrationRepo;
  @Inject CatalogIntegrationAccess catalogIntegrationAccess;

  @ConfigProperty(name = "floecat.storage.aws.region", defaultValue = "us-east-1")
  String defaultRegion;

  /**
   * Wall-clock limit on everything one vend asks of an upstream catalog. Matches the default the
   * validation and discovery paths use for the same calls.
   *
   * <p>One value for both callers, which is a compromise rather than a preference: the reconcile
   * vend RPC can afford to wait on a slow catalog, while the query path holds scan planning and
   * would rather give up and fall back. Nothing shortens it per caller -- {@code
   * CatalogUpstreamBudget} deliberately does not read the caller's gRPC deadline, for the reason
   * documented there -- so this default is what a stalled upstream costs a scan.
   */
  @ConfigProperty(name = "floecat.storage.source-catalog.upstream-timeout", defaultValue = "PT30S")
  Duration upstreamTimeout;

  /**
   * How long a vend waits for its own client to close. Short because teardown is not the answer the
   * caller is waiting for, and unbounded is what this replaces.
   */
  private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5);

  Clock clock = Clock.systemUTC();
  LongSupplier nanoTime = System::nanoTime;

  /**
   * Vends credentials for {@code table} from the catalog it was captured from.
   *
   * <p>Returns {@code null} only where a fall-back is meaningful: a table with no upstream
   * reference, or a Connector that did not opt in to vending. An Integration-backed table is
   * answered with credentials or with a refusal naming why not.
   *
   * @param table a persisted table, carrying its upstream catalog reference
   * @param responseLocationPrefix prefix to stamp on the returned credential
   * @param use how the caller will use the credentials, which sets how strictly they are validated
   */
  ResolveStorageAuthorityResponse vendForTable(
      Table table, String responseLocationPrefix, CredentialUse use) {
    if (table == null) {
      return null;
    }
    UpstreamRef upstream = table.getUpstream();
    ResourceId tableId = table.getResourceId();
    // The Integration branch first, so its invariant holds for this condition too: every "cannot
    // vend" on that path names the cause rather than falling through to a missing authority the
    // operator was never asked to configure. Reachable outside the overlay reconciler --
    // TableServiceImpl.validateUpstreamRef checks the integration id's resource kind and the
    // namespace segments but never requires a display name -- and by any record written before the
    // field was populated.
    if (upstream.hasCatalogIntegrationId()) {
      if (upstream.getTableDisplayName().isBlank()) {
        throw integrationCannotVend(
            "table " + tableId.getId() + " has no upstream table reference");
      }
      return vendFromCatalogIntegration(tableId, upstream, responseLocationPrefix, use);
    }
    if (upstream.getTableDisplayName().isBlank()) {
      LOG.infof(
          "source-catalog vending skipped: table %s has no upstream table reference",
          tableId.getId());
      return null;
    }
    if (!upstream.hasConnectorId()) {
      LOG.infof(
          "source-catalog vending skipped: table %s has no upstream catalog reference",
          tableId.getId());
      return null;
    }
    // Scoped to the table's own account rather than trusting the reference. The upstream ref is
    // stored verbatim from spec.upstream on table create -- validateUpstreamRef checks the resource
    // kind, not the account -- and ConnectorRepository.getById keys on the account inside the id it
    // is handed. A ref naming another tenant would otherwise resolve that tenant's connector, its
    // secret, and vend through it for a caller authorized only here. The catalog-integration path
    // rebuilds its id the same way.
    ResourceId connectorId =
        upstream.getConnectorId().toBuilder()
            .setAccountId(table.getResourceId().getAccountId())
            .build();
    Connector connector = connectorRepo.getById(connectorId).orElse(null);
    if (connector == null) {
      LOG.infof(
          "source-catalog vending skipped: upstream connector %s of table %s not found",
          LogSafeText.bounded(connectorId.getId(), MAX_LOGGED_PREFIX_CHARS), tableId.getId());
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
    try (FloecatConnector source = connectorFactory.apply(resolvedConfig)) {
      vended = source.vendStorageCredentials(namespaceFq, upstream.getTableDisplayName());
    } catch (StatusRuntimeException e) {
      throw e;
    } catch (RuntimeException e) {
      // A catalog that refuses us is a permanent condition: bad credentials, a revoked grant, a
      // principal without TABLE_READ_DATA. Letting it escape as INTERNAL makes the reconciler treat
      // it as transient and retry the job forever, so classify it terminally. Anything that is not
      // recognisably an authorization refusal stays retryable.
      throw catalogFailureStatus(e, connector, namespaceFq, upstream.getTableDisplayName(), use);
    }
    // Empty and absent are the same answer. A connector that hands back a credential object with
    // no properties has vended nothing, and falling through would reach requireUsableCredentials
    // and fail the job terminally on a condition the caller can recover from by using a storage
    // authority. Partial credentials are deliberately *not* screened here -- those do reach the
    // usability check, which is where "the catalog vended something unusable" belongs.
    if (vended.isEmpty() || vended.get().isEmpty()) {
      LOG.infof(
          "source-catalog vending skipped: connector %s returned no credentials for %s"
              + " (catalog does not delegate)",
          connector.getResourceId().getId(),
          boundedTable(namespaceFq, upstream.getTableDisplayName()));
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
              + " for %s (catalog did not delegate)",
          connector.getResourceId().getId(),
          boundedTable(namespaceFq, upstream.getTableDisplayName()));
      return null;
    }

    return credentialResponse(
        vended.get().properties(),
        vended.get().expiresAt(),
        resolvedConfig.options(),
        responsePrefix(
            vended.get(), responseLocationPrefix, namespaceFq, upstream.getTableDisplayName()),
        "connector=" + connector.getResourceId().getId(),
        namespaceFq,
        upstream.getTableDisplayName(),
        use,
        VendSource.CONNECTOR);
  }

  private ResolveStorageAuthorityResponse vendFromCatalogIntegration(
      ResourceId tableId, UpstreamRef upstream, String responseLocationPrefix, CredentialUse use) {
    ResourceId integrationId =
        upstream.getCatalogIntegrationId().toBuilder().setAccountId(tableId.getAccountId()).build();
    CatalogIntegration integration = catalogIntegrationRepo.getById(integrationId).orElse(null);
    if (integration == null) {
      throw integrationCannotVend(
          "upstream Catalog Integration "
              + LogSafeText.bounded(integrationId.getId(), MAX_LOGGED_PREFIX_CHARS)
              + " of table "
              + tableId.getId()
              + " not found");
    }
    if (integration.getType() != CatalogIntegrationType.CIT_ICEBERG_REST) {
      throw integrationCannotVend(
          "Catalog Integration "
              + integration.getResourceId().getId()
              + " is of a type that does not vend storage credentials");
    }

    // After the two checks above, not before. Opening an integration resolves its stored secret and
    // spends an OAuth exchange against the upstream on the caller's behalf, so it takes the same
    // permission every other site that opens one requires -- CatalogIntegrationsImpl's validate and
    // upstream listings, and the overlay reconcile. Table authorization admits the caller to this
    // vend; it does not admit them to the integration's own credential.
    //
    // Ordered last of the three because the other two describe the Integration rather than the
    // caller: a missing record or a type that cannot vend means this vend was never going to
    // happen, and answering PERMISSION_DENIED there would blame the caller for the configuration.
    authz.require(principal.get(), RolePermissions.CATALOG_INTEGRATION_USE);

    String namespaceFq = String.join(".", upstream.getNamespacePathList());
    CatalogObjectName tableName =
        new CatalogObjectName(
            new NamespacePath(upstream.getNamespacePathList()), upstream.getTableDisplayName());
    Optional<ai.floedb.floecat.catalog.access.VendedStorageCredentials> vended;
    // One deadline across the whole conversation with the upstream catalog: opening the client is
    // itself a config round trip and an OAuth exchange, and loadTable is a third. A catalog that
    // accepts the connection and then stalls would otherwise hold this thread -- a gRPC handler on
    // the vend path, scan planning on the query path -- for the sum of three socket timeouts.
    CatalogUpstreamBudget budget = CatalogUpstreamBudget.start(upstreamTimeout, nanoTime);
    // Not try-with-resources: the close has to run bounded and exception-swallowing. A close hook
    // throws would otherwise replace credentials already in hand with an INTERNAL failure, and the
    // suppression rules make that happen only on the success path, where it is least expected.
    CatalogClient source = null;
    try {
      source =
          budget.call(
              () -> catalogIntegrationAccess.open(integration),
              SourceCatalogCredentialVendor::closeQuietly);
      // Budgeted like the calls either side of it. It is local for the Iceberg REST provider, but
      // nothing in the CatalogClient contract says a provider cannot ask the catalog what it
      // supports, and one that does would sit outside the deadline this method exists to impose.
      CatalogClient opened = source;
      if (!budget.call(opened::capabilities).supports(CatalogCapability.VEND_STORAGE_CREDENTIALS)) {
        throw integrationCannotVend(
            "Catalog Integration "
                + integration.getResourceId().getId()
                + " does not support storage credential vending");
      }
      vended = budget.call(() -> opened.vendStorageCredentials(tableName));
    } catch (StatusRuntimeException e) {
      throw e;
    } catch (java.util.concurrent.CancellationException e) {
      // The budget raises this when the wait is interrupted, and BaseServiceImpl.toStatus maps it
      // to CANCELLED. Folding it into the classification below would report a caller who went away
      // as a server fault, and put a cancelled reconcile attempt on the retry path.
      throw e;
    } catch (RuntimeException e) {
      throw catalogIntegrationFailureStatus(
          e, findCatalogAccessFailure(e), integration, namespaceFq, tableName.name(), use);
    } finally {
      closeWithinBudget(source);
    }
    if (vended.isEmpty()) {
      throw integrationCannotVend(
          "Catalog Integration "
              + integration.getResourceId().getId()
              + " vended no storage credentials for "
              + boundedTable(namespaceFq, tableName.name()));
    }

    var credentials = vended.get();
    // The same helper the connector path uses, rather than a second rule beside it. It resolves all
    // three relations between the vended scope and the location the caller was authorized for: a
    // broader scope narrows to the request, a narrower one is stamped as itself, and a disjoint one
    // returns the request with a warning. An earlier inline check here admitted only the first,
    // which turned a catalog that legitimately scoped to a subtree of the request -- vend
    // s3://b/db/tbl/data for request s3://b/db/tbl -- into a missing-authority error.
    //
    // Nothing is gained by refusing the disjoint case either: this path is reached only once no
    // storage authority matched, so falling back means failing, while stamping the request lets the
    // read attempt proceed and lets S3 -- which knows the real grant -- have the final say.
    String stampedPrefix =
        responsePrefix(
            credentials.scopePrefix(), responseLocationPrefix, namespaceFq, tableName.name());
    return credentialResponse(
        credentials.properties(),
        credentials.expiresAt().orElse(null),
        integration.getPropertiesMap(),
        stampedPrefix,
        "catalog-integration=" + integration.getResourceId().getId(),
        namespaceFq,
        tableName.name(),
        use,
        VendSource.CATALOG_INTEGRATION);
  }

  private ResolveStorageAuthorityResponse credentialResponse(
      Map<String, String> properties,
      Instant expiresAt,
      Map<String, String> sourceOptions,
      String responseLocationPrefix,
      String sourceDescription,
      String namespaceFq,
      String tableName,
      CredentialUse use,
      VendSource source) {
    noteIgnoredAccessPoint(properties, namespaceFq, tableName);
    requireUsableCredentials(
        properties, expiresAt, clock.instant(), namespaceFq, tableName, use, source);

    // A delegating catalog vends credentials, not necessarily routing. Polaris, for example,
    // returns the session triple and no region. Resolve non-secret routing once so reconcile and
    // query consumers receive the same usable answer.
    Map<String, String> routing = routingProperties(properties, sourceOptions);

    LinkedHashMap<String, String> storageConfig = new LinkedHashMap<>();
    storageConfig.put("type", "s3");
    storageConfig.putAll(properties);
    storageConfig.putAll(routing);
    // Dropped rather than forwarded: the ARN is not acted on anywhere here, and advertising a
    // routing key no consumer honours invites one to start honouring it inconsistently. Same
    // reason FILE_IO_PROPERTY_KEYS omits it. noteIgnoredAccessPoint has already logged it.
    storageConfig.remove("s3.access-point");
    VendedStorageCredential.Builder credential =
        VendedStorageCredential.newBuilder()
            .setPrefix(responseLocationPrefix == null ? "" : responseLocationPrefix)
            .putAllConfig(Map.copyOf(storageConfig));
    if (expiresAt != null) {
      credential.setExpiresAt(Timestamps.fromMillis(expiresAt.toEpochMilli()));
    }
    LOG.infof(
        "vended storage credentials from source catalog %s table=%s expiresAt=%s",
        sourceDescription, boundedTable(namespaceFq, tableName), expiresAt);
    // Routing must also travel in client_safe_config: the refreshable reconcile path consumes it
    // separately from the secret credential tuple.
    return ResolveStorageAuthorityResponse.newBuilder()
        .putAllClientSafeConfig(routing)
        .addStorageCredentials(credential)
        .build();
  }

  /**
   * The prefix to stamp on the vended credential: the vended scope when it narrows the request,
   * otherwise the location the caller was authorized for.
   *
   * <p>The vended scope is taken only downwards, and it is whatever the catalog named -- the
   * Iceberg connector passes the catalog's prefix through and leaves it null when the catalog named
   * nothing, deliberately refusing to substitute the table location because {@code write.data.path}
   * and {@code add_files} can put files outside it. So the scope is a bound to intersect with
   * rather than an assertion about what the credential covers, and a null one bounds nothing.
   * {@code requestedPrefix} is {@code credentialScope.location()}, the same value {@code
   * isWithinExecutionScope} and {@code resolvePlannerBootstrapLocation} use to refuse out-of-scope
   * requests, so letting a broader scope replace it would hand back a credential stamped for more
   * than the caller may read: a table at {@code s3://warehouse/tpch_10/customer} covered by a
   * credential scoped to {@code s3://warehouse/tpch} would come back claiming all of {@code
   * s3://warehouse/tpch*}, and the consumers that key their client cache on this prefix would apply
   * it to sibling prefixes. The credential itself may well be broader; what travels back is bounded
   * by the authorization.
   */
  static String responsePrefix(
      FloecatConnector.VendedStorageCredentials vended,
      String requestedPrefix,
      String namespaceFq,
      String tableName) {
    return responsePrefix(
        vended == null ? null : vended.scopePrefix(), requestedPrefix, namespaceFq, tableName);
  }

  static String responsePrefix(
      String vendedScope, String requestedPrefix, String namespaceFq, String tableName) {
    // Both sides normalized, not just the vended one: three of the four exits below return the
    // requested prefix, and that is the common path. The trailing-slash hazard described for the
    // scope applies identically -- credentialScope.location() resolves from table properties or a
    // client-supplied location on the REST request, both of which routinely carry one. The record's
    // "trimming a bound widens it" caution does not apply here: this is the authorization floecat
    // computed, not a bound the catalog handed us.
    String requested =
        requestedPrefix == null
            ? ""
            : StorageAuthorityResolver.stripTrailingSlash(requestedPrefix.trim());
    // Trimmed here rather than in the record. The record keeps a prefix as the catalog sent it,
    // because that value is a bound and trimming one widens it; this method produces the value that
    // travels back and that consumers key a client cache on, so " s3://x " and "s3://x" naming two
    // different scopes is this method's problem to prevent. Trimming cannot widen past the
    // authorization: what is returned still had to clear the containment check below.
    String raw = vendedScope;
    // Trailing slash normalized alongside the whitespace, for the same reason: this value travels
    // back as storage_credentials[].prefix, and TableLoadService keys a merged map on it, so
    // "s3://bucket/table/" and "s3://bucket/table" would become two entries and two client FileIO
    // instances for one scope. matchesLocationPrefix strips the slash from its prefix argument
    // only, so the slashed form clears containment and would otherwise be returned verbatim.
    // stripTrailingSlash is what every other location comparison in this package normalizes with.
    String scope = raw == null ? null : StorageAuthorityResolver.stripTrailingSlash(raw.trim());
    if (scope == null || scope.isEmpty()) {
      return requested;
    }
    // Blank request: nothing to bound against, so the vended scope is the only answer available
    // and is narrower than the unbounded alternative. Otherwise containment is decided by the same
    // path-boundary rule the authority resolver uses -- a plain startsWith would accept
    // "s3://warehouse/tpch_other" as inside "s3://warehouse/tpch".
    // Compared with the s3 scheme aliases folded. matchesLocationPrefix normalizes whitespace and
    // the trailing slash but not the scheme, so an ordinarily-configured catalog reporting
    // "s3a://warehouse/orders" against an "s3://warehouse/orders/..." request failed both
    // containment tests and landed in the disjoint branch below -- firing, once per file group, the
    // WARN that branch reserves for something actually being wrong.
    String scopeCompared = StorageLocations.normalizeScheme(scope);
    String requestedCompared = StorageLocations.normalizeScheme(requested);
    if (requested.isBlank()
        || StorageAuthorityResolver.matchesLocationPrefix(scopeCompared, requestedCompared)) {
      // Spelled the way the request spells it, not the way the catalog did. This value is what a
      // client vend and an Iceberg loadTable hand to the reader, and S3FileIO selects a credential
      // with a raw storagePath.startsWith(storagePrefix) -- so an "s3a://" prefix is invisible to
      // an "s3://" data path, which silently falls through to the root client and reads with
      // ambient credentials or none. Folding the aliases for the comparison without folding what is
      // returned is what made an unmatchable prefix reachable in the first place.
      return withSchemeOf(scope, requested);
    }
    // WARN here, unlike the access-point line below, and the difference is the point: a vended
    // access point is benign and common, while a disjoint scope means the credential travels
    // stamped for a location it does not cover and the read will fail. Both repeat per file group,
    // but only one of them is reporting that something is actually wrong.
    //
    // Not inside the request, which splits two ways. A scope that *contains* the request is the
    // ordinary case -- the catalog scoped broadly, and narrowing to what the caller asked for is
    // the whole point of this method. Disjoint is different: there is no intersection to return, so
    // the credential travels stamped for a location it does not cover and the worker finds out at
    // storage. Still returns the requested prefix, because it is the only bound the caller was
    // authorized for; the log is what turns an opaque 403 into something traceable to the catalog.
    if (!StorageAuthorityResolver.matchesLocationPrefix(requestedCompared, scopeCompared)) {
      LOG.warnf(
          "source catalog vended a scope disjoint from the requested location for %s:"
              + " vended=%s requested=%s; returning the requested prefix",
          boundedTable(namespaceFq, tableName),
          LogSafeText.location(scope, MAX_LOGGED_PREFIX_CHARS),
          LogSafeText.location(requested, MAX_LOGGED_PREFIX_CHARS));
    }
    return requested;
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
   * redirect; an absent one fails mid-scan with nothing pointing at the cause. The connector's
   * region is read under every documented alias, {@link #REGION_ALIAS_KEYS}, so a connector
   * configured with {@code aws.region} is not silently replaced by the deployment default.
   */
  Map<String, String> routingProperties(
      Map<String, String> vendedProps, Map<String, String> connectorOptions) {
    LinkedHashMap<String, String> routing =
        new LinkedHashMap<>(clientSafeRoutingProperties(vendedProps));
    // A Unity response carries only the credential tuple and an optional access point, so anything
    // else the reader needs to reach the bucket at all -- a MinIO/S3-compatible endpoint,
    // path-style
    // addressing -- can only come from the connector. Vended wins where both are present.
    clientSafeRoutingProperties(connectorOptions).forEach(routing::putIfAbsent);
    String region =
        firstNonBlank(
            firstNonBlank(REGION_KEYS.stream().map(vendedProps::get).toArray(String[]::new)),
            firstNonBlank(
                REGION_ALIAS_KEYS.stream().map(connectorOptions::get).toArray(String[]::new)),
            defaultRegion);
    if (region != null) {
      // Same three keys putRegionConfig writes for an authority-backed response.
      REGION_KEYS.forEach(key -> routing.put(key, region));
    }
    return Map.copyOf(routing);
  }

  /**
   * {@code value} rewritten to carry {@code template}'s scheme, when the two are s3 aliases.
   *
   * <p>Only the aliases: {@code s3}, {@code s3a} and {@code s3n} name one store, so choosing
   * between them is spelling. Anything else is a different store and is left alone -- rewriting
   * there would not be normalization, it would be pointing the caller somewhere else.
   */
  private static String withSchemeOf(String value, String template) {
    if (template == null || template.isBlank()) {
      return value;
    }
    int valueScheme = value.indexOf("://");
    int templateScheme = template.indexOf("://");
    if (valueScheme < 0 || templateScheme < 0) {
      return value;
    }
    String templatePrefix = template.substring(0, templateScheme);
    // Both sides have to fold to the same store before the spelling is interchangeable.
    if (!StorageLocations.normalizeScheme(value.substring(0, valueScheme) + "://")
        .equals(StorageLocations.normalizeScheme(templatePrefix + "://"))) {
      return value;
    }
    return templatePrefix + value.substring(valueScheme);
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
   * <p>The broad predicate: it decides whether a vend is attempted at all, and answers true for
   * Delta and Unity as well as Iceberg. The reconciler's absorb gate is deliberately narrower --
   * {@code clientAppliesVendedCredentials}, false for Delta and Unity and for a static-table
   * Iceberg source -- so the two do not answer alike and are not meant to. Both spellings live in
   * {@code SourceCatalogVending} so that the difference stays deliberate rather than emergent.
   */
  static boolean connectorDeclaresVendedDelegation(Connector connector) {
    return SourceCatalogVending.declaresVendedCredentials(
        ConnectorConfigMapper.fromProto(connector));
  }

  /**
   * Refuses vended credentials the caller cannot actually use.
   *
   * <p>Two requirements, and only one of them is universal. Any reader needs a complete key pair.
   * Beyond that, {@link CredentialUse#RECONCILE} additionally needs a session token and an expiry,
   * because the reconcile worker registers a refresh provider only when it can see one -- its
   * {@code is_refreshable()} is exactly {@code expires_at.is_some()} -- and without it embeds the
   * credentials statically and never re-vends, so they expire mid-read with no recovery. Failing at
   * vend time makes that visible here instead of as an opaque 403 partway through a file group.
   *
   * <p>Which of those apply is decided by {@link VendSource} first and {@link CredentialUse}
   * second, because the two sources hold different things. An integration vends only from a
   * temporary session, so it owes the full triple on either path -- see the reasoning at the {@code
   * requireSessionTriple} assignment. A connector can legitimately hold a long-lived static key,
   * and for one of those {@link CredentialUse#QUERY} hands credentials straight to the scan
   * engine's FileIO for reads that happen now and registers no refresh provider, so the renewal
   * requirement has no meaning there: enforcing it would reject credentials that read perfectly
   * well, and reject them terminally on a path where nothing is retrying.
   *
   * <p>An expiry that has already passed is the third requirement, split out into {@link
   * #requireLiveExpiry} because it is the one condition here whose answer depends on how far past
   * it is.
   */
  static void requireUsableCredentials(
      FloecatConnector.VendedStorageCredentials vended,
      Instant now,
      String namespaceFq,
      String tableName,
      CredentialUse use,
      VendSource source) {
    requireUsableCredentials(
        vended.properties(), vended.expiresAt(), now, namespaceFq, tableName, use, source);
  }

  private static void requireUsableCredentials(
      Map<String, String> props,
      Instant expiresAt,
      Instant now,
      String namespaceFq,
      String tableName,
      CredentialUse use,
      VendSource source) {
    // A tuple with neither a session token nor an expiry is a long-lived static key, not a session
    // credential missing its renewal fields, and reconcile can use one: it fails
    // isRefreshableExecutionCredential, so the merge path embeds it statically -- the right answer
    // for a credential that never expires. Unity vends this shape for an external location backed
    // by long-lived keys, so refusing it would leave such a table queryable but never capturable.
    //
    // Connector only, and the reason is what the two sources hold rather than a matter of taste.
    //
    // A catalog integration vends only when the credential it holds is itself a temporary session,
    // and an AWS temporary credential is always the triple. A pair arriving without a session token
    // therefore does not mean "long-lived and fine": it means the integration is not holding what
    // it is supposed to. Accepting it would also publish a credential floecat cannot bound -- the
    // authority path refuses to mint a client-facing credential with no known expiry for the same
    // reason -- and it would travel into worker payloads and loadTable responses, where a consumer
    // cannot tell an absent expiry from "never expires".
    // Reads a null expiry as "permanent", which is only sound while null means the catalog said
    // nothing. It cannot be tightened by inspecting the raw property instead: this record carries
    // its expiry as a parsed field independent of the property map -- connectors populate one
    // without the other -- so by the time the value arrives here, "absent" and "rejected" are the
    // same null. That is why the connector-side parser deliberately does not reject an out-of-unit
    // expiry; see FloecatConnector.VendedStorageCredentials.expiryFromEpochMillis.
    String sessionToken = props.get("s3.session-token");
    boolean longLivedStaticKey =
        source == VendSource.CONNECTOR
            && (sessionToken == null || sessionToken.isBlank())
            && expiresAt == null;
    // An integration always needs the full triple, on either path. The reason a token-less pair is
    // refused is what it says about the integration -- it vends only from a temporary session, so a
    // missing token means it is not holding one -- and that does not depend on what the caller
    // intends to do with the result. A query vend travels into scan payloads and back over the RPC
    // just as a reconcile one does, so "read once" is not a reason to publish something unbounded.
    boolean requireSessionTriple =
        source == VendSource.CATALOG_INTEGRATION
            || (use == CredentialUse.RECONCILE && !longLivedStaticKey);
    List<String> required =
        requireSessionTriple
            ? List.of("s3.access-key-id", "s3.secret-access-key", "s3.session-token")
            : List.of("s3.access-key-id", "s3.secret-access-key");
    List<String> missing = new java.util.ArrayList<>();
    for (String key : required) {
      String value = props.get(key);
      if (value == null || value.isBlank()) {
        missing.add(key);
      }
    }
    // Exempts the static key above, which has nothing to expire. What stays terminal is a
    // session token whose expiry is absent: that one cannot be renewed and would lapse
    // mid-capture. Only absent, not past -- the two are disjoint, and an expiry already in the
    // past belongs to requireLiveExpiry at the end of this method, which weighs it against the
    // skew tolerance and the calling path instead of folding it in here as a missing field.
    if (requireSessionTriple && expiresAt == null) {
      missing.add("s3.session-token-expires-at-ms");
    }
    if (!missing.isEmpty()) {
      // The whole tuple, not just the expiry. An access key and secret with an expiry but no
      // session token satisfies isExecutionBoundStorageCredential yet fails
      // isRefreshableExecutionCredential, so the reconciler embeds it statically and never renews
      // -- recreating exactly the defect the expiry check was added to close.
      //
      // Structured and terminal: a catalog that omits a field will keep omitting it, and a bare
      // FAILED_PRECONDITION is classified retryable, so the job would loop forever rather than
      // fail mid-read.
      throw SourceCatalogVendingGrpcStatus.vendedCredentialsNotRefreshable(
          "source catalog vended unusable storage credentials for "
              + boundedTable(namespaceFq, tableName)
              + "; missing "
              + String.join(", ", missing));
    }
    requireLiveExpiry(expiresAt, now, namespaceFq, tableName, use, source);
  }

  /**
   * Rejects an expiry that has already passed, always as a retryable failure.
   *
   * <p>{@link #EXPIRY_SKEW_TOLERANCE} decides whether the timestamp is evidence at all. Inside it,
   * it is not: a credential can expire between the catalog issuing it and us reading it, and
   * floecat's clock is not the catalog's. {@link CredentialUse#QUERY} therefore reads it -- it
   * registers no refresh provider, the credential is almost certainly live, and refusing would fail
   * a scan that would have worked. {@link CredentialUse#RECONCILE} still asks for a retry, because
   * it cannot register a refresh provider against an expiry already in the past.
   *
   * <p>Outside the tolerance the credential is dead, and both paths fail -- but retryably, not
   * terminally. A past expiry is a temporal condition: the next vend mints a new credential, which
   * is why {@code integrationStatus} lists an expired credential among the failures a retry clears.
   * Terminal belongs to what will not change, such as a tuple missing a field the catalog never
   * sends, and {@link #requireUsableCredentials} already covers that. The asymmetry decides the
   * rest: an over-eager terminal permanently fails a capture job, an over-eager retry only costs
   * time, and the reconciler's own attempt budget bounds the retrying.
   *
   * <p>Failing here rather than at S3 still matters. {@code ServerSideFileIoPropertiesResolver}
   * consults this vendor only once no storage authority covers the location, so there is no
   * fall-back to lose, and handing a credential known to be dead to the scan engine converts a
   * diagnosable failure into an opaque 403 partway through a file group.
   */
  private static void requireLiveExpiry(
      Instant expiresAt,
      Instant now,
      String namespaceFq,
      String tableName,
      CredentialUse use,
      VendSource source) {
    // A connector read is exempt, which is where this check started: before the vend paths were
    // shared it was gated on RECONCILE, and a scan that reads a connector-vended tuple once
    // registers no refresh provider and has nothing to renew. Sharing the method quietly extended
    // it to connector queries, failing scans that had been reading fine -- an integration is the
    // one this branch deliberately made stricter, so the exemption is written by source rather
    // than left to the shared path.
    if (source == VendSource.CONNECTOR && use == CredentialUse.QUERY) {
      return;
    }
    if (expiresAt == null || expiresAt.isAfter(now)) {
      return;
    }
    boolean race = expiresAt.isAfter(now.minus(EXPIRY_SKEW_TOLERANCE));
    if (race && use == CredentialUse.QUERY) {
      return;
    }
    String detail =
        "source catalog vended expired storage credentials for "
            + boundedTable(namespaceFq, tableName)
            + "; s3.session-token-expires-at-ms is "
            + expiresAt
            + ", now "
            + now;
    // Retryable regardless of path, and deliberately so even when the timestamp is old. The
    // mechanism that argues for terminal -- Entry.resolveCredentials swallowing a retryable refusal
    // while its snapshot is still live, re-vending once per signed request -- only exists once a
    // provider is registered. It does not reach the first vend, which has no snapshot behind it and
    // no evidence that re-vending returns the same timestamp, and whose failure the reconciler's
    // attempt budget does bound. Terminal there would permanently fail a capture job on one stale
    // or cached response, and on any clock disagreement past the tolerance it would fail every
    // table at once. The asymmetry decides it: an over-eager terminal is unrecoverable, an
    // over-eager retry only costs time. Bounding the refresh-path loop belongs in the registry
    // that swallows the failure, not here.
    throw retryableVendFailure(detail, null);
  }

  /**
   * Notes that a vended access point is being ignored, and uses the credential regardless.
   *
   * <p>Unity returns {@code access_point} on the credentials response for an external location that
   * has one configured. That is routing, not a statement that bucket-addressed requests will be
   * refused: the underlying grant commonly permits addressing the bucket directly, and every read
   * path here addresses the bucket named in the object URI. So the ARN is dropped and the rest of
   * the tuple is used, rather than refusing a credential that most likely reads.
   *
   * <p>Logged rather than silent, because the case it cannot serve is a grant that really is
   * access-point-only: those reads fail at storage with a 403 that says nothing about why, and this
   * line is the breadcrumb. At INFO, not WARN: vending runs per file group, so a workspace whose
   * external location has an access point -- an ordinary setup, and one this reads fine -- would
   * otherwise raise a warning per group of every table indefinitely, which trains operators to
   * filter away the very line they need when the 403 does come. Addressing the ARN properly means
   * threading the table's own bucket through both readers so a foreign absolute path in a Delta log
   * is not retargeted, and Iceberg has no property for it at all -- see the enhancement issue.
   */
  private static void noteIgnoredAccessPoint(
      Map<String, String> properties, String namespaceFq, String tableName) {
    String accessPoint = properties.get("s3.access-point");
    if (accessPoint == null || accessPoint.isBlank()) {
      return;
    }
    LOG.infof(
        "source catalog vended an S3 access point for %s, which floecat does not address;"
            + " using the credential against the bucket in the object URI. A read that fails with"
            + " 403 here means the grant is access-point-only: accessPoint=%s",
        boundedTable(namespaceFq, tableName),
        LogSafeText.bounded(accessPoint, MAX_LOGGED_PREFIX_CHARS));
  }

  /**
   * Classifies a source-catalog failure so the reconciler can stop retrying what will never
   * succeed.
   *
   * <p>Only recognisable authentication and authorization refusals become terminal. Anything else
   * -- a connection reset, a 5xx, a timeout -- is genuinely transient and keeps INTERNAL so the
   * existing retry behaviour still applies.
   */
  static StatusRuntimeException catalogFailureStatus(
      RuntimeException cause,
      Connector connector,
      String namespaceFq,
      String tableName,
      CredentialUse use) {
    // Every interpolated value here is catalog-influenced: the two identifiers come off the
    // persisted UpstreamRef, and cause carries whatever the catalog -- or a proxy in front of it --
    // returned. Flattened before it reaches the log, because a newline in any of them forges an
    // entry, and bounded because an HTML error page in an exception message is a flood. Generously
    // bounded rather than tightly: the split below deliberately puts the diagnostic in the log and
    // a summary in the status, so cutting this to the status's size would defeat it.
    String raw =
        String.format(
            "source catalog %s failed to vend credentials for %s: %s",
            connector.getResourceId().getId(), boundedTable(namespaceFq, tableName), cause);
    String detail = LogSafeText.bounded(raw, MAX_LOGGED_DETAIL_CHARS);
    // Bounded far tighter for the status than for the log. grpc-message is percent-encoded trailer
    // metadata that travels to every consumer and competes with HTTP/2's header-size limits, so an
    // HTML error page there is unreadable noise; the log above holds the longer form. Through the
    // same helper rather than substring, which can cut a surrogate pair in half and emit a lone
    // surrogate into that metadata -- bounded cuts on a code-point boundary and marks the elision.
    //
    // From raw rather than from detail: bounded reports the length of whatever it was handed, so
    // bounding an already-bounded string makes the marker describe the intermediate truncation
    // instead of the original -- understating what was dropped, and cutting away the accurate
    // marker to do it.
    // Deliberately not built from raw. Once these refusals carry a keyed template whose body is
    // the detail parameter, whatever is here becomes the message a client reads -- and raw
    // interpolates cause.toString(), which is upstream-controlled: a proxy's HTML, an internal host
    // name, the shape of somebody else's stack. The class name is as much of the throwable as a
    // caller needs to tell one failure from another; the whole of it stays in the log line below.
    String description =
        LogSafeText.bounded(
            String.format(
                "source catalog %s failed to vend credentials for %s: %s",
                connector.getResourceId().getId(),
                boundedTable(namespaceFq, tableName),
                cause.getClass().getSimpleName()),
            MAX_STATUS_DESCRIPTION);

    StatusRuntimeException terminal = terminalStatus(cause, description);
    if (terminal != null) {
      // cause is passed so the stack trace survives: which of the several refusal sites fired is
      // the diagnostic. Its message reappears in the trace unflattened, which a determined catalog
      // could use to forge a line, but a trace is multi-line by nature and any consumer parsing
      // this log already handles that -- losing the throw site to close a weaker version of a
      // vector already closed above is a bad trade.
      LOG.warnf(cause, "%s", detail);
      return terminal;
    }
    // Retryable, and the level depends on who is behind the call. Reconcile work vends per file
    // group and its failure is reported by the reconciler anyway, so a WARN there would write a
    // stack trace per group per attempt, all describing the one outage. A query vend has neither
    // property: it runs once per scan session, and nothing else reports it -- at DEBUG a transient
    // catalog outage would leave the caller a bare INTERNAL and the server log empty.
    if (use == CredentialUse.RECONCILE) {
      LOG.debugf(cause, "%s", detail);
    } else {
      LOG.warnf(cause, "%s", detail);
    }
    // Unrecognised stays retryable: an over-eager terminal permanently fails a job, an over-eager
    // retry only costs time.
    return io.grpc.Status.INTERNAL
        .withDescription(description)
        .withCause(cause)
        .asRuntimeException();
  }

  /**
   * The terminal status for a recognised refusal, or null when the failure is retryable.
   *
   * <p>Separate from its caller so the log level can follow the verdict: a permanent refusal is
   * worth a stack trace, a timeout on a per-file-group path is not.
   */
  private static StatusRuntimeException terminalStatus(RuntimeException cause, String description) {
    // Typed exceptions only. Substring-matching the cause chain for 401/403/"access denied" gets
    // the risk backwards: a transient failure whose text merely contains one of those tokens -- a
    // gateway page echoing "Access Denied", an S3 denial during IAM propagation lag, a URL with 403
    // in it -- would be classified terminal and stop the reconciler retrying a job that would have
    // recovered. Iceberg's REST client raises NotAuthorizedException for 401 and ForbiddenException
    // for 403, so classification uses those and nothing else.
    // Membership, not self-reference: `c.getCause() == c` catches only a one-node loop, so a chain
    // where A causes B and B causes A spins here forever. SourceCatalogVendingGrpcStatus.hasReason
    // walks a cause chain the same way and already guards it this way.
    java.util.Set<Throwable> seen = new java.util.HashSet<>();
    for (Throwable c = cause; c != null && seen.add(c); c = c.getCause()) {
      if (c instanceof org.apache.iceberg.exceptions.NotAuthorizedException) {
        return SourceCatalogVendingGrpcStatus.sourceCatalogVendRefused(
            io.grpc.Status.Code.UNAUTHENTICATED, ErrorCode.MC_UNAUTHENTICATED, description, cause);
      }
      if (c instanceof org.apache.iceberg.exceptions.ForbiddenException) {
        return SourceCatalogVendingGrpcStatus.sourceCatalogVendRefused(
            io.grpc.Status.Code.PERMISSION_DENIED,
            ErrorCode.MC_PERMISSION_DENIED,
            description,
            cause);
      }
      // Format-neutral refusal for connectors that do not speak Iceberg REST (e.g. Unity Catalog
      // over HTTP): they raise a typed SourceCatalogAccessException rather than an Iceberg
      // exception, carrying whether it was an authentication or authorization failure.
      if (c instanceof SourceCatalogAccessException access) {
        return switch (access.denial()) {
          case UNAUTHENTICATED ->
              SourceCatalogVendingGrpcStatus.sourceCatalogVendRefused(
                  io.grpc.Status.Code.UNAUTHENTICATED,
                  ErrorCode.MC_UNAUTHENTICATED,
                  description,
                  cause);
          case PERMISSION_DENIED ->
              SourceCatalogVendingGrpcStatus.sourceCatalogVendRefused(
                  io.grpc.Status.Code.PERMISSION_DENIED,
                  ErrorCode.MC_PERMISSION_DENIED,
                  description,
                  cause);
          // Terminal, but not a denial, so it must not travel as one: PERMISSION_DENIED here would
          // report "you may not read this table" for a catalog that simply cannot vend for it. It
          // goes back as the structured vend-refused reason, which the reconciler matches by reason
          // rather than by the shared FAILED_PRECONDITION code.
          case UNSUPPORTED ->
              ai.floedb.floecat.storage.errors.SourceCatalogVendingGrpcStatus
                  .sourceCatalogVendRefused(description);
        };
      }
    }
    return null;
  }

  /**
   * Classifies a Catalog Integration vending failure.
   *
   * <p>Three outcomes, chosen by whether a retry or a fall-back could produce a better answer.
   * Terminal where neither can: an authorization refusal, a vanished upstream table, a
   * configuration that is wrong and will stay wrong. Retryable where the next attempt could
   * succeed: {@code CREDENTIAL_UNAVAILABLE}, the window while an integration's stored secret is
   * superseded, and an expired credential, which re-vending replaces. The asymmetry is what decides
   * the boundary -- an over-eager terminal permanently fails a capture job, an over-eager retry
   * only costs time -- but it is not a licence to demote a whole code: {@code
   * INVALID_CONFIGURATION} also covers an upstream 400 and an unrecognised auth configuration,
   * neither of which any number of retries changes.
   *
   * <p>Nothing here falls back. Every {@code CatalogAccessException} from the vend reaches this
   * method, and an Integration has no storage authority to be handed to instead -- see the class
   * javadoc for why. So {@code UNSUPPORTED} and {@code CREDENTIAL_SCOPE_INVALID} are refusals like
   * the rest: they describe the Integration rather than this attempt, and no number of retries
   * changes an auth mode the SPI does not implement or a scope that does not reach the table.
   */
  private static StatusRuntimeException catalogIntegrationFailureStatus(
      RuntimeException cause,
      CatalogAccessException accessFailure,
      CatalogIntegration integration,
      String namespaceFq,
      String tableName,
      CredentialUse use) {
    String safeCause =
        accessFailure == null
            ? cause.getClass().getSimpleName()
            : accessFailure.code() + ": " + accessFailure.getMessage();
    // Bounded and flattened exactly as catalogFailureStatus does it, and for the same reasons: both
    // identifiers come off the persisted UpstreamRef with no length or character limit, a newline
    // in either forges a log entry, and the text leaves the node as the Error detail's parameter
    // and as the grpc-message rendered from it. safeCause is already a summary rather than the raw
    // throwable, so the split here is narrower than the sibling's -- but the long form still
    // belongs in the log rather than in a trailer.
    String raw =
        String.format(
            "source Catalog Integration %s could not vend credentials for %s: %s",
            integration.getResourceId().getId(), boundedTable(namespaceFq, tableName), safeCause);
    String detail = LogSafeText.bounded(raw, MAX_LOGGED_DETAIL_CHARS);
    // From raw rather than from detail, so the elision marker describes what was actually dropped
    // instead of an intermediate truncation.
    String description = LogSafeText.bounded(raw, MAX_STATUS_DESCRIPTION);

    StatusRuntimeException status = integrationStatus(accessFailure, cause, description);
    // Same split as the connector path, on both axes. A terminal refusal is worth a stack trace. A
    // retryable one is not on the reconcile path -- vending runs per file group, so a catalog
    // outage would write one per group per attempt, all describing the same outage, and the
    // reconciler reports the classified failure anyway. A query vend has neither property: it runs
    // once per scan session and nothing else reports it, so at debug a transient outage would
    // leave the caller a bare INTERNAL and the server log empty.
    //
    // An INTERNAL carries a third reason, and it is the log or nothing. It is the one answer here
    // with no floecat Error detail, so BaseServiceImpl rebuilds it -- containsFloecatError is false
    // -- and GrpcErrors.shouldHideMessage hides every code it does not list, INTERNAL among them,
    // replacing this description with "Internal error. correlation_id=...". Attaching a detail to
    // keep the text would be arguing with that rule rather than applying it: the message is hidden
    // on purpose, and the correlation id is what joins it to the server side. So the server side
    // has to hold the cause. It also does not flood the way a retryable answer does -- the SPI
    // raises INTERNAL for a provider that broke its own contract, and an unclassified one is an
    // exception IcebergRestCatalogErrors.translate did not recognise, neither of which is the
    // repeating per-group outage the debug branch exists for.
    if (warrantsWarn(status, use)) {
      LOG.warnf(cause, "%s", detail);
    } else {
      LOG.debugf(cause, "%s", detail);
    }
    return status;
  }

  /**
   * Whether this vending failure is worth a stack trace, or belongs at debug.
   *
   * <p>Package-visible so the matrix is asserted directly rather than by capturing log output. See
   * the call site for why each arm is where it is.
   */
  static boolean warrantsWarn(StatusRuntimeException status, CredentialUse use) {
    return SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(status)
        || use == CredentialUse.QUERY
        || status.getStatus().getCode() == io.grpc.Status.Code.INTERNAL;
  }

  private static StatusRuntimeException integrationStatus(
      CatalogAccessException accessFailure, RuntimeException cause, String detail) {
    if (accessFailure == null) {
      return io.grpc.Status.INTERNAL.withDescription(detail).withCause(cause).asRuntimeException();
    }
    return switch (accessFailure.code()) {
      case UNAUTHENTICATED ->
          SourceCatalogVendingGrpcStatus.sourceCatalogVendRefused(
              io.grpc.Status.Code.UNAUTHENTICATED, ErrorCode.MC_UNAUTHENTICATED, detail, cause);
      case PERMISSION_DENIED ->
          SourceCatalogVendingGrpcStatus.sourceCatalogVendRefused(
              io.grpc.Status.Code.PERMISSION_DENIED, ErrorCode.MC_PERMISSION_DENIED, detail, cause);
      case NOT_FOUND, INVALID_CONFIGURATION ->
          SourceCatalogVendingGrpcStatus.sourceCatalogVendRefused(detail);
      // Deterministic descriptions of the Integration, not of this attempt: an auth mode the SPI
      // does not implement, or a provider reporting that what it holds does not cover the table.
      case UNSUPPORTED, CREDENTIAL_SCOPE_INVALID ->
          SourceCatalogVendingGrpcStatus.sourceCatalogVendRefused(detail);
      case CREDENTIAL_UNAVAILABLE, CREDENTIAL_EXPIRED -> retryableVendFailure(detail, cause);
      // The upstream stalled or fell over: a 503, a RESTException, a socket timeout, this path's
      // own
      // budget deadline. Exactly what the retryable reason exists to name, and saying it as a bare
      // INTERNAL costs the caller the description too -- GrpcErrors hides an INTERNAL message, so
      // the integration and table named in the detail never reach whoever has to act on it.
      case UNAVAILABLE, TIMEOUT -> retryableVendFailure(detail, cause);
      // Not the upstream's fault and not a retry's business: the SPI raises this when a provider
      // broke its own contract.
      case INTERNAL ->
          io.grpc.Status.INTERNAL.withDescription(detail).withCause(cause).asRuntimeException();
    };
  }

  /**
   * A vending failure a later attempt could clear.
   *
   * <p>Structured, for the same reason the refusals are: the reconciler classifies only the refusal
   * reasons terminally, so this keeps the retry behaviour, and {@code UNAVAILABLE} alone cannot be
   * told apart from floecat's own storage service being unreachable.
   */
  private static StatusRuntimeException retryableVendFailure(String detail, Throwable cause) {
    return SourceCatalogVendingGrpcStatus.sourceCatalogVendUnavailable(detail, cause);
  }

  /**
   * Closes a catalog client without letting the close itself become the caller's answer.
   *
   * <p>Used on both the abandoned client a timed-out open hands back and the ordinary close, which
   * try-with-resources would otherwise let escape: a provider whose close hook throws -- an HTTP
   * pool already shut down, an interrupted keep-alive -- would turn credentials already in hand
   * into an INTERNAL failure and discard them.
   */
  private static void closeQuietly(CatalogClient client) {
    if (client == null) {
      return;
    }
    try {
      client.close();
    } catch (RuntimeException e) {
      LOG.debugf(e, "closing a catalog client failed");
    }
  }

  /**
   * Closes the client the vend opened, waiting only so long.
   *
   * <p>{@link #closeQuietly} bounds exceptions, not time, and it used to run on the request thread
   * outside the budget that bounds open, capabilities and the vend -- so a provider whose teardown
   * blocked would hold a gRPC handler or a scan-planning thread past the deadline the budget exists
   * to impose, and nothing in the {@code CatalogClient} contract says close returns promptly.
   *
   * <p>Bounded rather than handed off. Firing it at a virtual thread and not waiting removes the
   * hold, but nothing then guarantees the client is released before the vend returns, so a burst
   * holds more of them open than it used to and the release is unobservable. A fresh budget keeps
   * the ordinary case inline and deterministic while capping the wait; when it lapses the close
   * carries on where it is and this stops watching.
   *
   * <p>A fresh budget, not the vend's: that one may already be spent, and {@code remainingNanos}
   * refuses to start work on an exhausted budget -- which would leak the client rather than close
   * it late.
   */
  private void closeWithinBudget(CatalogClient client) {
    if (client == null) {
      return;
    }
    try {
      CatalogUpstreamBudget.start(CLOSE_TIMEOUT, nanoTime)
          .call(
              () -> {
                client.close();
                return null;
              },
              ignored -> {});
    } catch (RuntimeException e) {
      LOG.debugf(e, "closing a catalog client failed or outlasted its budget");
    }
  }

  /**
   * A Catalog Integration that cannot vend for this read.
   *
   * <p>Terminal, and deliberately not a {@code null} fall-back. A storage authority is not a second
   * way for an Integration-backed table to reach its data -- it is the split-brain arrangement this
   * feature replaces, where floecat authenticates to the catalog and something else supplies the
   * storage credential. The Integration model has no way to express the alternative: there is no
   * authority field on the record, and {@code ValidateCatalogIntegration} reports an Integration
   * whose provider cannot vend as invalid rather than as configured differently.
   *
   * <p>So falling back here was never recovery. This vend is reached only once no authority covers
   * the location, and {@code StorageAuthorityResolver.buildResponse} raises no-matching-authority
   * for a null one -- the only consumer that absorbs that error gates on a {@code ConnectorConfig},
   * which an Integration does not have. Returning null therefore replaced a specific cause with
   * "you configured no storage authority", which is neither true nor actionable.
   */
  private static StatusRuntimeException integrationCannotVend(String detail) {
    String message =
        LogSafeText.bounded("source-catalog vending refused: " + detail, MAX_STATUS_DESCRIPTION);
    // Warned, not thrown silently, by the same rule catalogIntegrationFailureStatus applies: a
    // refusal warns, a retryable answer does not. Every status this helper builds is a refusal, so
    // the rule always says warn here. It does not flood for the reason it does not there -- a
    // refusal is terminal, so the reconcile job ends rather than repeating the line per file group
    // -- and on the query path the caller sees a scan failure and the server log otherwise sees
    // nothing at all, which is the case these conditions are most likely to be read from.
    LOG.warnf("%s", message);
    return SourceCatalogVendingGrpcStatus.sourceCatalogVendRefused(message);
  }

  private static CatalogAccessException findCatalogAccessFailure(Throwable cause) {
    java.util.Set<Throwable> seen = new java.util.HashSet<>();
    for (Throwable current = cause;
        current != null && seen.add(current);
        current = current.getCause()) {
      if (current instanceof CatalogAccessException failure) {
        return failure;
      }
    }
    return null;
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
