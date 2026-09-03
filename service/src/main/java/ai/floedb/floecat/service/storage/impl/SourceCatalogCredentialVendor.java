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
import java.util.function.Function;
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
 * table and decided that no storage authority covers the location. It returns {@code null} for
 * every "this catalog cannot or will not vend" condition so the caller can fall back, and throws
 * only when the catalog actively refused or answered unusably.
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
   * bound. A status description becomes percent-encoded grpc-message trailer metadata that travels
   * to every consumer and competes with HTTP/2 header limits, and {@code withReason} copies the
   * same text into two {@code Any} details -- three carriages of whatever the catalog named its
   * table. Flattening matters for the log line too: a name containing a newline would forge one.
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
   * What the caller will do with the credentials. Selects how strictly they are validated: only the
   * reconcile path renews them, so only it requires a renewable session tuple.
   */
  enum CredentialUse {
    /** Execution-bound capture. A refresh provider is registered, so renewal must be possible. */
    RECONCILE,
    /** Query read-back. Handed to the scan engine's FileIO for immediate reads; never renewed. */
    QUERY
  }

  @Inject ConnectorRepository connectorRepo;
  @Inject CredentialResolver credentialResolver;
  Function<ConnectorConfig, FloecatConnector> connectorFactory = ConnectorFactory::create;

  @ConfigProperty(name = "floecat.storage.aws.region", defaultValue = "us-east-1")
  String defaultRegion;

  /**
   * Vends credentials for {@code table} from the catalog it was captured from, or returns {@code
   * null} when that catalog does not delegate and the caller should fall back.
   *
   * @param table a persisted table, carrying the upstream connector reference
   * @param responseLocationPrefix prefix to stamp on the returned credential
   * @param use how the caller will use the credentials, which sets how strictly they are validated
   */
  ResolveStorageAuthorityResponse vendForTable(
      Table table, String responseLocationPrefix, CredentialUse use) {
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
          LogSafeText.bounded(connectorId.getId(), MAX_LOGGED_PREFIX_CHARS), tableId);
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

    noteIgnoredAccessPoint(vended.get(), namespaceFq, upstream.getTableDisplayName());
    requireUsableCredentials(vended.get(), namespaceFq, upstream.getTableDisplayName(), use);

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
    // Dropped rather than forwarded: the ARN is not acted on anywhere here, and advertising a
    // routing key no consumer honours invites one to start honouring it inconsistently. Same
    // reason FILE_IO_PROPERTY_KEYS omits it. noteIgnoredAccessPoint has already logged it.
    storageConfig.remove("s3.access-point");
    VendedStorageCredential.Builder credential =
        VendedStorageCredential.newBuilder()
            .setPrefix(
                responsePrefix(
                    vended.get(),
                    responseLocationPrefix,
                    namespaceFq,
                    upstream.getTableDisplayName()))
            .putAllConfig(Map.copyOf(storageConfig));
    Instant expiresAt = vended.get().expiresAt();
    if (expiresAt != null) {
      credential.setExpiresAt(Timestamps.fromMillis(expiresAt.toEpochMilli()));
    }
    LOG.infof(
        "vended storage credentials from source catalog connector=%s table=%s expiresAt=%s",
        connector.getResourceId().getId(),
        boundedTable(namespaceFq, upstream.getTableDisplayName()),
        expiresAt);
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
    String raw = vended == null ? null : vended.scopePrefix();
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
    if (requested.isBlank() || StorageAuthorityResolver.matchesLocationPrefix(scope, requested)) {
      return scope;
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
    if (!StorageAuthorityResolver.matchesLocationPrefix(requested, scope)) {
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
   * <p>{@link CredentialUse#QUERY} hands credentials straight to the scan engine's FileIO for reads
   * that happen now, and registers no refresh provider, so the renewal requirement has no meaning
   * there. Enforcing it anyway would reject credentials that read perfectly well -- and reject them
   * with a terminal classification, on a path where nothing is retrying and there is no job to fail
   * terminally.
   */
  static void requireUsableCredentials(
      FloecatConnector.VendedStorageCredentials vended,
      String namespaceFq,
      String tableName,
      CredentialUse use) {
    Map<String, String> props = vended.properties();
    // A tuple with neither a session token nor an expiry is a long-lived static key, not a session
    // credential missing its renewal fields, and reconcile can use one: it fails
    // isRefreshableExecutionCredential, so the merge path embeds it statically -- which is the
    // right answer for a credential that never expires. Unity returns this shape for an external
    // location backed by long-lived keys, and refusing it here would leave such a table queryable
    // but never capturable.
    String sessionToken = props.get("s3.session-token");
    boolean longLivedStaticKey =
        (sessionToken == null || sessionToken.isBlank()) && vended.expiresAt() == null;
    List<String> required =
        use == CredentialUse.RECONCILE && !longLivedStaticKey
            ? List.of("s3.access-key-id", "s3.secret-access-key", "s3.session-token")
            : List.of("s3.access-key-id", "s3.secret-access-key");
    List<String> missing = new java.util.ArrayList<>();
    for (String key : required) {
      String value = props.get(key);
      if (value == null || value.isBlank()) {
        missing.add(key);
      }
    }
    // Already-expired counts as missing, not as present. RefreshingAwsCredentialsProviderRegistry
    // reads a past expiry as "refresh now" -- computeRefreshSkew returns Duration.ZERO and
    // shouldRefresh is then true on every resolveCredentials call -- so a catalog that keeps
    // returning the same stale timestamp is re-vended once per credential resolution and still
    // hands back credentials S3 will refuse. Failing here instead names the field once, terminally.
    // Skipped for the static key above, which has nothing to expire. What stays terminal is the
    // dangerous middle case: a session token present with an expiry missing or already past. That
    // one is a session credential floecat cannot renew, so it would lapse mid-capture with nothing
    // to recover it.
    if (use == CredentialUse.RECONCILE
        && !longLivedStaticKey
        && (vended.expiresAt() == null || !vended.expiresAt().isAfter(Instant.now()))) {
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
                + boundedTable(namespaceFq, tableName)
                + "; missing "
                + String.join(", ", missing));
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
      FloecatConnector.VendedStorageCredentials vended, String namespaceFq, String tableName) {
    String accessPoint = vended.properties().get("s3.access-point");
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
    String description = LogSafeText.bounded(raw, MAX_STATUS_DESCRIPTION);

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
        return io.grpc.Status.UNAUTHENTICATED
            .withDescription(description)
            .withCause(cause)
            .asRuntimeException();
      }
      if (c instanceof org.apache.iceberg.exceptions.ForbiddenException) {
        return io.grpc.Status.PERMISSION_DENIED
            .withDescription(description)
            .withCause(cause)
            .asRuntimeException();
      }
      // Format-neutral refusal for connectors that do not speak Iceberg REST (e.g. Unity Catalog
      // over HTTP): they raise a typed SourceCatalogAccessException rather than an Iceberg
      // exception, carrying whether it was an authentication or authorization failure.
      if (c instanceof SourceCatalogAccessException access) {
        return switch (access.denial()) {
          case UNAUTHENTICATED ->
              io.grpc.Status.UNAUTHENTICATED
                  .withDescription(description)
                  .withCause(cause)
                  .asRuntimeException();
          case PERMISSION_DENIED ->
              io.grpc.Status.PERMISSION_DENIED
                  .withDescription(description)
                  .withCause(cause)
                  .asRuntimeException();
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
