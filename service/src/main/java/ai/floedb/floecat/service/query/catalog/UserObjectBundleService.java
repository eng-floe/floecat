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

package ai.floedb.floecat.service.query.catalog;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.connector.common.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeKind;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.ColumnFailure;
import ai.floedb.floecat.query.rpc.ColumnFailureCode;
import ai.floedb.floecat.query.rpc.ColumnInfo;
import ai.floedb.floecat.query.rpc.ColumnResult;
import ai.floedb.floecat.query.rpc.ColumnStatus;
import ai.floedb.floecat.query.rpc.EngineSpecific;
import ai.floedb.floecat.query.rpc.FlightEndpointRef;
import ai.floedb.floecat.query.rpc.Origin;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.RelationKind;
import ai.floedb.floecat.query.rpc.RelationPinIdentity;
import ai.floedb.floecat.query.rpc.RelationPinSet;
import ai.floedb.floecat.query.rpc.RelationResolution;
import ai.floedb.floecat.query.rpc.RelationResolutions;
import ai.floedb.floecat.query.rpc.ResolutionFailure;
import ai.floedb.floecat.query.rpc.ResolutionStatus;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SqlDefinition;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.query.rpc.TableReferenceCandidate;
import ai.floedb.floecat.query.rpc.UserObjectsBundleChunk;
import ai.floedb.floecat.query.rpc.UserObjectsBundleEnd;
import ai.floedb.floecat.query.rpc.UserObjectsBundleHeader;
import ai.floedb.floecat.query.rpc.ViewDefinition;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.MetadataResolutionContext;
import ai.floedb.floecat.scanner.spi.StatsProvider;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.PinValidator;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.QueryPins;
import ai.floedb.floecat.service.query.ViewContextUtils;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.query.resolver.QueryInputResolver;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.spi.decorator.ColumnDecoration;
import ai.floedb.floecat.systemcatalog.spi.decorator.DecorationException;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecorator;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecoratorProvider;
import ai.floedb.floecat.systemcatalog.spi.decorator.RelationDecoration;
import ai.floedb.floecat.systemcatalog.spi.decorator.ViewDecoration;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import ai.floedb.floecat.types.Hashing;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeFormat;
import io.opentelemetry.api.trace.Span;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class UserObjectBundleService {

  private static final int MAX_RESOLUTIONS_PER_CHUNK = 25;
  private static final Logger LOG = Logger.getLogger(UserObjectBundleService.class);
  private static final Set<String> LOCAL_FLIGHT_HOSTS = Set.of("localhost", "127.0.0.1", "0.0.0.0");
  private static final String SYSTEM_FLIGHT_ENDPOINTS_PREFIX = "floedb.system-flight.endpoints.";
  private static final String RELATION_HINT_PERSIST_NANOS_KEY =
      "decorator.relation_hint_persist_nanos";
  private static final String COLUMN_HINT_PERSIST_NANOS_KEY = "decorator.column_hint_persist_nanos";
  private static final String COLUMN_WARM_HIT_COUNT_KEY = "decorator.column_warm_hits";

  private final CatalogOverlay overlay;
  private final QueryInputResolver inputResolver;
  private final QueryContextStore queryStore;
  private final EngineMetadataDecoratorProvider decoratorProvider;
  private final EngineContextProvider engineContext;
  private final boolean engineSpecificEnabled;
  // Bumped when the engine decorator's behavior changes WITHOUT moving the engine version; folded
  // into the identity-only possession token so a decorator change invalidates cached decoration.
  private final String decorationEpoch;
  private final StatsProviderFactory statsFactory;
  private final PinValidator pinValidator;
  private final long slowRpcMs;
  private final LogicalSchemaMapper logicalSchemaMapper = new LogicalSchemaMapper();
  private final FlightEndpointRef floecatFlightEndpoint;

  @Inject Observability observability;

  private static void warnFlightHost(String flightHost, String quarkusProfile) {
    if (flightHost == null) {
      return;
    }
    String normalized =
        flightHost
            .trim()
            .toLowerCase(Locale.ROOT)
            .replaceAll("^\\[(.*)]$", "$1"); // handle IPv6 braces
    boolean isLocalHost = LOCAL_FLIGHT_HOSTS.contains(normalized);
    boolean isDevProfile =
        quarkusProfile != null
            && (quarkusProfile.equalsIgnoreCase("dev") || quarkusProfile.equalsIgnoreCase("test"));
    if (isLocalHost && !isDevProfile) {
      LOG.warnf(
          "floecat.flight.advertised-host=%s resolves to %s; configure"
              + " FLOECAT_FLIGHT_ADVERTISED_HOST to a routable endpoint before running in prod so"
              + " workers can connect.",
          flightHost, normalized);
    }
  }

  private PhaseDiagnostics diagnostics(String operation) {
    return observability == null
        ? PhaseDiagnostics.NOOP
        : observability.diagnostics("service", operation);
  }

  @Inject
  public UserObjectBundleService(
      CatalogOverlay overlay,
      QueryInputResolver inputResolver,
      QueryContextStore queryStore,
      StatsProviderFactory statsFactory,
      EngineMetadataDecoratorProvider decoratorProvider,
      EngineContextProvider engineContext,
      PinValidator pinValidator,
      @ConfigProperty(name = "floecat.catalog.bundle.emit_engine_specific", defaultValue = "true")
          boolean engineSpecificEnabled,
      @ConfigProperty(name = "floecat.catalog.bundle.decoration_epoch", defaultValue = "1")
          String decorationEpoch,
      @ConfigProperty(name = "floecat.flight.advertised-host", defaultValue = "localhost")
          String flightHost,
      @ConfigProperty(name = "floecat.flight.advertised-port", defaultValue = "80") int flightPort,
      @ConfigProperty(name = "quarkus.grpc.server.plain-text", defaultValue = "true")
          boolean grpcPlainText,
      @ConfigProperty(name = "quarkus.profile", defaultValue = "prod") String quarkusProfile,
      @ConfigProperty(name = "floecat.rpc.log.slow-ms", defaultValue = "250") long slowRpcMs) {
    this.overlay = overlay;
    this.inputResolver = inputResolver;
    this.queryStore = queryStore;
    this.statsFactory = statsFactory;
    this.decoratorProvider = decoratorProvider;
    this.engineContext = engineContext;
    this.pinValidator = pinValidator;
    this.engineSpecificEnabled = engineSpecificEnabled;
    this.decorationEpoch = safe(decorationEpoch);
    this.slowRpcMs = Math.max(0L, slowRpcMs);
    this.floecatFlightEndpoint =
        FlightEndpointRef.newBuilder()
            .setHost(flightHost)
            .setPort(flightPort)
            .setTls(!grpcPlainText)
            .build();
    warnFlightHost(flightHost, quarkusProfile);
  }

  UserObjectBundleService(
      CatalogOverlay overlay,
      QueryInputResolver inputResolver,
      QueryContextStore queryStore,
      StatsProviderFactory statsFactory,
      EngineMetadataDecoratorProvider decoratorProvider,
      EngineContextProvider engineContext,
      boolean engineSpecificEnabled,
      String flightHost,
      int flightPort,
      boolean grpcPlainText,
      String quarkusProfile) {
    // Test-only: these tests never reach per-read pin validation (their schema flows go through
    // the fake overlay). Fail explicitly if one ever does, rather than NPE-ing on null repos.
    this(
        overlay,
        inputResolver,
        queryStore,
        statsFactory,
        decoratorProvider,
        engineContext,
        new PinValidator(
            null, ai.floedb.floecat.service.catalog.impl.RootRepairRequests.disabled()) {
          @Override
          public void validate(String correlationId, ai.floedb.floecat.query.rpc.TablePin pin) {
            throw new IllegalStateException(
                "test-only UserObjectBundleService has no repositories to validate pins");
          }
        },
        engineSpecificEnabled,
        "1",
        flightHost,
        flightPort,
        grpcPlainText,
        quarkusProfile,
        250L);
  }

  /** {@link #stream(String, QueryContext, List, Set)} with no possession hint. */
  public Multi<UserObjectsBundleChunk> stream(
      String correlationId, QueryContext ctx, List<TableReferenceCandidate> tables) {
    return stream(correlationId, ctx, tables, Set.of());
  }

  public Multi<UserObjectsBundleChunk> stream(
      String correlationId,
      QueryContext ctx,
      List<TableReferenceCandidate> tables,
      Set<String> knownBlobVersions) {
    List<TableReferenceCandidate> candidates = List.copyOf(tables);
    if (LOG.isDebugEnabled()) {
      LOG.debugf(
          "GetUserObjects stream start query_id=%s correlation_id=%s candidates=%d"
              + " default_catalog_id=%s",
          ctx.getQueryId(),
          correlationId,
          candidates.size(),
          ctx.getQueryDefaultCatalogId().getId());
    }
    return Multi.createFrom()
        .<UserObjectsBundleChunk>deferred(
            () -> {
              UserObjectBundleIterator iterator =
                  new UserObjectBundleIterator(correlationId, ctx, candidates, knownBlobVersions);
              return Multi.createFrom()
                  .iterable(() -> iterator)
                  .onFailure()
                  .invoke(ignored -> iterator.publishStreamTelemetry("failed"))
                  .onTermination()
                  .invoke(() -> iterator.publishStreamTelemetry("cancelled"));
            });
  }

  private List<QueryInput> normalizeCandidates(
      String correlationId,
      TableReferenceCandidate candidate,
      Supplier<String> defaultCatalogSupplier) {
    if (candidate.getCandidatesCount() == 0) {
      throw GrpcErrors.invalidArgument(correlationId, CATALOG_BUNDLE_CANDIDATE_MISSING, Map.of());
    }
    List<QueryInput> normalized = new ArrayList<>(candidate.getCandidatesCount());
    for (QueryInput input : candidate.getCandidatesList()) {
      if (!input.hasName()) {
        normalized.add(input);
        continue;
      }

      // Only apply the query default catalog when the incoming NameRef is not already
      // fully-qualified.
      NameRef name = input.getName();
      if (name.getCatalog().isEmpty() || name.getCatalog().isBlank()) {
        NameRef adjusted =
            UserObjectBundleUtils.applyDefaultCatalog(name, defaultCatalogSupplier.get());
        normalized.add(input.toBuilder().setName(adjusted).build());
      } else {
        normalized.add(input);
      }
    }
    return normalized;
  }

  private Optional<ResolvedRelation> selectResolvedRelation(
      String correlationId,
      TableReferenceCandidate candidate,
      List<QueryInput> normalizedCandidates,
      Function<NameRef, Optional<ResourceId>> nameResolver,
      Function<ResourceId, Optional<GraphNode>> nodeResolver) {
    for (QueryInput input : normalizedCandidates) {
      ResourceId relationId = extractResourceId(input, nameResolver);
      if (relationId == null) {
        continue;
      }
      Optional<GraphNode> node = nodeResolver.apply(relationId);
      if (node.isEmpty()) {
        if (input.getTargetCase() == QueryInput.TargetCase.NAME) {
          throw new GraphNodeMissingException(
              relationId, "Id " + relationId + " does not map to any known object");
        }
        continue;
      }
      GraphNode gn = node.get();
      if (!(gn instanceof RelationNode rel)) {
        throw new GraphNodeMissingException(
            relationId,
            "Resolved id " + relationId + " maps to non-relation node kind=" + gn.kind());
      }
      return Optional.of(new ResolvedRelation(candidate, relationId, rel, input));
    }
    return Optional.empty();
  }

  private ResourceId extractResourceId(
      QueryInput input, Function<NameRef, Optional<ResourceId>> nameResolver) {
    switch (input.getTargetCase()) {
      case TABLE_ID:
        return input.getTableId();
      case VIEW_ID:
        return input.getViewId();
      case NAME:
        return nameResolver.apply(input.getName()).orElse(null);
      default:
        return null;
    }
  }

  private RelationPinSet collectChunkPins(
      String correlationId,
      QueryContext ctx,
      List<ResolvedRelation> relations,
      Map<ResourceId, TablePin> currentSnapshotPinCache,
      PhaseDiagnostics diagnostics) {
    if (relations == null || relations.isEmpty()) {
      return RelationPinSet.getDefaultInstance();
    }
    diagnostics.add("pin.relations", relations.size());
    List<QueryInput> inputs = new ArrayList<>(relations.size());
    long buildInputsStartNs = System.nanoTime();
    for (ResolvedRelation relation : relations) {
      QueryInput input = buildCanonicalQueryInput(relation);
      if (input != null) {
        inputs.add(input);
      }
    }
    diagnostics.nanos("pin.build_inputs", System.nanoTime() - buildInputsStartNs);
    diagnostics.add("pin.inputs", inputs.size());
    if (inputs.isEmpty()) {
      return RelationPinSet.getDefaultInstance();
    }
    long asOfStartNs = System.nanoTime();
    var asOfDefault = ctx.parseAsOfDefault(correlationId);
    diagnostics.nanos("pin.asof_default", System.nanoTime() - asOfStartNs);
    long resolverStartNs = System.nanoTime();
    var resolution =
        inputResolver.resolveInputs(
            ctx.getQueryId(),
            correlationId,
            inputs,
            asOfDefault,
            Optional.of(ctx.getQueryDefaultCatalogId()),
            currentSnapshotPinCache,
            diagnostics);
    diagnostics.nanos("pin.resolver", System.nanoTime() - resolverStartNs);
    RelationPinSet incoming = resolution.relationPinSet();
    RelationPinSet pins = incoming == null ? RelationPinSet.getDefaultInstance() : incoming;
    diagnostics.add("pin.output_pins", pins.getPinsCount());
    return pins;
  }

  private UserObjectsBundleChunk headerChunk(String queryId, int seq) {
    UserObjectsBundleHeader header = UserObjectsBundleHeader.newBuilder().build();
    return UserObjectsBundleChunk.newBuilder()
        .setQueryId(queryId)
        .setSeq(seq)
        .setHeader(header)
        .build();
  }

  private UserObjectsBundleChunk resolutionsChunk(
      String queryId, int seq, List<RelationResolution> resolutions) {
    RelationResolutions chunk = RelationResolutions.newBuilder().addAllItems(resolutions).build();
    return UserObjectsBundleChunk.newBuilder()
        .setQueryId(queryId)
        .setSeq(seq)
        .setResolutions(chunk)
        .build();
  }

  private UserObjectsBundleChunk endChunk(
      String queryId, int seq, int resolutionCount, int foundCount, int notFoundCount) {
    UserObjectsBundleEnd end =
        UserObjectsBundleEnd.newBuilder()
            .setResolutionCount(resolutionCount)
            .setFoundCount(foundCount)
            .setNotFoundCount(notFoundCount)
            .build();
    return UserObjectsBundleChunk.newBuilder().setQueryId(queryId).setSeq(seq).setEnd(end).build();
  }

  private static final class TimingAccumulator {
    private long statsLookupNanos;
    private long decorateRelationNanos;
    private long decorateViewNanos;
    private long decorateColumnsNanos;
    private long decorateColumnInvokeNanos;
    private long decorateCompleteNanos;
    private long decoratePersistRelationNanos;
    private long decoratePersistColumnsNanos;
    private long decorateColumnWarmHits;

    private void addStatsLookupNanos(long nanos) {
      statsLookupNanos += nanos;
    }

    private long statsLookupNanos() {
      return statsLookupNanos;
    }

    private void addDecorateRelationNanos(long nanos) {
      decorateRelationNanos += nanos;
    }

    private long decorateRelationNanos() {
      return decorateRelationNanos;
    }

    private void addDecorateViewNanos(long nanos) {
      decorateViewNanos += nanos;
    }

    private long decorateViewNanos() {
      return decorateViewNanos;
    }

    private void addDecorateColumnsNanos(long nanos) {
      decorateColumnsNanos += nanos;
    }

    private long decorateColumnsNanos() {
      return decorateColumnsNanos;
    }

    private void addDecorateColumnInvokeNanos(long nanos) {
      decorateColumnInvokeNanos += nanos;
    }

    private long decorateColumnInvokeNanos() {
      return decorateColumnInvokeNanos;
    }

    private void addDecorateCompleteNanos(long nanos) {
      decorateCompleteNanos += nanos;
    }

    private long decorateCompleteNanos() {
      return decorateCompleteNanos;
    }

    private void addDecoratePersistRelationNanos(long nanos) {
      decoratePersistRelationNanos += nanos;
    }

    private long decoratePersistRelationNanos() {
      return decoratePersistRelationNanos;
    }

    private void addDecoratePersistColumnsNanos(long nanos) {
      decoratePersistColumnsNanos += nanos;
    }

    private long decoratePersistColumnsNanos() {
      return decoratePersistColumnsNanos;
    }

    private void addDecorateColumnWarmHits(long warmHits) {
      decorateColumnWarmHits += warmHits;
    }

    private long decorateColumnWarmHits() {
      return decorateColumnWarmHits;
    }

    private long decorationTotalNanos() {
      return decorateRelationNanos
          + decorateViewNanos
          + decorateColumnsNanos
          + decorateCompleteNanos;
    }
  }

  /**
   * True when the payload built for this candidate carries the relation's complete column set (no
   * projection). Mirrors {@link UserObjectBundleUtils#pruneSchema} exactly: a candidate that wants
   * all columns, or names none, is served the full schema. The pin-identity token is only stamped
   * for such responses (see buildRelation), so a cached version always denotes the full schema.
   */
  private static boolean servesFullSchema(TableReferenceCandidate candidate) {
    return candidate.getWantsAllColumns() || candidate.getInitialColumnsCount() == 0;
  }

  /*
   * The opaque pin identity for a resolved relation. Tables carry the query
   * pin's identity, frozen at first touch. Views and system relations have no
   * query pin in V1; they carry a derived content token — the SHA-256 of the
   * relation id and the node's cache identity (see below for why the id is
   * required) — which is immutable per content version, leaks no URI or
   * storage authority, and (with an empty constraints ref) states the
   * deterministic truth that no constraints bundle exists for them.
   */
  private Optional<PinIdentitySource> pinIdentityFor(
      String correlationId, ResolvedRelation relation, QueryContext queryContext) {
    // Only a USER table carries a per-query snapshot pin; route it through the pin's identity.
    // Views AND system tables have no query pin, so they take the derived content token below —
    // previously system tables fell into the pin branch and emitted no identity at all, so
    // clients could never cache them despite that being the whole point of the content token.
    // Discriminate on kind+origin (a system table is also a TABLE node, and a view may be USER
    // origin) rather than the concrete node class, so the routing holds for every node backing.
    if (relation.node().kind() == GraphNodeKind.TABLE
        && relation.node().origin() == GraphNodeOrigin.USER) {
      return queryContext
          .findTablePin(relation.relationId(), correlationId)
          .map(pin -> new PinIdentitySource(QueryPins.identity(pin), schemaScope(pin)));
    }
    String cacheIdentity = relation.node().cacheIdentity();
    if (cacheIdentity == null || cacheIdentity.isBlank()) {
      return Optional.empty();
    }
    // Derived content token for views and system relations: a hash of the relation id plus the
    // node's registry cacheIdentity. The relation id is ESSENTIAL, not decoration: SystemTableNode
    // does not override GraphNode.cacheIdentity(), which returns the bare catalog-fingerprint
    // version (SystemNodeRegistry hands every system table in a catalog the same value), so hashing
    // cacheIdentity alone would collide across all system tables — a client that cached one would
    // be served another identity-only under the shared token and reuse the wrong schema. Mixing the
    // id in makes the token unique per relation while still moving with engine content (the version
    // changes on catalog upgrade). It also folds in a system table's resolved EXECUTION metadata
    // (backend kind + the resolved Flight/storage endpoint) — an identity-only reply omits that
    // metadata, and a config-resolved endpoint (configuredEndpointForKey) can change without moving
    // cacheIdentity. A floecat redeploy does NOT reset an external caching client, so without this
    // the client would match the token, get no endpoint, and route to the stale one. The endpoint is
    // resolved through resolveSystemExecution — the same helper buildRelation uses to stamp it — so
    // the token cannot drift from the served routing.
    ResourceId relId = relation.relationId();
    StringBuilder keyMaterial =
        new StringBuilder()
            .append(relId.getAccountId())
            .append('\0')
            .append(relId.getId())
            .append('\0')
            .append(relId.getKindValue())
            .append('\0')
            .append(cacheIdentity);
    if (relation.node() instanceof SystemTableNode systemTableNode) {
      keyMaterial.append('\0').append(resolveSystemExecution(systemTableNode).tokenMaterial());
    }
    // A CONTENT-derived identity: only table_blob_version is meaningful. A view or system relation
    // has no query snapshot pin, so snapshot_id, pin_kind, pin_fingerprint, and constraints_ref
    // stay unset (0 / UNSPECIFIED / empty) — deliberately, not as a placeholder. Consumers must key
    // such a relation on table_blob_version alone and MUST NOT read the snapshot-pin fields off it
    // (there is no snapshot to describe). The in-repo planner does exactly this — it reads only
    // table_blob_version, constraints_ref_version, and snapshot_id off pin_identity and never
    // branches on pin_kind (see RPC_parsing.cpp) — so the present-but-defaulted fields are inert.
    // No schema scope either: the content hash above IS the schema identity.
    return Optional.of(
        new PinIdentitySource(
            RelationPinIdentity.newBuilder()
                .setTableBlobVersion(Hashing.sha256Hex(keyMaterial.toString()))
                .build(),
            ""));
  }

  /**
   * A wire-facing pin identity plus the server-side schema-scope material its possession token
   * folds in. The scope stays OFF the identity (RelationPinIdentity is planner-facing; the
   * fingerprint is internal pin state) — this pair is how it travels from pinIdentityFor to
   * possessionToken without widening the wire message.
   */
  private record PinIdentitySource(RelationPinIdentity identity, String schemaScope) {}

  /**
   * The schema-scope material a table pin contributes to the possession token: the read-schema
   * fingerprint stamped on the pinned manifest entry, or — for pins built from pre-fingerprint
   * entries — the snapshot blob version (correct but coarser: it also moves on data-only ingests,
   * so legacy entries run cold on ingest until their next snapshot write stamps a fingerprint).
   */
  private static String schemaScope(ai.floedb.floecat.query.rpc.TablePin pin) {
    return pin.getSchemaFingerprint().isBlank()
        ? pin.getSnapshotBlobVersion()
        : pin.getSchemaFingerprint();
  }

  /**
   * A system table's resolved execution metadata: the backend kind plus the concrete endpoint the
   * bundle serves (a Flight endpoint, whether built-in, node-declared, or config-resolved, or a
   * storage-path fallback). Resolved in ONE place so buildRelation (which stamps these fields) and
   * pinIdentityFor (which folds them into the possession token) can never disagree — the token must
   * cover exactly the routing an identity-only reply omits.
   */
  private record SystemExecution(String backendKind, FlightEndpointRef flightEndpoint, String storagePath) {
    String tokenMaterial() {
      // Build from the endpoint's explicit, contractual fields (host/port/tls) rather than
      // FlightEndpointRef.toString(): protobuf documents Message.toString() as non-contractual and
      // subject to change, and this token is persisted by clients and matched across queries. The
      // reserved `ticket` field is deliberately excluded — workers must not inspect it, and it is
      // not routing identity. The token then moves exactly when the routing it covers moves.
      String endpoint =
          flightEndpoint != null
              ? flightEndpoint.getHost() + ':' + flightEndpoint.getPort() + ':' + flightEndpoint.getTls()
              : "";
      return backendKind + '\0' + endpoint + '\0' + storagePath;
    }
  }

  private SystemExecution resolveSystemExecution(SystemTableNode node) {
    String backendKind = String.valueOf(node.backendKind());
    if (node instanceof SystemTableNode.FloeCatSystemTableNode) {
      return new SystemExecution(backendKind, floecatFlightEndpoint, "");
    }
    if (node instanceof SystemTableNode.StorageSystemTableNode storage) {
      if (storage.flightEndpoint() != null) {
        return new SystemExecution(backendKind, storage.flightEndpoint(), "");
      }
      Optional<FlightEndpointRef> configured = configuredEndpointForKey(storage.storageEndpointKey());
      if (configured.isPresent()) {
        return new SystemExecution(backendKind, configured.get(), "");
      }
      if (!storage.storagePath().isBlank()) {
        return new SystemExecution(backendKind, null, storage.storagePath());
      }
    }
    return new SystemExecution(backendKind, null, "");
  }

  /**
   * The pin identity as stamped on the wire, with its {@code table_blob_version} scoped to the
   * SERVED PAYLOAD rather than the bare content version (see {@link #possessionToken}). Both the
   * full-response stamp and the identity-only match go through here, so the token a client
   * advertises and the token the gate compares can never drift.
   */
  private Optional<RelationPinIdentity> scopedPinIdentity(
      String correlationId,
      ResolvedRelation relation,
      QueryContext queryContext,
      EngineContext ctx) {
    return pinIdentityFor(correlationId, relation, queryContext)
        .map(
            src ->
                src.identity().toBuilder()
                    .setTableBlobVersion(
                        possessionToken(
                            src.identity().getTableBlobVersion(), src.schemaScope(), ctx))
                    .build());
  }

  /**
   * The possession token a caching client advertises
   * (GetUserObjectsRequest.known_table_blob_versions) and the identity-only gate matches on. It
   * must identify the WITHHELD PAYLOAD, not merely the content version: withheld columns carry
   * engine-keyed payload (decorateColumns / hasRequiredEnginePayload), so a bare content version
   * would let a client that shares one catalog cache across engines — or that spans an
   * engine-version or decorator upgrade — advertise a version decorated for engine A, be served
   * identity-only under engine B, and reuse engine-A decoration for an engine-B query. The
   * requesting engine is already on the wire (EngineContext), so we fold it in server-side at both
   * mint sites; the client stays engine-agnostic and correctness no longer depends on it keying its
   * own cache by engine.
   *
   * <p>The token folds in a SCHEMA scope ({@code schemaScope}), because the served column schema is
   * read from the pinned snapshot (schema-on-read) and CreateSnapshot/UpdateSnapshot can change
   * that schema WITHOUT moving the definition ref (table_blob_version). A definition-only token
   * would therefore let a client that holds an old schema be served identity-only for a NEW schema
   * and reuse stale columns/types. The scope is the read-schema fingerprint stamped on the pinned
   * manifest entry (SnapshotManifestEntry.schema_fingerprint): identical read schemas share it, so
   * a data-only ingest keeps the token — and the client's schema — warm, while a snapshot-backed
   * schema change moves it. Pins built from pre-fingerprint manifest entries fall back to the
   * snapshot blob version (see {@link #schemaScope}): still never stale, just cold on every ingest
   * until the table's next snapshot write stamps a fingerprint. Views and system relations pass an
   * empty scope — their content hash is already the schema identity.
   *
   * <p>{@code decorationEpoch} additionally invalidates cached decoration when the decorator's
   * behavior changes without moving the engine version. When there is nothing to fold in — no
   * schema scope (views/system) AND no engine decoration — the token IS the content version,
   * byte-identical to the unscoped behavior.
   */
  private String possessionToken(String contentVersion, String schemaScope, EngineContext ctx) {
    if (contentVersion == null || contentVersion.isBlank()) {
      return contentVersion;
    }
    String scope = safe(schemaScope);
    boolean decorate = decorationRequired(ctx);
    if (scope.isBlank() && !decorate) {
      return contentVersion;
    }
    StringBuilder material = new StringBuilder(contentVersion).append('\0').append(scope);
    if (decorate) {
      material
          .append('\0')
          .append(safe(ctx.normalizedKind()))
          .append('\0')
          .append(safe(ctx.normalizedVersion()))
          .append('\0')
          .append(decorationEpoch);
    }
    return Hashing.sha256Hex(material.toString());
  }

  /*
   * Identity-only response when the request proved possession of the exact
   * content version this resolution serves: the payload (schema, columns,
   * view definition, decoration) is omitted — the identity plus the
   * lightweight stats are all a caching client needs, and the omitted bytes
   * are provably identical to what it holds. A generic conditional-request
   * feature, never client-special-casing: servers MAY ignore the hint and
   * clients MUST treat a full payload as equally correct. Returns null when
   * the relation must be built in full.
   */
  private RelationInfo identityOnlyOrNull(
      ResolvedRelation relation,
      Optional<RelationPinIdentity> scopedIdentity,
      StatsProvider statsProvider,
      Set<String> knownBlobVersions,
      TimingAccumulator timings) {
    // The token is the engine-scoped payload token (scopedIdentity), not the bare content version,
    // so a client that proved possession under a different engine cannot be served identity-only.
    // A blank version can never prove possession: a user table whose definition blob had no etag
    // resolves to table_blob_version="" (the repository defaults a missing etag to empty), and
    // every such table would otherwise share that key — one cached, the rest served the wrong
    // schema identity-only. Force the full payload rather than match on the empty string.
    if (knownBlobVersions.isEmpty()
        || scopedIdentity.isEmpty()
        || scopedIdentity.get().getTableBlobVersion().isBlank()
        || !knownBlobVersions.contains(scopedIdentity.get().getTableBlobVersion())) {
      return null;
    }
    RelationInfo.Builder slim = baseRelationInfo(relation).setPinIdentity(scopedIdentity.get());
    // Time the stats lookup exactly as the full path does (buildRelation): the slim path still hits
    // the stats provider, which can block on its latency budget. Without this, once the
    // conditional-request feature is doing its job a large fraction of resolutions would contribute
    // zero to statsLookup telemetry and the slow-RPC threshold.
    long statsLookupStartNs = System.nanoTime();
    attachTableStats(slim, relation.relationId(), statsProvider);
    timings.addStatsLookupNanos(System.nanoTime() - statsLookupStartNs);
    return slim.build();
  }

  /**
   * A {@link RelationInfo} builder carrying the identity fields every response sets — id, canonical
   * name, kind, and origin. Both the slim identity-only reply and the full payload start here, so
   * the two can never disagree on a relation's identity.
   */
  private RelationInfo.Builder baseRelationInfo(ResolvedRelation relation) {
    return RelationInfo.newBuilder()
        .setRelationId(relation.relationId())
        .setName(canonicalName(relation.relationId(), relation.node()))
        .setKind(mapKind(relation.node().kind(), relation.node().origin()))
        .setOrigin(mapOrigin(relation.node().origin()));
  }

  /**
   * Attach the relation's live snapshot-scoped estimates (row count, size) when the stats provider
   * has them. Both response paths keep these on the wire: they move with every ingest, so a caching
   * client relies on the reply to refresh them even when the schema payload is omitted.
   */
  private static void attachTableStats(
      RelationInfo.Builder builder, ResourceId relationId, StatsProvider statsProvider) {
    statsProvider
        .tableStats(relationId)
        .map(StatsProviderFactory::toRelationStats)
        .ifPresent(builder::setStats);
  }

  private RelationInfo buildRelation(
      String correlationId,
      ResolvedRelation relation,
      QueryContext queryContext,
      MetadataResolutionContext resolutionContext,
      StatsProvider statsProvider,
      TimingAccumulator timings,
      Optional<RelationPinIdentity> scopedIdentity) {
    if (LOG.isTraceEnabled()) {
      LOG.tracef(
          "Building relation bundle query_id=%s relation=%s kind=%s origin=%s",
          queryContext.getQueryId(),
          relation.relationId(),
          relation.node().kind(),
          relation.node().origin());
    }

    // origin is needed below for columnsFor; kind and name are set via baseRelationInfo.
    Origin origin = mapOrigin(relation.node().origin());

    List<SchemaColumn> schemaColumns =
        relation.node() instanceof ViewNode view
            ? view.outputColumns()
            : relation.node() instanceof UserTableNode userTable
                ? UserObjectBundleUtils.qualifyNestedColumnNames(
                    logicalSchemaForRelation(
                            correlationId, relation.relationId(), userTable, queryContext)
                        .getColumnsList())
                : overlay.tableSchema(relation.node().id());

    List<SchemaColumn> pruned =
        UserObjectBundleUtils.pruneSchema(schemaColumns, relation.candidate(), correlationId);

    List<ColumnInfo> columns =
        UserObjectBundleUtils.columnsFor(schemaColumns, pruned, origin, correlationId);

    RelationInfo.Builder builder = baseRelationInfo(relation);

    /*
     * Populate the bundled endpoint metadata so workers know how to reach the table. FLOECAT
     * tables always use our built-in Flight server, and STORAGE tables can either point at their
     * own Flight endpoint, use an endpoint key resolved from service config, or expose a storage
     * path fallback. ENGINE tables never set an endpoint.
     */
    if (relation.node() instanceof SystemTableNode systemTableNode) {
      // Resolve through the shared helper — the SAME implementation pinIdentityFor uses to fold
      // routing into the token — so the served routing and the token that covers it cannot drift.
      // It is invoked independently at each site (a cheap in-memory config lookup), not memoized
      // across them; both resolve deterministically from the same node, so they always agree.
      SystemExecution exec = resolveSystemExecution(systemTableNode);
      builder.setBackendKind(systemTableNode.backendKind());
      if (exec.flightEndpoint() != null) {
        builder.setFlightEndpoint(exec.flightEndpoint());
      } else if (!exec.storagePath().isBlank()) {
        builder.setStoragePath(exec.storagePath());
      }
    }

    long statsLookupStartNs = System.nanoTime();
    attachTableStats(builder, relation.relationId(), statsProvider);
    timings.addStatsLookupNanos(System.nanoTime() - statsLookupStartNs);

    // If this is a view, keep a mutable builder around for decoration.
    ViewDefinition.Builder viewBuilder = null;
    if (relation.node() instanceof ViewNode view) {
      viewBuilder = viewDefinitionBuilder(view);
      builder.setViewDefinition(viewBuilder);
    }

    // The engine captured at iterator construction, not a live provider re-read: this runs on
    // executor threads where the request context is unreliable, and a silently empty engine would
    // skip engine-specific decoration with no log line (eng-floe/floecat#361).
    EngineContext ctx = resolutionContext.engineContext();
    boolean decorationRequired = decorationRequired(ctx);
    Optional<EngineMetadataDecorator> decorator = currentDecorator(ctx);
    RelationDecoration relationDecoration = null;
    boolean relationDecorationSucceeded = true;
    // Per-phase payload-decoration success, tracked separately from
    // relationDecorationSucceeded (which gates hint commits below). The possession-token stamp
    // needs EVERY payload phase to have succeeded — a view or completion failure leaves the served
    // payload incomplete, and stamping a token for it would lock that incomplete payload into a
    // caching client until it happened to re-miss, instead of self-healing on the next query.
    boolean viewDecorationSucceeded = true;
    boolean completeRelationSucceeded = true;
    long relationDecorationBeforeNanos = timings.decorationTotalNanos();

    if (decorationRequired && decorator.isPresent()) {
      relationDecoration =
          new RelationDecoration(
              builder,
              relation.relationId(),
              relation.node(),
              requireSchema(schemaColumns),
              requireSchema(pruned),
              resolutionContext);

      try {
        long decorateRelationStartNs = System.nanoTime();
        try {
          decorator.get().decorateRelation(ctx, relationDecoration);
        } finally {
          timings.addDecorateRelationNanos(System.nanoTime() - decorateRelationStartNs);
        }
      } catch (RuntimeException e) {
        relationDecorationSucceeded = false;
        LOG.debugf(
            e,
            "Decorator threw while decorating relation %s (engine=%s)",
            relation.relationId(),
            ctx.normalizedKind());
      }

      // Decorate columns
      // handled below so columns can always emit READY/FAILED status

      // decorate view
      if (viewBuilder != null) {
        ViewDecoration viewDecoration =
            new ViewDecoration(
                builder, viewBuilder, relation.relationId(), relation.node(), resolutionContext);

        try {
          long decorateViewStartNs = System.nanoTime();
          try {
            decorator.get().decorateView(ctx, viewDecoration);
          } finally {
            timings.addDecorateViewNanos(System.nanoTime() - decorateViewStartNs);
          }
        } catch (RuntimeException e) {
          viewDecorationSucceeded = false;
          LOG.debugf(
              e,
              "Decorator threw while decorating view %s (engine=%s)",
              relation.relationId(),
              ctx.normalizedKind());
        }
      }
    }

    List<ColumnResult> columnResults =
        decorateColumns(
            columns,
            pruned,
            relationDecoration,
            decorator,
            ctx,
            decorationRequired,
            relation.relationId(),
            timings);
    long relationWarmHitCount = decorationCounter(relationDecoration, COLUMN_WARM_HIT_COUNT_KEY);
    timings.addDecorateColumnWarmHits(relationWarmHitCount);

    if (relationDecoration != null && decorator.isPresent()) {
      boolean commitRelationHints = relationDecorationSucceeded;
      boolean commitColumnHints =
          relationDecorationSucceeded && shouldCommitColumnDecorations(columnResults);
      Set<Long> readyColumnIds = commitColumnHints ? readyColumnIds(columnResults) : Set.of();
      if (LOG.isDebugEnabled()) {
        LOG.debugf(
            "Decorator completion decisions relation=%s relation_succeeded=%s"
                + " commit_relation_hints=%s commit_column_hints=%s ready_column_ids=%d",
            relation.relationId(),
            relationDecorationSucceeded,
            commitRelationHints,
            commitColumnHints,
            readyColumnIds.size());
      }
      try {
        long decorateCompleteStartNs = System.nanoTime();
        try {
          decorator
              .get()
              .completeRelation(
                  ctx, relationDecoration, commitRelationHints, commitColumnHints, readyColumnIds);
        } finally {
          timings.addDecorateCompleteNanos(System.nanoTime() - decorateCompleteStartNs);
          timings.addDecoratePersistRelationNanos(
              decorationTimingNanos(relationDecoration, RELATION_HINT_PERSIST_NANOS_KEY));
          timings.addDecoratePersistColumnsNanos(
              decorationTimingNanos(relationDecoration, COLUMN_HINT_PERSIST_NANOS_KEY));
        }
      } catch (RuntimeException e) {
        completeRelationSucceeded = false;
        LOG.debugf(
            e,
            "Decorator threw while completing relation %s (engine=%s)",
            relation.relationId(),
            ctx == null ? "" : ctx.normalizedKind());
      }
    }

    if (LOG.isDebugEnabled()) {
      long relationDecorationNanos =
          Math.max(0L, timings.decorationTotalNanos() - relationDecorationBeforeNanos);
      long relationColdMissCount = Math.max(0L, columnResults.size() - relationWarmHitCount);
      LOG.debugf(
          "Built relation bundle relation=%s columns=%d ready=%d failed=%d warm=%d cold=%d"
              + " decorationMs=%.1f",
          relation.relationId(),
          columnResults.size(),
          countColumnsWithStatus(columnResults, ColumnStatus.COLUMN_STATUS_OK),
          countColumnsWithStatus(columnResults, ColumnStatus.COLUMN_STATUS_FAILED),
          relationWarmHitCount,
          relationColdMissCount,
          relationDecorationNanos / 1_000_000.0);
    }

    // Stamp the pin identity. Two distinct concerns share the message and must NOT share a gate:
    //
    //   - The DATA identity (pin_fingerprint, snapshot id, AS-OF provenance, constraints_ref_version)
    //     is a property of the pinned relation, not of the served payload shape. Callers rely on it
    //     to tell a current pin from a historical one and to skip the constraints RPC, so it is
    //     stamped UNCONDITIONALLY whenever the relation is pinned — including on projected or
    //     decoration-incomplete replies, which previously lost it entirely.
    //
    //   - The possession token (table_blob_version) is payload-scoped: a client that advertises it
    //     is later served identity-only and reuses its cached payload verbatim. It is kept only when
    //     the served payload is complete and cacheable, and blanked otherwise:
    //       * full schema — a projected subset must never advertise "I hold every column", or a
    //         later request would be starved of columns it never received;
    //       * every payload-decoration phase succeeded (relation, view, completion) and no column
    //         ended up FAILED — else a transient decoration failure would lock into a caching
    //         client instead of self-healing next query;
    //       * non-blank — a blank version can never prove possession (the match path rejects it).
    //
    // scopedIdentity is computed once by the caller and threaded into both the identity-only match
    // and this stamp, so a cache miss under a populated hint set does not hash the relation twice.
    boolean payloadCacheable =
        servesFullSchema(relation.candidate())
            && relationDecorationSucceeded
            && viewDecorationSucceeded
            && completeRelationSucceeded
            && countColumnsWithStatus(columnResults, ColumnStatus.COLUMN_STATUS_FAILED) == 0;
    scopedIdentity.ifPresent(
        id ->
            builder.setPinIdentity(
                payloadCacheable && !id.getTableBlobVersion().isBlank()
                    ? id
                    : id.toBuilder().clearTableBlobVersion().build()));

    builder.addAllColumns(columnResults);
    return builder.build();
  }

  private static long decorationTimingNanos(
      RelationDecoration relationDecoration, String attributeKey) {
    if (relationDecoration == null || attributeKey == null || attributeKey.isBlank()) {
      return 0L;
    }
    Object value = relationDecoration.attribute(attributeKey);
    if (!(value instanceof Number number)) {
      return 0L;
    }
    return Math.max(0L, number.longValue());
  }

  private static long decorationCounter(
      RelationDecoration relationDecoration, String attributeKey) {
    if (relationDecoration == null || attributeKey == null || attributeKey.isBlank()) {
      return 0L;
    }
    Object value = relationDecoration.attribute(attributeKey);
    if (!(value instanceof Number number)) {
      return 0L;
    }
    return Math.max(0L, number.longValue());
  }

  private Optional<FlightEndpointRef> configuredEndpointForKey(String endpointKey) {
    if (endpointKey == null || endpointKey.isBlank()) {
      return Optional.empty();
    }

    String normalizedKey = endpointKey.trim();
    String prefix = SYSTEM_FLIGHT_ENDPOINTS_PREFIX + normalizedKey + ".";
    Config config = ConfigProvider.getConfig();
    Optional<String> host =
        config
            .getOptionalValue(prefix + "host", String.class)
            .map(String::trim)
            .filter(value -> !value.isBlank());
    Optional<Integer> port =
        config.getOptionalValue(prefix + "port", Integer.class).filter(value -> value > 0);
    if (host.isEmpty() || port.isEmpty()) {
      LOG.debugf(
          "Storage endpoint key '%s' has no config at %shost/%sport; falling back to storage path",
          normalizedKey, prefix, prefix);
      return Optional.empty();
    }

    boolean tls = config.getOptionalValue(prefix + "tls", Boolean.class).orElse(false);
    return Optional.of(
        FlightEndpointRef.newBuilder().setHost(host.get()).setPort(port.get()).setTls(tls).build());
  }

  List<ColumnResult> decorateColumns(
      List<ColumnInfo> columns,
      List<SchemaColumn> pruned,
      RelationDecoration relationDecoration,
      Optional<EngineMetadataDecorator> decorator,
      EngineContext ctx,
      boolean decorationRequired,
      ResourceId relationId) {
    return decorateColumns(
        columns,
        pruned,
        relationDecoration,
        decorator,
        ctx,
        decorationRequired,
        relationId,
        new TimingAccumulator());
  }

  private List<ColumnResult> decorateColumns(
      List<ColumnInfo> columns,
      List<SchemaColumn> pruned,
      RelationDecoration relationDecoration,
      Optional<EngineMetadataDecorator> decorator,
      EngineContext ctx,
      boolean decorationRequired,
      ResourceId relationId,
      TimingAccumulator timings) {

    if (pruned == null || pruned.size() != columns.size()) {
      String msg =
          String.format(
              "Column/schema mismatch columns=%d pruned=%s",
              columns.size(), pruned == null ? "null" : Integer.toString(pruned.size()));
      LOG.debugf("Column decoration mismatch relation=%s %s", relationId, msg);
      if (!decorationRequired) {
        return columns.stream().map(UserObjectBundleService::readyColumn).toList();
      }
      List<ColumnResult> failed = new ArrayList<>(columns.size());
      for (ColumnInfo column : columns) {
        failed.add(
            failedColumn(
                column,
                ColumnFailureCode.COLUMN_FAILURE_CODE_SCHEMA_MISMATCH,
                msg,
                Map.of("relation_id", relationId.getId())));
      }
      return failed;
    }

    if (!decorationRequired) {
      return columns.stream().map(UserObjectBundleService::readyColumn).toList();
    }

    if (decorator.isEmpty() || relationDecoration == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debugf(
            "Column decoration unavailable relation=%s engine_kind=%s engine_version=%s",
            relationId,
            safe(ctx == null ? null : ctx.normalizedKind()),
            safe(ctx == null ? null : ctx.normalizedVersion()));
      }
      List<ColumnResult> failed = new ArrayList<>(columns.size());
      for (ColumnInfo column : columns) {
        failed.add(
            failedColumn(
                column,
                ColumnFailureCode.COLUMN_FAILURE_CODE_DECORATOR_UNAVAILABLE,
                "Engine-specific column decorator is unavailable",
                Map.of(
                    "engine_kind", safe(ctx == null ? null : ctx.normalizedKind()),
                    "engine_version", safe(ctx == null ? null : ctx.normalizedVersion()))));
      }
      return failed;
    }

    List<ColumnResult> decorated = new ArrayList<>(columns.size());
    for (int i = 0; i < columns.size(); i++) {
      long decorateColumnTotalStartNs = System.nanoTime();
      ColumnInfo column = columns.get(i);
      SchemaColumn schema = pruned.get(i);
      ColumnInfo.Builder builder = column.toBuilder();
      LogicalType logicalType = parseLogicalType(schema);
      ColumnDecoration columnDecoration =
          new ColumnDecoration(
              builder, schema, logicalType, column.getOrdinal(), relationDecoration);
      try {
        long decorateColumnInvokeStartNs = System.nanoTime();
        try {
          decorator.get().decorateColumn(ctx, columnDecoration);
        } finally {
          timings.addDecorateColumnInvokeNanos(System.nanoTime() - decorateColumnInvokeStartNs);
        }
        ColumnInfo decoratedColumn = columnDecoration.builder().build();
        if (hasRequiredEnginePayload(decoratedColumn, ctx)) {
          decorated.add(readyColumn(decoratedColumn));
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debugf(
                "Column decoration missing required payload relation=%s column=%s ordinal=%d"
                    + " engine_kind=%s",
                relationId,
                column.getName(),
                column.getOrdinal(),
                safe(ctx == null ? null : ctx.normalizedKind()));
          }
          decorated.add(
              failedColumn(
                  decoratedColumn,
                  ColumnFailureCode.COLUMN_FAILURE_CODE_ENGINE_PAYLOAD_REQUIRED_MISSING,
                  "Engine-specific payload is required but missing",
                  Map.of(
                      "engine_kind", safe(ctx == null ? null : ctx.normalizedKind()),
                      "engine_version", safe(ctx == null ? null : ctx.normalizedVersion()))));
        }
      } catch (RuntimeException e) {
        ColumnFailure failure = mapFailure(e, ctx);
        LOG.debugf(
            e,
            "Decorator threw while decorating column %s.%s (engine=%s mapped_code=%s"
                + " extension_code=%d)",
            relationId,
            column.getName(),
            ctx == null ? "" : ctx.normalizedKind(),
            failure.getCode(),
            failure.hasExtensionCodeValue() ? failure.getExtensionCodeValue() : 0);
        decorated.add(failedColumn(column, failure));
      } finally {
        timings.addDecorateColumnsNanos(System.nanoTime() - decorateColumnTotalStartNs);
      }
    }
    return decorated;
  }

  private static ColumnResult readyColumn(ColumnInfo column) {
    return ColumnResult.newBuilder()
        .setColumnId(column.getId())
        .setColumnName(column.getName())
        .setOrdinal(column.getOrdinal())
        .setStatus(ColumnStatus.COLUMN_STATUS_OK)
        .setColumn(column)
        .build();
  }

  private static ColumnResult failedColumn(
      ColumnInfo column, ColumnFailureCode code, String message, Map<String, String> details) {
    ColumnFailure.Builder failure = ColumnFailure.newBuilder().setCode(code).setMessage(message);
    if (details != null && !details.isEmpty()) {
      failure.putAllDetails(details);
    }
    return ColumnResult.newBuilder()
        .setColumnId(column.getId())
        .setColumnName(column.getName())
        .setOrdinal(column.getOrdinal())
        .setStatus(ColumnStatus.COLUMN_STATUS_FAILED)
        .setFailure(failure)
        .build();
  }

  private static ColumnResult failedColumn(ColumnInfo column, ColumnFailure failure) {
    return ColumnResult.newBuilder()
        .setColumnId(column.getId())
        .setColumnName(column.getName())
        .setOrdinal(column.getOrdinal())
        .setStatus(ColumnStatus.COLUMN_STATUS_FAILED)
        .setFailure(failure)
        .build();
  }

  private ColumnFailure mapFailure(RuntimeException e, EngineContext ctx) {
    if (e instanceof DecorationException de) {
      ColumnFailureCode code =
          de.hasExtensionCodeValue()
              ? ColumnFailureCode.COLUMN_FAILURE_CODE_ENGINE_EXTENSION
              : de.code();
      String message = userFacingFailureMessage(code);
      if (de.hasExtensionCodeValue()) {
        String extensionMessage = safe(de.getMessage()).trim();
        if (!extensionMessage.isBlank()) {
          message = extensionMessage;
        }
      }
      ColumnFailure.Builder builder = ColumnFailure.newBuilder().setCode(code).setMessage(message);
      if (!de.details().isEmpty()) {
        builder.putAllDetails(de.details());
      }
      if (de.hasExtensionCodeValue()) {
        builder.setExtensionCodeValue(de.extensionCodeValue());
      }
      addEngineDetails(builder, ctx);
      if (LOG.isDebugEnabled()) {
        LOG.debugf(
            "Mapped DecorationException to column failure code=%s extension_code=%d engine_kind=%s",
            code,
            de.hasExtensionCodeValue() ? de.extensionCodeValue() : 0,
            safe(ctx == null ? null : ctx.normalizedKind()));
      }
      return builder.build();
    }

    ColumnFailureCode code = ColumnFailureCode.COLUMN_FAILURE_CODE_INTERNAL_ERROR;
    if (e instanceof SecurityException) {
      code = ColumnFailureCode.COLUMN_FAILURE_CODE_PERMISSION_DENIED;
    } else if (e instanceof UnsupportedOperationException) {
      code = ColumnFailureCode.COLUMN_FAILURE_CODE_TYPE_NOT_SUPPORTED;
    } else if (e instanceof NoSuchElementException) {
      code = ColumnFailureCode.COLUMN_FAILURE_CODE_NOT_FOUND;
    }

    ColumnFailure.Builder builder =
        ColumnFailure.newBuilder().setCode(code).setMessage(userFacingFailureMessage(code));
    addEngineDetails(builder, ctx);
    if (LOG.isDebugEnabled()) {
      LOG.debugf(
          "Mapped RuntimeException to column failure exception=%s code=%s engine_kind=%s",
          e.getClass().getSimpleName(), code, safe(ctx == null ? null : ctx.normalizedKind()));
    }
    return builder.build();
  }

  private static boolean hasRequiredEnginePayload(ColumnInfo column, EngineContext ctx) {
    String normalizedKind = ctx == null ? "" : safe(ctx.normalizedKind());
    for (EngineSpecific spec : column.getEngineSpecificList()) {
      String specKind = safe(spec.getEngineKind());
      boolean kindMatches =
          specKind.isBlank() || normalizedKind.isBlank() || specKind.equals(normalizedKind);
      if (!kindMatches) {
        continue;
      }
      if (!safe(spec.getPayloadType()).isBlank() && !spec.getPayload().isEmpty()) {
        return true;
      }
    }
    return false;
  }

  private static String userFacingFailureMessage(ColumnFailureCode code) {
    if (code == null) {
      return "Column resolution failed.";
    }
    return switch (code) {
      case COLUMN_FAILURE_CODE_SCHEMA_MISMATCH ->
          "Column metadata does not match the relation schema.";
      case COLUMN_FAILURE_CODE_DECORATOR_UNAVAILABLE ->
          "Engine-specific column metadata is unavailable.";
      case COLUMN_FAILURE_CODE_ENGINE_PAYLOAD_REQUIRED_MISSING ->
          "Required engine-specific metadata is missing for this column.";
      case COLUMN_FAILURE_CODE_PERMISSION_DENIED ->
          "Permission denied while decorating this column.";
      case COLUMN_FAILURE_CODE_TYPE_NOT_SUPPORTED ->
          "This column type is not supported by the engine metadata decorator.";
      case COLUMN_FAILURE_CODE_LOGICAL_TYPE_INVALID ->
          "The column logical type is invalid for engine metadata decoration.";
      case COLUMN_FAILURE_CODE_NOT_FOUND -> "Column metadata was not found during decoration.";
      case COLUMN_FAILURE_CODE_ENGINE_EXTENSION ->
          "Engine extension failed to provide column metadata.";
      default -> "Column resolution failed.";
    };
  }

  private static String safe(String value) {
    return value == null ? "" : value;
  }

  private static void addEngineDetails(ColumnFailure.Builder failure, EngineContext ctx) {
    if (ctx == null) {
      return;
    }
    failure.putDetails("engine_kind", safe(ctx.normalizedKind()));
    failure.putDetails("engine_version", safe(ctx.normalizedVersion()));
  }

  private static boolean shouldCommitColumnDecorations(List<ColumnResult> columnResults) {
    if (columnResults == null || columnResults.isEmpty()) {
      return true;
    }
    for (ColumnResult result : columnResults) {
      if (result.getStatus() == ColumnStatus.COLUMN_STATUS_OK) {
        return true;
      }
    }
    return false;
  }

  private static Set<Long> readyColumnIds(List<ColumnResult> columnResults) {
    if (columnResults == null || columnResults.isEmpty()) {
      return Set.of();
    }
    Set<Long> ids = new java.util.HashSet<>();
    for (ColumnResult result : columnResults) {
      if (result.getStatus() == ColumnStatus.COLUMN_STATUS_OK && result.getColumnId() > 0) {
        ids.add(result.getColumnId());
      }
    }
    return ids;
  }

  private static int countColumnsWithStatus(List<ColumnResult> columnResults, ColumnStatus status) {
    int count = 0;
    if (columnResults == null || status == null) {
      return count;
    }
    for (ColumnResult result : columnResults) {
      if (result.getStatus() == status) {
        count++;
      }
    }
    return count;
  }

  private LogicalType parseLogicalType(SchemaColumn column) {
    if (column == null) {
      return null;
    }
    String logical = column.getLogicalType();
    if (logical == null || logical.isBlank()) {
      return null;
    }
    try {
      return LogicalTypeFormat.parse(logical);
    } catch (IllegalArgumentException e) {
      LOG.debugf(e, "Failed to parse logical type '%s'", logical);
      return null;
    }
  }

  private Optional<EngineMetadataDecorator> currentDecorator(EngineContext ctx) {
    if (!decorationRequired(ctx)) {
      return Optional.empty();
    }
    return decoratorProvider.decorator(ctx);
  }

  private boolean decorationRequired(EngineContext ctx) {
    return engineSpecificEnabled && ctx != null && ctx.enginePluginOverlaysEnabled();
  }

  private static List<SchemaColumn> requireSchema(List<SchemaColumn> schema) {
    if (schema == null) {
      return List.of();
    }
    return List.copyOf(schema);
  }

  private QueryInput buildCanonicalQueryInput(ResolvedRelation relation) {
    // Built-in system relations are not version-pinned in query context snapshots.
    if (relation.node().origin() == GraphNodeOrigin.SYSTEM) {
      return null;
    }
    QueryInput.Builder builder;
    GraphNodeKind kind = relation.node().kind();
    if (kind == GraphNodeKind.TABLE) {
      builder = QueryInput.newBuilder().setTableId(relation.relationId());
    } else if (kind == GraphNodeKind.VIEW) {
      builder = QueryInput.newBuilder().setViewId(relation.relationId());
    } else {
      return null;
    }
    if (relation.selectedInput().hasSnapshot()) {
      builder.setSnapshot(relation.selectedInput().getSnapshot());
    }
    return builder.build();
  }

  private ai.floedb.floecat.query.rpc.SchemaDescriptor logicalSchemaForRelation(
      String correlationId,
      ResourceId relationId,
      UserTableNode userTable,
      QueryContext queryContext) {
    Optional<TablePin> pin = queryContext.findTablePin(relationId, correlationId);
    if (pin.isEmpty()) {
      // Not yet pinned (e.g. a relation resolved outside the pinned set): fall back to the table's
      // default schema.
      return logicalSchemaMapper.map(userTable);
    }
    // Consume the pinned snapshot identity, validating the pinned blobs; a bad pinned blob fails
    // hard rather than falling back to current catalog state.
    pinValidator.validate(correlationId, pin.get());
    SnapshotRef snapshotRef =
        SnapshotRef.newBuilder().setSnapshotId(pin.get().getSnapshotId()).build();
    CatalogOverlay.SchemaResolution resolved =
        overlay.schemaFor(
            correlationId,
            relationId,
            snapshotRef,
            pin.get().getTableBlobUri(),
            pin.get().getSnapshotBlobUri());
    return logicalSchemaMapper.map(resolved.table(), resolved.schemaJson());
  }

  private NameRef canonicalName(ResourceId id, GraphNode node) {
    return switch (node.kind()) {
      case TABLE ->
          overlay.tableName(id).orElse(NameRef.newBuilder().setName(node.displayName()).build());
      case VIEW ->
          overlay.viewName(id).orElse(NameRef.newBuilder().setName(node.displayName()).build());
      default -> NameRef.newBuilder().setName(node.displayName()).build();
    };
  }

  private ViewDefinition.Builder viewDefinitionBuilder(ViewNode view) {
    ViewDefinition.Builder builder =
        ViewDefinition.newBuilder().setCanonicalSql(view.sql()).setDialect(view.dialect());
    builder.addAllSqlDefinitions(
        view.sqlDefinitions().stream()
            .map(
                def ->
                    SqlDefinition.newBuilder()
                        .setSql(def.getSql())
                        .setDialect(def.getDialect())
                        .build())
            .toList());
    builder.addAllBaseRelations(view.baseRelations());
    builder.addAllCreationSearchPath(view.creationSearchPath());
    return builder;
  }

  private RelationKind mapKind(GraphNodeKind kind, GraphNodeOrigin origin) {
    if (kind == GraphNodeKind.VIEW && origin == GraphNodeOrigin.SYSTEM) {
      return RelationKind.RELATION_KIND_SYSTEM_VIEW;
    }
    return switch (kind) {
      case TABLE -> RelationKind.RELATION_KIND_TABLE;
      case VIEW -> RelationKind.RELATION_KIND_VIEW;
      default -> RelationKind.RELATION_KIND_UNSPECIFIED;
    };
  }

  private Origin mapOrigin(GraphNodeOrigin origin) {
    return origin == GraphNodeOrigin.SYSTEM ? Origin.ORIGIN_BUILTIN : Origin.ORIGIN_USER;
  }

  private QueryContext mergeRelationPins(
      QueryContext existing, RelationPinSet incoming, String correlationId) {
    if (incoming == null || incoming.getPinsCount() == 0) {
      return existing;
    }
    RelationPinSet current = existing.parseRelationPins(correlationId);
    RelationPinSet merged = QueryPins.mergeSets(current, incoming, correlationId);
    if (current.equals(merged)) {
      return existing;
    }
    return existing.toBuilder().relationPins(merged.toByteArray()).build();
  }

  private record ResolvedRelation(
      TableReferenceCandidate candidate,
      ResourceId relationId,
      RelationNode node,
      QueryInput selectedInput) {}

  private record RelationCacheKey(
      ResourceId relationId,
      boolean wantsAllColumns,
      List<String> initialColumns,
      String engineKind,
      String engineVersion,
      SnapshotRef snapshotOverride) {}

  private record NormalizedNameRef(String catalog, List<String> path, String name) {}

  private static final class GraphNodeMissingException extends RuntimeException {
    private final ResourceId relationId;

    private GraphNodeMissingException(ResourceId relationId, String msg) {
      super(msg);
      this.relationId = relationId;
    }

    private ResourceId relationId() {
      return relationId;
    }
  }

  private final class UserObjectBundleIterator implements Iterator<UserObjectsBundleChunk> {

    private final String correlationId;
    private final QueryContext ctx;
    private final List<TableReferenceCandidate> tables;
    private final int resolutionCount;
    private final ResourceId defaultCatalogId;
    private final StatsProvider statsProvider;
    private final MetadataResolutionContext resolutionContext;
    private final String engineKind;
    private final String engineVersion;
    /* Content versions the request proved it holds; relations resolving to
     * one of these get an identity-only response (see identityOnlyOrNull). */
    private final Set<String> knownBlobVersions;

    // Maintains the order inputs were resolved so the emitted chunk mirrors the request order.
    private final List<PendingItem> pending = new ArrayList<>(MAX_RESOLUTIONS_PER_CHUNK);
    private final Map<NormalizedNameRef, Optional<ResourceId>> nameResolutionCache =
        new HashMap<>();
    private final Map<ResourceId, Optional<GraphNode>> nodeResolutionCache = new HashMap<>();
    private final ArrayDeque<EagerBaseCursor> eagerBaseQueue = new ArrayDeque<>();
    private final Set<String> eagerBaseSeen = new HashSet<>();
    private final Map<RelationCacheKey, RelationInfo> relationInfoCache = new HashMap<>();
    private final Map<ResourceId, TablePin> currentSnapshotPinCache = new HashMap<>();
    private final TimingAccumulator timings = new TimingAccumulator();
    private final PhaseDiagnostics diagnostics = diagnostics("get_user_objects");
    private final long streamStartNs = System.nanoTime();
    private final Span parentSpan = Span.current();
    private RelationPinSet pendingChunkPins = RelationPinSet.getDefaultInstance();

    private int seq = 1;
    private int nextInputIndex = 0;
    private int foundCount = 0;
    private int notFoundCount = 0;
    private int emittedResolutionChunks = 0;
    private boolean headerEmitted = false;
    private boolean endEmitted = false;
    private final AtomicBoolean telemetryPublished = new AtomicBoolean(false);
    private boolean defaultCatalogResolved = false;
    private String defaultCatalogName = "";
    private long resolveNanos = 0L;
    private long normalizeNanos = 0L;
    private long selectRelationNanos = 0L;
    private long defaultCatalogNanos = 0L;
    private long nameResolveNanos = 0L;
    private long nodeResolveNanos = 0L;
    private long baseInjectNanos = 0L;
    private long pinCollectNanos = 0L;
    private long pinCommitNanos = 0L;
    private long relationBuildNanos = 0L;
    private long decorationNanos = 0L;
    private long defaultCatalogLookups = 0L;
    private long nameResolutionCacheHits = 0L;
    private long nameResolutionCacheMisses = 0L;
    private long nodeResolutionCacheHits = 0L;
    private long nodeResolutionCacheMisses = 0L;

    UserObjectBundleIterator(
        String correlationId,
        QueryContext ctx,
        List<TableReferenceCandidate> tables,
        Set<String> knownBlobVersions) {
      this.correlationId = correlationId;
      this.ctx = ctx;
      this.tables = tables;
      this.knownBlobVersions = knownBlobVersions;
      this.resolutionCount = tables.size();
      this.defaultCatalogId = ctx.getQueryDefaultCatalogId();
      this.statsProvider = statsFactory.forQuery(ctx, correlationId);
      EngineContext requestEngine = engineContext.engineContext();
      this.engineKind = requestEngine.normalizedKind();
      this.engineVersion = requestEngine.normalizedVersion();
      this.resolutionContext =
          MetadataResolutionContext.of(
              overlay,
              Objects.requireNonNull(ctx.getQueryDefaultCatalogId(), "query default catalog id"),
              requestEngine,
              statsProvider);
      initializeParentSpan();
      if (LOG.isDebugEnabled()) {
        LOG.debugf(
            "Initialized bundle iterator query_id=%s correlation_id=%s resolution_count=%d"
                + " default_catalog_id=%s",
            ctx.getQueryId(), correlationId, resolutionCount, defaultCatalogId.getId());
      }
    }

    @Override
    public boolean hasNext() {
      return !endEmitted;
    }

    @Override
    public UserObjectsBundleChunk next() {
      if (!headerEmitted) {
        headerEmitted = true;
        if (LOG.isDebugEnabled()) {
          LOG.debugf("Emitting header chunk query_id=%s seq=%d", ctx.getQueryId(), seq);
        }
        return headerChunk(ctx.getQueryId(), seq++);
      }

      if (pending.isEmpty() && (nextInputIndex < resolutionCount || !eagerBaseQueue.isEmpty())) {
        fillPending();
      }

      if (!pending.isEmpty()) {
        return flushResolutionChunk();
      }

      if (!endEmitted) {
        endEmitted = true;
        publishStreamTelemetry("completed");
        if (LOG.isDebugEnabled()) {
          LOG.debugf(
              "Emitting end chunk query_id=%s seq=%d resolutions=%d found=%d not_found=%d",
              ctx.getQueryId(), seq, resolutionCount, foundCount, notFoundCount);
        }
        return endChunk(ctx.getQueryId(), seq++, resolutionCount, foundCount, notFoundCount);
      }

      throw new NoSuchElementException();
    }

    private void fillPending() {
      List<ResolvedRelation> toPin = new ArrayList<>(MAX_RESOLUTIONS_PER_CHUNK);
      drainEagerBaseTables(toPin);
      while (nextInputIndex < resolutionCount && pending.size() < MAX_RESOLUTIONS_PER_CHUNK) {
        PendingItem item = resolveNextResolution();
        pending.add(item);
        if (item instanceof PendingFound found) {
          toPin.add(found.relation());
          if (found.relation().node() instanceof ViewNode view && !view.baseRelations().isEmpty()) {
            eagerBaseQueue.addLast(new EagerBaseCursor(view));
            drainEagerBaseTables(toPin);
          }
        }
      }
      if (!toPin.isEmpty()) {
        long pinStartNs = System.nanoTime();
        try {
          RelationPinSet chunkPins =
              collectChunkPins(correlationId, ctx, toPin, currentSnapshotPinCache, diagnostics);
          long accumulateStartNs = System.nanoTime();
          try {
            accumulateChunkPins(chunkPins);
          } finally {
            diagnostics.nanos("pin.accumulate", System.nanoTime() - accumulateStartNs);
          }
        } finally {
          pinCollectNanos += System.nanoTime() - pinStartNs;
        }
      }
    }

    /**
     * For a view with a populated {@code base_relations} list, eagerly resolves each base-table
     * {@link NameRef}, builds a synthetic {@link ResolvedRelation}, pins its snapshot, and adds it
     * to {@code pending} with {@code inputIndex = -1} to signal it was not explicitly requested.
     * Failures to resolve a NameRef are silently skipped (base_relations is a performance hint).
     * Duplicate base-table IDs are deduplicated across the entire request stream. When a view has
     * more base relations than fit in the current chunk, remaining base relations are carried over
     * and emitted in subsequent chunks.
     */
    private void drainEagerBaseTables(List<ResolvedRelation> toPin) {
      while (pending.size() < MAX_RESOLUTIONS_PER_CHUNK && !eagerBaseQueue.isEmpty()) {
        EagerBaseCursor cursor = eagerBaseQueue.peekFirst();
        if (cursor == null) {
          break;
        }
        if (drainEagerBaseCursor(cursor, toPin)) {
          eagerBaseQueue.removeFirst();
        }
      }
    }

    private boolean drainEagerBaseCursor(EagerBaseCursor cursor, List<ResolvedRelation> toPin) {
      List<NameRef> baseRelations = cursor.view.baseRelations();
      while (cursor.nextBaseIndex < baseRelations.size()
          && pending.size() < MAX_RESOLUTIONS_PER_CHUNK) {
        NameRef baseRef = baseRelations.get(cursor.nextBaseIndex++);
        long resolveStartNs = System.nanoTime();
        try {
          NameRef enriched =
              ViewContextUtils.enrichForViewContext(baseRef, cursor.view, defaultCatalogName());
          Optional<ResourceId> baseIdOpt = resolveNameCached(enriched);
          if (baseIdOpt.isEmpty()) {
            continue;
          }
          ResourceId baseId = baseIdOpt.get();
          String baseKey = QueryPins.pinKey(baseId);
          if (eagerBaseSeen.contains(baseKey)) {
            continue; // deduplicate
          }
          Optional<GraphNode> nodeOpt = resolveNodeCached(baseId);
          if (nodeOpt.isEmpty() || !(nodeOpt.get() instanceof RelationNode rel)) {
            continue;
          }
          eagerBaseSeen.add(baseKey);
          QueryInput syntheticInput = QueryInput.newBuilder().setTableId(baseId).build();
          ResolvedRelation syntheticRelation =
              new ResolvedRelation(
                  TableReferenceCandidate.getDefaultInstance(), baseId, rel, syntheticInput);
          // Base-table pins are already derived from the parent view candidate (including AS-OF
          // overrides). Avoid re-adding a synthetic TABLE_ID pin here, which would otherwise
          // resolve to CURRENT and can overwrite AS-OF pins in the same batch.
          pending.add(new PendingFound(-1, syntheticRelation));
        } finally {
          baseInjectNanos += System.nanoTime() - resolveStartNs;
        }
      }
      return cursor.nextBaseIndex >= baseRelations.size();
    }

    private PendingItem resolveNextResolution() {
      long resolveStartNs = System.nanoTime();
      try {
        TableReferenceCandidate candidate = tables.get(nextInputIndex);
        int inputIndex = nextInputIndex;
        nextInputIndex++;
        if (LOG.isTraceEnabled()) {
          LOG.tracef(
              "Resolving candidate query_id=%s input_index=%d candidate_count=%d",
              ctx.getQueryId(), inputIndex, candidate.getCandidatesCount());
        }
        List<QueryInput> normalized;
        long normalizeStartNs = System.nanoTime();
        try {
          normalized = normalizeCandidates(correlationId, candidate, this::defaultCatalogName);
        } finally {
          normalizeNanos += System.nanoTime() - normalizeStartNs;
        }
        try {
          long selectStartNs = System.nanoTime();
          Optional<ResolvedRelation> resolved =
              selectResolvedRelationTimed(correlationId, candidate, normalized, selectStartNs);
          if (resolved.isPresent()) {
            foundCount++;
            if (LOG.isTraceEnabled()) {
              LOG.tracef(
                  "Resolved candidate query_id=%s input_index=%d relation=%s",
                  ctx.getQueryId(), inputIndex, resolved.get().relationId());
            }
            return new PendingFound(inputIndex, resolved.get());
          }
        } catch (GraphNodeMissingException e) {
          if (LOG.isDebugEnabled()) {
            LOG.debugf(
                "Resolved candidate missing graph node query_id=%s input_index=%d resource_id=%s",
                ctx.getQueryId(), inputIndex, e.relationId() == null ? "" : e.relationId().getId());
          }
          ResolutionFailure failure =
              ResolutionFailure.newBuilder()
                  .setCode("catalog_bundle.graph.missing_node")
                  .setMessage("relation resolved but missing from graph")
                  .putDetails("resource_id", e.relationId().getId())
                  .putDetails("default_catalog", defaultCatalogForDiagnostics())
                  .addAllAttempted(normalized)
                  .build();
          return new PendingResolved(
              RelationResolution.newBuilder()
                  .setInputIndex(inputIndex)
                  .setStatus(ResolutionStatus.RESOLUTION_STATUS_ERROR)
                  .setFailure(failure)
                  .build());
        }
        notFoundCount++;
        if (LOG.isTraceEnabled()) {
          LOG.tracef(
              "Candidate not found query_id=%s input_index=%d attempted=%d",
              ctx.getQueryId(), inputIndex, normalized.size());
        }
        ResolutionFailure failure =
            ResolutionFailure.newBuilder()
                .setCode("catalog_bundle.relation_not_found")
                .setMessage("relation not found")
                .putDetails("candidate_count", Integer.toString(normalized.size()))
                .putDetails("default_catalog", defaultCatalogForDiagnostics())
                .addAllAttempted(normalized)
                .build();
        return new PendingResolved(
            RelationResolution.newBuilder()
                .setInputIndex(inputIndex)
                .setStatus(ResolutionStatus.RESOLUTION_STATUS_NOT_FOUND)
                .setFailure(failure)
                .build());
      } finally {
        resolveNanos += System.nanoTime() - resolveStartNs;
      }
    }

    private UserObjectsBundleChunk flushResolutionChunk() {
      List<PendingItem> chunkItems = new ArrayList<>(pending);
      pending.clear();
      if (LOG.isDebugEnabled()) {
        LOG.debugf(
            "Flushing resolution chunk query_id=%s seq=%d pending_items=%d pending_pins=%d",
            ctx.getQueryId(), seq, chunkItems.size(), pendingChunkPins.getPinsCount());
      }
      // Ensure pins are durable before accessing stats (which expect the QueryContext to be
      // pinned).
      long pinCommitStartNs = System.nanoTime();
      commitChunkPins();
      pinCommitNanos += System.nanoTime() - pinCommitStartNs;
      QueryContext liveCtx = null;
      List<RelationResolution> resolutions = new ArrayList<>(chunkItems.size());
      for (PendingItem item : chunkItems) {
        if (item instanceof PendingResolved resolved) {
          resolutions.add(resolved.resolution());
          continue;
        }
        PendingFound found = (PendingFound) item;
        RelationCacheKey cacheKey = relationCacheKey(found.relation());
        RelationInfo cachedInfo = relationInfoCache.get(cacheKey);
        if (cachedInfo != null) {
          resolutions.add(
              RelationResolution.newBuilder()
                  .setInputIndex(found.inputIndex())
                  .setStatus(ResolutionStatus.RESOLUTION_STATUS_FOUND)
                  .setRelation(cachedInfo)
                  .build());
          continue;
        }
        long statsBeforeNanos = timings.statsLookupNanos();
        long decorationBeforeNanos = timings.decorationTotalNanos();
        long buildStartNs = System.nanoTime();
        if (liveCtx == null) {
          liveCtx = queryStore.get(ctx.getQueryId()).orElse(ctx);
        }
        // Compute the pin identity at most once per relation: the identity-only match consults it
        // when the client sent hints, and the stamp reuses it — so a cache miss under a populated
        // hint set does not hash the relation twice. Computed for EVERY pinned relation (not only
        // full-schema ones), because the stamp now preserves the data identity even on a projected
        // reply — it merely blanks the possession token there. The extra work for a projected,
        // hint-less reply is one pin read plus a hash, off the warm path.
        Optional<RelationPinIdentity> scopedIdentity =
            scopedPinIdentity(
                correlationId, found.relation(), liveCtx, resolutionContext.engineContext());
        /* Identity-only fast path: never cached — the info cache must only
         * ever hold full payloads, or a later request that did NOT prove
         * possession would be served a payload-less relation. */
        RelationInfo slim =
            identityOnlyOrNull(
                found.relation(), scopedIdentity, statsProvider, knownBlobVersions, timings);
        if (slim != null) {
          // Account the slim path symmetric with the full path below: its stats time already landed
          // in timings via identityOnlyOrNull; fold the remaining (identity-build) time into
          // relationBuildNanos so identity-only resolutions are not invisible to the summary event.
          long buildNanos = System.nanoTime() - buildStartNs;
          long statsDeltaNanos = timings.statsLookupNanos() - statsBeforeNanos;
          relationBuildNanos += Math.max(0L, buildNanos - statsDeltaNanos);
          resolutions.add(
              RelationResolution.newBuilder()
                  .setInputIndex(found.inputIndex())
                  .setStatus(ResolutionStatus.RESOLUTION_STATUS_FOUND)
                  .setRelation(slim)
                  .build());
          continue;
        }
        RelationInfo info =
            buildRelation(
                correlationId,
                found.relation(),
                liveCtx,
                resolutionContext,
                statsProvider,
                timings,
                scopedIdentity);
        long buildNanos = System.nanoTime() - buildStartNs;
        long statsDeltaNanos = timings.statsLookupNanos() - statsBeforeNanos;
        long decorationDeltaNanos = timings.decorationTotalNanos() - decorationBeforeNanos;
        relationBuildNanos += Math.max(0L, buildNanos - statsDeltaNanos - decorationDeltaNanos);
        decorationNanos += Math.max(0L, decorationDeltaNanos);
        relationInfoCache.put(cacheKey, info);
        resolutions.add(
            RelationResolution.newBuilder()
                .setInputIndex(found.inputIndex())
                .setStatus(ResolutionStatus.RESOLUTION_STATUS_FOUND)
                .setRelation(info)
                .build());
      }
      emittedResolutionChunks++;
      if (LOG.isDebugEnabled()) {
        int chunkFound = 0;
        int chunkNotFound = 0;
        int chunkError = 0;
        for (RelationResolution resolution : resolutions) {
          switch (resolution.getStatus()) {
            case RESOLUTION_STATUS_FOUND -> chunkFound++;
            case RESOLUTION_STATUS_NOT_FOUND -> chunkNotFound++;
            case RESOLUTION_STATUS_ERROR -> chunkError++;
            default -> {}
          }
        }
        LOG.debugf(
            "Resolved chunk query_id=%s seq=%d items=%d found=%d not_found=%d error=%d",
            ctx.getQueryId(), seq, resolutions.size(), chunkFound, chunkNotFound, chunkError);
      }
      return resolutionsChunk(ctx.getQueryId(), seq++, resolutions);
    }

    private void publishStreamTelemetry(String outcome) {
      if (!telemetryPublished.compareAndSet(false, true)) {
        return;
      }
      long totalNanos = System.nanoTime() - streamStartNs;
      long schedulingNanos =
          Math.max(
              0L,
              totalNanos
                  - resolveNanos
                  - baseInjectNanos
                  - pinCollectNanos
                  - pinCommitNanos
                  - relationBuildNanos
                  - decorationNanos
                  - timings.statsLookupNanos());
      double totalMs = totalNanos / 1_000_000.0;
      double pinMs = (pinCollectNanos + pinCommitNanos) / 1_000_000.0;
      emitSummaryEvent(outcome, totalMs, pinMs, schedulingNanos / 1_000_000.0);
      updateParentSpanSummary(outcome, totalMs);

      if (totalMs >= slowRpcMs) {
        LOG.infof(
            "op=GetUserObjects slow query_id=%s correlation_id=%s totalMs=%.1f"
                + " resolveMs=%.1f baseInjectMs=%.1f pinMs=%.1f relationBuildMs=%.1f"
                + " decorationMs=%.1f statsLookupMs=%.1f schedulingMs=%.1f"
                + " candidates=%d chunks=%d found=%d notFound=%d outcome=%s",
            ctx.getQueryId(),
            correlationId,
            totalMs,
            resolveNanos / 1_000_000.0,
            baseInjectNanos / 1_000_000.0,
            pinMs,
            relationBuildNanos / 1_000_000.0,
            decorationNanos / 1_000_000.0,
            timings.statsLookupNanos() / 1_000_000.0,
            schedulingNanos / 1_000_000.0,
            resolutionCount,
            emittedResolutionChunks,
            foundCount,
            notFoundCount,
            outcome);
      }

      if (LOG.isTraceEnabled()) {
        LOG.tracef(
            "GetUserObjects telemetry query_id=%s correlation_id=%s candidates=%d chunks=%d"
                + " found=%d notFound=%d outcome=%s",
            ctx.getQueryId(),
            correlationId,
            resolutionCount,
            emittedResolutionChunks,
            foundCount,
            notFoundCount,
            outcome);
      }
    }

    private void initializeParentSpan() {
      if (!parentSpan.getSpanContext().isValid()) {
        return;
      }
      parentSpan.setAttribute("correlation_id", correlationId);
      parentSpan.setAttribute("floecat.get_user_objects.candidates", resolutionCount);
      parentSpan.setAttribute(
          "floecat.get_user_objects.default_catalog_id", defaultCatalogId.getId());
      parentSpan.setAttribute("floecat.get_user_objects.engine_kind", safe(engineKind));
      parentSpan.setAttribute("floecat.get_user_objects.engine_version", safe(engineVersion));
    }

    private void updateParentSpanSummary(String outcome, double totalMs) {
      if (!parentSpan.getSpanContext().isValid()) {
        return;
      }
      parentSpan.setAttribute("floecat.get_user_objects.outcome", safe(outcome));
      parentSpan.setAttribute("floecat.get_user_objects.duration_ms", totalMs);
      parentSpan.setAttribute("floecat.get_user_objects.chunks", emittedResolutionChunks);
      parentSpan.setAttribute("floecat.get_user_objects.found", foundCount);
      parentSpan.setAttribute("floecat.get_user_objects.not_found", notFoundCount);
    }

    // The GetUserObjects RPC has many internal sub-phases (resolve, decoration, ...). We do NOT
    // emit a span per phase -- they are not RPCs and only add noise to the trace. Per-phase
    // timings are attached as one summary event on the GetUserObjects RPC span, so Jaeger stays
    // readable for small catalog lookups.
    private void emitSummaryEvent(
        String outcome, double totalMs, double pinMs, double schedulingMs) {
      diagnostics.put("query_id", ctx.getQueryId());
      diagnostics.put("correlation_id", correlationId);
      diagnostics.put("candidates", resolutionCount);
      diagnostics.put("chunks", emittedResolutionChunks);
      diagnostics.put("found", foundCount);
      diagnostics.put("not_found", notFoundCount);
      diagnostics.put("total_ms", totalMs);
      diagnostics.nanos("resolve", resolveNanos);
      diagnostics.nanos("normalize", normalizeNanos);
      diagnostics.nanos("select_relation", selectRelationNanos);
      diagnostics.nanos("default_catalog", defaultCatalogNanos);
      diagnostics.nanos("name_resolve", nameResolveNanos);
      diagnostics.nanos("node_resolve", nodeResolveNanos);
      diagnostics.nanos("base_inject", baseInjectNanos);
      diagnostics.nanos("pin_collect", pinCollectNanos);
      diagnostics.nanos("pin_commit", pinCommitNanos);
      diagnostics.put("pin_ms", pinMs);
      diagnostics.nanos("relation_build", relationBuildNanos);
      diagnostics.nanos("decoration", decorationNanos);
      diagnostics.nanos("stats_lookup", timings.statsLookupNanos());
      diagnostics.nanos("decorate_relation", timings.decorateRelationNanos());
      diagnostics.nanos("decorate_view", timings.decorateViewNanos());
      diagnostics.nanos("decorate_columns", timings.decorateColumnsNanos());
      diagnostics.nanos("decorate_column_invoke", timings.decorateColumnInvokeNanos());
      diagnostics.nanos("decorate_complete", timings.decorateCompleteNanos());
      diagnostics.put("scheduling_ms", schedulingMs);
      diagnostics.put("decorator_warm_hits", timings.decorateColumnWarmHits());
      diagnostics.nanos(
          "hint_persist",
          timings.decoratePersistRelationNanos() + timings.decoratePersistColumnsNanos());
      diagnostics.put("default_catalog_lookups", defaultCatalogLookups);
      diagnostics.put("name_cache_hits", nameResolutionCacheHits);
      diagnostics.put("name_cache_misses", nameResolutionCacheMisses);
      diagnostics.put("node_cache_hits", nodeResolutionCacheHits);
      diagnostics.put("node_cache_misses", nodeResolutionCacheMisses);
      diagnostics.put("name_cache_entries", nameResolutionCache.size());
      diagnostics.put("node_cache_entries", nodeResolutionCache.size());
      diagnostics.put("relation_cache_entries", relationInfoCache.size());
      diagnostics.put("outcome", safe(outcome));
      diagnostics.emit("floecat.get_user_objects.summary");
    }

    private RelationCacheKey relationCacheKey(ResolvedRelation relation) {
      TableReferenceCandidate candidate = relation.candidate();
      List<String> initialColumns =
          candidate.getInitialColumnsCount() == 0
              ? List.of()
              : List.copyOf(candidate.getInitialColumnsList());
      SnapshotRef snapshotOverride =
          relation.selectedInput().hasSnapshot()
              ? relation.selectedInput().getSnapshot()
              : SnapshotRef.getDefaultInstance();
      return new RelationCacheKey(
          relation.relationId(),
          candidate.getWantsAllColumns(),
          initialColumns,
          engineKind,
          engineVersion,
          snapshotOverride);
    }

    private String defaultCatalogName() {
      if (!defaultCatalogResolved) {
        long startNs = System.nanoTime();
        try {
          defaultCatalogName =
              overlay.catalog(defaultCatalogId).map(CatalogNode::displayName).orElse("");
          defaultCatalogResolved = true;
          defaultCatalogLookups++;
        } finally {
          defaultCatalogNanos += System.nanoTime() - startNs;
        }
      }
      return defaultCatalogName;
    }

    private String defaultCatalogForDiagnostics() {
      return defaultCatalogResolved ? defaultCatalogName : "";
    }

    private Optional<ResourceId> resolveNameCached(NameRef ref) {
      NormalizedNameRef key = normalizedNameRef(ref);
      // Stored values are non-null Optional instances; null means cache miss.
      Optional<ResourceId> cached = nameResolutionCache.get(key);
      if (cached != null) {
        nameResolutionCacheHits++;
        return cached;
      }
      long startNs = System.nanoTime();
      try {
        // Pass the engine captured at iterator construction: re-reading it from the request
        // context per lookup is fragile across executor hops, and an empty engine silently
        // un-resolves engine-gated system objects (eng-floe/floecat#361).
        Optional<ResourceId> resolved =
            overlay.resolveName(correlationId, ref, resolutionContext.engineContext());
        nameResolutionCache.put(key, resolved);
        nameResolutionCacheMisses++;
        return resolved;
      } finally {
        nameResolveNanos += System.nanoTime() - startNs;
      }
    }

    private Optional<GraphNode> resolveNodeCached(ResourceId id) {
      // Stored values are non-null Optional instances; null means cache miss.
      Optional<GraphNode> cached = nodeResolutionCache.get(id);
      if (cached != null) {
        nodeResolutionCacheHits++;
        return cached;
      }
      long startNs = System.nanoTime();
      try {
        // Pass the engine captured at iterator construction: re-reading it from the request
        // context per lookup is fragile across executor hops (eng-floe/floecat#361).
        Optional<GraphNode> resolved = overlay.resolve(id, resolutionContext.engineContext());
        nodeResolutionCache.put(id, resolved);
        nodeResolutionCacheMisses++;
        return resolved;
      } finally {
        nodeResolveNanos += System.nanoTime() - startNs;
      }
    }

    private Optional<ResolvedRelation> selectResolvedRelationTimed(
        String correlationId,
        TableReferenceCandidate candidate,
        List<QueryInput> normalized,
        long selectStartNs) {
      try {
        return selectResolvedRelation(
            correlationId, candidate, normalized, this::resolveNameCached, this::resolveNodeCached);
      } finally {
        selectRelationNanos += System.nanoTime() - selectStartNs;
      }
    }

    private NormalizedNameRef normalizedNameRef(NameRef ref) {
      List<String> normalizedPath = new ArrayList<>(ref.getPathCount());
      for (String segment : ref.getPathList()) {
        normalizedPath.add(normalizeNameToken(segment));
      }
      return new NormalizedNameRef(
          normalizeNameToken(ref.getCatalog()),
          List.copyOf(normalizedPath),
          normalizeNameToken(ref.getName()));
    }

    private String normalizeNameToken(String token) {
      if (token == null) {
        return "";
      }
      return token.trim();
    }

    /**
     * Represents inputs that are ready to be emitted. Keeping items in insertion order ensures we
     * re-emit resolutions in the same order the client requested them, even after buffering pins.
     */
    private interface PendingItem {
      int inputIndex();
    }

    private static final class PendingResolved implements PendingItem {
      private final RelationResolution resolution;

      private PendingResolved(RelationResolution resolution) {
        this.resolution = resolution;
      }

      @Override
      public int inputIndex() {
        return resolution.getInputIndex();
      }

      public RelationResolution resolution() {
        return resolution;
      }
    }

    private static final class PendingFound implements PendingItem {
      private final int inputIndex;
      private final ResolvedRelation relation;

      private PendingFound(int inputIndex, ResolvedRelation relation) {
        this.inputIndex = inputIndex;
        this.relation = relation;
      }

      @Override
      public int inputIndex() {
        return inputIndex;
      }

      public ResolvedRelation relation() {
        return relation;
      }
    }

    private static final class EagerBaseCursor {
      private final ViewNode view;
      private int nextBaseIndex;

      private EagerBaseCursor(ViewNode view) {
        this.view = view;
      }
    }

    // Track every pin that must be durable before the next chunk is emitted.
    private void accumulateChunkPins(RelationPinSet incomingPins) {
      if (incomingPins == null || incomingPins.getPinsCount() == 0) {
        return;
      }
      try {
        pendingChunkPins = QueryPins.mergeSets(pendingChunkPins, incomingPins, correlationId);
      } catch (RuntimeException | Error e) {
        queryStore.releaseResolvingPinBlobs(ctx.getQueryId(), QueryPins.gcRootUris(incomingPins));
        throw e;
      }
    }

    private void commitChunkPins() {
      if (pendingChunkPins.getPinsCount() == 0) {
        return;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debugf(
            "Committing chunk pins query_id=%s pin_count=%d",
            ctx.getQueryId(), pendingChunkPins.getPinsCount());
      }
      RelationPinSet toCommit = pendingChunkPins;
      // The resolver registered these pins' blobs as transient GC roots at resolution, so they are
      // protected across the collect→commit window; this update makes the context a durable root.
      Optional<QueryContext> updated;
      try {
        updated =
            queryStore.update(
                ctx.getQueryId(), existing -> mergeRelationPins(existing, toCommit, correlationId));
      } catch (RuntimeException | Error e) {
        queryStore.releaseResolvingPinBlobs(ctx.getQueryId(), QueryPins.gcRootUris(toCommit));
        throw e;
      }
      pendingChunkPins = RelationPinSet.getDefaultInstance();
      if (updated.isEmpty()) {
        queryStore.releaseResolvingPinBlobs(ctx.getQueryId(), QueryPins.gcRootUris(toCommit));
        LOG.warnf(
            "Failed to commit chunk pins query_id=%s query context missing", ctx.getQueryId());
        throw GrpcErrors.notFound(
            correlationId, QUERY_NOT_FOUND, Map.of("query_id", ctx.getQueryId()));
      }
      if (LOG.isDebugEnabled()) {
        LOG.debugf("Committed chunk pins query_id=%s", ctx.getQueryId());
      }
    }
  }
}
