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
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeKind;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.ColumnInfo;
import ai.floedb.floecat.query.rpc.ColumnResult;
import ai.floedb.floecat.query.rpc.FlightEndpointRef;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.RelationPinIdentity;
import ai.floedb.floecat.query.rpc.RelationPinSet;
import ai.floedb.floecat.query.rpc.RelationResolution;
import ai.floedb.floecat.query.rpc.RelationResolutions;
import ai.floedb.floecat.query.rpc.ResolutionFailure;
import ai.floedb.floecat.query.rpc.ResolutionStatus;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.query.rpc.TableReferenceCandidate;
import ai.floedb.floecat.query.rpc.UserObjectsBundleChunk;
import ai.floedb.floecat.query.rpc.UserObjectsBundleEnd;
import ai.floedb.floecat.query.rpc.UserObjectsBundleHeader;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.MetadataResolutionContext;
import ai.floedb.floecat.scanner.spi.StatsProvider;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.concurrent.BoundedFanout;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.PinValidator;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.QueryPins;
import ai.floedb.floecat.service.query.ViewContextUtils;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.query.resolver.QueryInputResolver;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecorator;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecoratorProvider;
import ai.floedb.floecat.systemcatalog.spi.decorator.RelationDecoration;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import ai.floedb.floecat.types.Hashing;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class UserObjectBundleService {

  private static final int MAX_RESOLUTIONS_PER_CHUNK = 25;
  private static final Logger LOG = Logger.getLogger(UserObjectBundleService.class);

  private static void throwIfCancelled(BooleanSupplier cancelled) {
    if (cancelled.getAsBoolean()) {
      throw new CancellationException("GetUserObjects stream cancelled");
    }
  }

  private static final Set<String> LOCAL_FLIGHT_HOSTS = Set.of("localhost", "127.0.0.1", "0.0.0.0");

  private final CatalogOverlay overlay;
  private final QueryInputResolver inputResolver;
  private final QueryContextStore queryStore;
  private final EngineContextProvider engineContext;
  // Bumped when the engine decorator's behavior changes WITHOUT moving the engine version; folded
  // into the identity-only possession token so a decorator change invalidates cached decoration.
  private final String decorationEpoch;
  private final StatsProviderFactory statsFactory;
  private final long slowRpcMs;
  private final RelationBundleBuilder relationBuilder;

  @Inject Observability observability;

  // Caps how many of a chunk's relations resolve concurrently. Each is an independent, mostly
  // store-bound resolution; a small fan-out overlaps their round-trips without flooding the store.
  private static final int MAX_PARALLEL_RELATION_TASKS = 8;

  // The stream driver blocks while it gathers this fan-out. Keep its blocking metadata work off
  // the application worker pool so concurrent drivers cannot starve the executor that runs them.
  private final ExecutorService blockingExecutor = Executors.newVirtualThreadPerTaskExecutor();

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
    this.engineContext = engineContext;
    this.decorationEpoch = safe(decorationEpoch);
    this.slowRpcMs = Math.max(0L, slowRpcMs);
    FlightEndpointRef advertisedFlightEndpoint =
        FlightEndpointRef.newBuilder()
            .setHost(flightHost)
            .setPort(flightPort)
            .setTls(!grpcPlainText)
            .build();
    this.relationBuilder =
        new RelationBundleBuilder(
            overlay,
            decoratorProvider,
            engineSpecificEnabled,
            advertisedFlightEndpoint,
            pinValidator);
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
                  .onCancellation()
                  .invoke(iterator::cancel)
                  .onTermination()
                  .invoke(() -> iterator.publishStreamTelemetry("terminated"));
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
      ConcurrentMap<ResourceId, CompletableFuture<TablePin>> currentSnapshotPinCache,
      PhaseDiagnostics diagnostics,
      BooleanSupplier cancelled) {
    throwIfCancelled(cancelled);
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
            diagnostics,
            cancelled);
    diagnostics.nanos("pin.resolver", System.nanoTime() - resolverStartNs);
    throwIfCancelled(cancelled);
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

  static final class TimingAccumulator {
    private long statsLookupNanos;
    private long decorateRelationNanos;
    private long decorateViewNanos;
    private long decorateColumnsNanos;
    private long decorateColumnInvokeNanos;
    private long decorateCompleteNanos;
    private long decoratePersistRelationNanos;
    private long decoratePersistColumnsNanos;
    private long decorateColumnWarmHits;

    void addStatsLookupNanos(long nanos) {
      statsLookupNanos += nanos;
    }

    long statsLookupNanos() {
      return statsLookupNanos;
    }

    void addDecorateRelationNanos(long nanos) {
      decorateRelationNanos += nanos;
    }

    long decorateRelationNanos() {
      return decorateRelationNanos;
    }

    void addDecorateViewNanos(long nanos) {
      decorateViewNanos += nanos;
    }

    long decorateViewNanos() {
      return decorateViewNanos;
    }

    void addDecorateColumnsNanos(long nanos) {
      decorateColumnsNanos += nanos;
    }

    long decorateColumnsNanos() {
      return decorateColumnsNanos;
    }

    void addDecorateColumnInvokeNanos(long nanos) {
      decorateColumnInvokeNanos += nanos;
    }

    long decorateColumnInvokeNanos() {
      return decorateColumnInvokeNanos;
    }

    void addDecorateCompleteNanos(long nanos) {
      decorateCompleteNanos += nanos;
    }

    long decorateCompleteNanos() {
      return decorateCompleteNanos;
    }

    void addDecoratePersistRelationNanos(long nanos) {
      decoratePersistRelationNanos += nanos;
    }

    long decoratePersistRelationNanos() {
      return decoratePersistRelationNanos;
    }

    void addDecoratePersistColumnsNanos(long nanos) {
      decoratePersistColumnsNanos += nanos;
    }

    long decoratePersistColumnsNanos() {
      return decoratePersistColumnsNanos;
    }

    void addDecorateColumnWarmHits(long warmHits) {
      decorateColumnWarmHits += warmHits;
    }

    long decorateColumnWarmHits() {
      return decorateColumnWarmHits;
    }

    long decorationTotalNanos() {
      return decorateRelationNanos
          + decorateViewNanos
          + decorateColumnsNanos
          + decorateCompleteNanos;
    }

    /**
     * Add every total from {@code other} into this accumulator. Used to fold a build task's own
     * accumulator back into the request's on the driver thread once the task has joined.
     */
    void mergeFrom(TimingAccumulator other) {
      statsLookupNanos += other.statsLookupNanos;
      decorateRelationNanos += other.decorateRelationNanos;
      decorateViewNanos += other.decorateViewNanos;
      decorateColumnsNanos += other.decorateColumnsNanos;
      decorateColumnInvokeNanos += other.decorateColumnInvokeNanos;
      decorateCompleteNanos += other.decorateCompleteNanos;
      decoratePersistRelationNanos += other.decoratePersistRelationNanos;
      decoratePersistColumnsNanos += other.decoratePersistColumnsNanos;
      decorateColumnWarmHits += other.decorateColumnWarmHits;
    }
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
    // the client would match the token, get no endpoint, and route to the stale one. The endpoint
    // is
    // resolved through the builder's systemExecution — the same helper buildRelation uses to stamp
    // it — so the token cannot drift from the served routing.
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
      keyMaterial
          .append('\0')
          .append(relationBuilder.systemExecution(systemTableNode).tokenMaterial());
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
    boolean decorate = relationBuilder.decorationRequired(ctx);
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
    // The slim payload assembly (baseRelationInfo + attachTableStats + setPinIdentity, no columns)
    // lives in the builder; the driver keeps only the possession DECISION above. Its stats lookup
    // is timed into the passed accumulator there, exactly as the full build path times it.
    return relationBuilder.buildIdentityOnly(relation, scopedIdentity, statsProvider, timings);
  }

  List<ColumnResult> decorateColumns(
      List<ColumnInfo> columns,
      List<SchemaColumn> pruned,
      RelationDecoration relationDecoration,
      Optional<EngineMetadataDecorator> decorator,
      EngineContext ctx,
      boolean decorationRequired,
      ResourceId relationId) {
    return relationBuilder.decorateColumns(
        columns, pruned, relationDecoration, decorator, ctx, decorationRequired, relationId);
  }

  private static String safe(String value) {
    return value == null ? "" : value;
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

  record ResolvedRelation(
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
    // Per-request resolution memos. Both resolve each key at most once: a repeated lookup returns
    // the stored Optional (present or empty). See memoize() / resolveNameCached /
    // resolveNodeCached.
    private final Map<NormalizedNameRef, Optional<ResourceId>> nameResolutionCache =
        new ConcurrentHashMap<>();
    private final Map<ResourceId, Optional<GraphNode>> nodeResolutionCache =
        new ConcurrentHashMap<>();
    private final ArrayDeque<EagerBaseCursor> eagerBaseQueue = new ArrayDeque<>();
    private final Set<String> eagerBaseSeen = new HashSet<>();
    // Requested inputs selected for a chunk that filled before they could be emitted (a view ahead
    // of them expanded into enough base tables to reach the cap). Emitted, in order, ahead of newly
    // selected inputs in the next chunk — so a resolution's position never depends on chunk size.
    private final ArrayDeque<PendingItem> resolvedSpillover = new ArrayDeque<>();
    private final Map<RelationCacheKey, RelationInfo> relationInfoCache = new HashMap<>();
    private final ConcurrentMap<ResourceId, CompletableFuture<TablePin>> currentSnapshotPinCache =
        new ConcurrentHashMap<>();
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
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private boolean defaultCatalogResolved = false;
    private String defaultCatalogName = "";
    // Driver-thread wall-clock of the resolve stage (the parallel select fan-out), not a per-input
    // sum — under concurrency the sum would exceed the elapsed time and mislead.
    private long resolveNanos = 0L;
    private long normalizeNanos = 0L;
    private long defaultCatalogNanos = 0L;
    private long baseInjectNanos = 0L;
    private long pinCollectNanos = 0L;
    private long pinCommitNanos = 0L;
    private long relationBuildNanos = 0L;
    private long decorationNanos = 0L;
    private long defaultCatalogLookups = 0L;
    // Written from the parallel select tasks: LongAdder so the totals stay correct without locking.
    // These are aggregate sub-totals (total time/count across relations), not wall-clock.
    private final LongAdder selectRelationNanos = new LongAdder();
    private final LongAdder nameResolveNanos = new LongAdder();
    private final LongAdder nodeResolveNanos = new LongAdder();
    private final LongAdder nameResolutionCacheHits = new LongAdder();
    private final LongAdder nameResolutionCacheMisses = new LongAdder();
    private final LongAdder nodeResolutionCacheHits = new LongAdder();
    private final LongAdder nodeResolutionCacheMisses = new LongAdder();

    UserObjectBundleIterator(
        String correlationId,
        QueryContext ctx,
        List<TableReferenceCandidate> tables,
        Set<String> knownBlobVersions) {
      this.correlationId = correlationId;
      this.ctx = ctx;
      this.tables = tables;
      // Read-only for the life of the iterator (consulted by the identity-only fast path); copy so
      // that stays true regardless of what the caller does with its set afterwards.
      this.knownBlobVersions = Set.copyOf(knownBlobVersions);
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
      throwIfCancelled(this::isCancelled);
      if (!headerEmitted) {
        headerEmitted = true;
        if (LOG.isDebugEnabled()) {
          LOG.debugf("Emitting header chunk query_id=%s seq=%d", ctx.getQueryId(), seq);
        }
        return headerChunk(ctx.getQueryId(), seq++);
      }

      if (pending.isEmpty()
          && (nextInputIndex < resolutionCount
              || !eagerBaseQueue.isEmpty()
              || !resolvedSpillover.isEmpty())) {
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
      // Carry-over first, in emit order: a prior view's undrained base tables, then requested
      // inputs a prior chunk selected but could not fit.
      drainEagerBaseTables(toPin);
      while (!resolvedSpillover.isEmpty() && pending.size() < MAX_RESOLUTIONS_PER_CHUNK) {
        gather(resolvedSpillover.removeFirst(), toPin);
      }
      // Then newly requested inputs, up to this chunk's remaining capacity. Selection failures are
      // kept as ordered outcomes: if a view ahead fills this chunk, a later failure waits in
      // spillover and is not allowed to suppress the view and its eager base tables.
      int budget = MAX_RESOLUTIONS_PER_CHUNK - pending.size();
      if (budget > 0 && nextInputIndex < resolutionCount) {
        int planCount = Math.min(budget, resolutionCount - nextInputIndex);
        List<PlannedInput> plan = new ArrayList<>(planCount);
        for (int i = 0; i < planCount; i++) {
          int inputIndex = nextInputIndex + i;
          try {
            throwIfCancelled(this::isCancelled);
            plan.add(planInput(inputIndex));
          } catch (CancellationException e) {
            throw e;
          } catch (RuntimeException e) {
            plan.add(PlannedInput.failed(inputIndex, e));
          }
        }
        nextInputIndex += planCount;
        // Resolve and consume planned inputs concurrently in request order. Consuming each result
        // as it is joined gives an earlier deferred failure precedence over a later task failure,
        // while still letting an eager view fill this chunk and spill later requested inputs.
        long selectStageStartNs = System.nanoTime();
        try {
          BoundedFanout.forEachOrdered(
              plan,
              MAX_PARALLEL_RELATION_TASKS,
              blockingExecutor,
              this::selectOne,
              item -> {
                // A view gathered earlier in this loop can fill the chunk via its base tables;
                // the remaining already-selected inputs wait for the next chunk (their nodes are
                // cached).
                if (pending.size() >= MAX_RESOLUTIONS_PER_CHUNK) {
                  resolvedSpillover.addLast(item);
                } else {
                  gather(item, toPin);
                }
              },
              this::isCancelled);
        } finally {
          resolveNanos += System.nanoTime() - selectStageStartNs;
        }
      }
      if (!toPin.isEmpty()) {
        long pinStartNs = System.nanoTime();
        try {
          RelationPinSet chunkPins =
              collectChunkPins(
                  correlationId,
                  ctx,
                  toPin,
                  currentSnapshotPinCache,
                  diagnostics,
                  this::isCancelled);
          throwIfCancelled(this::isCancelled);
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

    private boolean isCancelled() {
      return cancelled.get();
    }

    private void cancel() {
      cancelled.set(true);
      publishStreamTelemetry("cancelled");
    }

    /**
     * Append one selected resolution to the chunk on the driver thread: tally its found/not-found
     * count, queue a FOUND table for pinning, and — for a FOUND view — drain its base tables right
     * after it so bases follow their view in the emitted order. ERROR resolutions count toward
     * neither found nor not-found, matching the end-chunk contract.
     */
    private void gather(PendingItem item, List<ResolvedRelation> toPin) {
      if (item instanceof PendingFailure failure) {
        throw failure.failure();
      }
      pending.add(item);
      if (item instanceof PendingFound found) {
        foundCount++;
        toPin.add(found.relation());
        if (found.relation().node() instanceof ViewNode view && !view.baseRelations().isEmpty()) {
          eagerBaseQueue.addLast(new EagerBaseCursor(view));
          drainEagerBaseTables(toPin);
        }
      } else if (item instanceof PendingResolved resolved
          && resolved.resolution().getStatus() == ResolutionStatus.RESOLUTION_STATUS_NOT_FOUND) {
        notFoundCount++;
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

    /** A requested input paired with its normalized candidates, ready to select against. */
    private record PlannedInput(
        int inputIndex,
        TableReferenceCandidate candidate,
        List<QueryInput> normalized,
        RuntimeException planningFailure) {
      static PlannedInput failed(int inputIndex, RuntimeException failure) {
        return new PlannedInput(inputIndex, null, List.of(), failure);
      }

      boolean failed() {
        return planningFailure != null;
      }
    }

    /** Normalize one requested input's candidates. Pure with respect to resolution state. */
    private PlannedInput planInput(int inputIndex) {
      TableReferenceCandidate candidate = tables.get(inputIndex);
      if (LOG.isTraceEnabled()) {
        LOG.tracef(
            "Planning candidate query_id=%s input_index=%d candidate_count=%d",
            ctx.getQueryId(), inputIndex, candidate.getCandidatesCount());
      }
      long normalizeStartNs = System.nanoTime();
      try {
        List<QueryInput> normalized =
            normalizeCandidates(correlationId, candidate, this::defaultCatalogName);
        return new PlannedInput(inputIndex, candidate, normalized, null);
      } finally {
        normalizeNanos += System.nanoTime() - normalizeStartNs;
      }
    }

    /**
     * Resolve one planned input to a {@link PendingItem} (FOUND, NOT_FOUND, or ERROR), without
     * touching found/not-found counters or chunk order — the driver's {@link #gather} owns those.
     */
    private PendingItem selectOne(PlannedInput planned) {
      int inputIndex = planned.inputIndex();
      if (planned.failed()) {
        return new PendingFailure(inputIndex, planned.planningFailure());
      }
      try {
        throwIfCancelled(this::isCancelled);
        long selectStartNs = System.nanoTime();
        Optional<ResolvedRelation> resolved;
        try {
          resolved =
              selectResolvedRelation(
                  correlationId,
                  planned.candidate(),
                  planned.normalized(),
                  this::resolveNameCached,
                  this::resolveNodeCached);
        } finally {
          selectRelationNanos.add(System.nanoTime() - selectStartNs);
        }
        if (resolved.isPresent()) {
          if (LOG.isTraceEnabled()) {
            LOG.tracef(
                "Resolved candidate query_id=%s input_index=%d relation=%s",
                ctx.getQueryId(), inputIndex, resolved.get().relationId());
          }
          return new PendingFound(inputIndex, resolved.get());
        }
      } catch (CancellationException e) {
        throw e;
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
                .addAllAttempted(planned.normalized())
                .build();
        return new PendingResolved(
            RelationResolution.newBuilder()
                .setInputIndex(inputIndex)
                .setStatus(ResolutionStatus.RESOLUTION_STATUS_ERROR)
                .setFailure(failure)
                .build());
      } catch (RuntimeException e) {
        return new PendingFailure(inputIndex, e);
      }
      if (LOG.isTraceEnabled()) {
        LOG.tracef(
            "Candidate not found query_id=%s input_index=%d attempted=%d",
            ctx.getQueryId(), inputIndex, planned.normalized().size());
      }
      ResolutionFailure failure =
          ResolutionFailure.newBuilder()
              .setCode("catalog_bundle.relation_not_found")
              .setMessage("relation not found")
              .putDetails("candidate_count", Integer.toString(planned.normalized().size()))
              .putDetails("default_catalog", defaultCatalogForDiagnostics())
              .addAllAttempted(planned.normalized())
              .build();
      return new PendingResolved(
          RelationResolution.newBuilder()
              .setInputIndex(inputIndex)
              .setStatus(ResolutionStatus.RESOLUTION_STATUS_NOT_FOUND)
              .setFailure(failure)
              .build());
    }

    /**
     * Warm the pinned table stats for this chunk's FOUND tables in one batched, parallel read after
     * the pin barrier, so each relation's build reads its stats from the provider cache instead of
     * a serial store round-trip (which can block on the sync-capture budget). Views carry no table
     * stats and are skipped. Best-effort: a failure here is swallowed and the per-relation lookup
     * during build resolves stats as before.
     */
    private void warmChunkStats(List<PendingItem> chunkItems) {
      List<ResourceId> tableIds = new ArrayList<>(chunkItems.size());
      for (PendingItem item : chunkItems) {
        if (item instanceof PendingFound found
            && found.relation().node().kind() == GraphNodeKind.TABLE) {
          tableIds.add(found.relation().relationId());
        }
      }
      if (tableIds.isEmpty()) {
        return;
      }
      long startNs = System.nanoTime();
      try {
        statsProvider.tableStatsBatch(tableIds);
      } catch (RuntimeException e) {
        LOG.debugf(
            e,
            "stats batch warm failed query_id=%s; build will resolve stats per relation",
            ctx.getQueryId());
      } finally {
        // The reads happen here; the per-relation tableStats during build then hits the cache.
        timings.addStatsLookupNanos(System.nanoTime() - startNs);
      }
    }

    /** A built relation's outcome: exactly one of {@code info} / {@code error} is non-null. */
    private record BuildOutcome(
        PendingFound source,
        RelationInfo info,
        RelationResolution error,
        long relationBuildNanos,
        long decorationNanos,
        TimingAccumulator taskTimings) {}

    /**
     * Build one relation's full payload, timing into a task-local accumulator so parallel builds
     * need no shared-counter deltas. A build failure is isolated to this relation as an ERROR
     * resolution — one relation's decoration/schema/stats fault must not sink the whole bundle.
     */
    private BuildOutcome buildOne(
        PendingFound found, QueryContext liveCtx, Optional<RelationPinIdentity> scopedIdentity) {
      long buildStartNs = System.nanoTime();
      RelationBundleBuilder.BuildResult result =
          relationBuilder.build(
              correlationId,
              found.relation(),
              liveCtx,
              resolutionContext,
              statsProvider,
              scopedIdentity);
      long buildNanos = System.nanoTime() - buildStartNs;
      TimingAccumulator taskTimings = result.timings();
      if (result.isSuccess()) {
        long relationBuild =
            Math.max(
                0L,
                buildNanos - taskTimings.statsLookupNanos() - taskTimings.decorationTotalNanos());
        long decoration = Math.max(0L, taskTimings.decorationTotalNanos());
        return new BuildOutcome(found, result.info(), null, relationBuild, decoration, taskTimings);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debugf(
            "relation build failed query_id=%s input_index=%d resource_id=%s",
            ctx.getQueryId(), found.inputIndex(), found.relation().relationId().getId());
      }
      return new BuildOutcome(
          found, null, buildErrorResolution(found, result.error()), 0L, 0L, taskTimings);
    }

    private RelationResolution buildErrorResolution(
        PendingFound found, RelationBundleBuilder.BuildError error) {
      ResolutionFailure failure =
          ResolutionFailure.newBuilder()
              .setCode(error.code())
              .setMessage(error.message())
              .putDetails("resource_id", error.resourceId())
              .build();
      return RelationResolution.newBuilder()
          .setInputIndex(found.inputIndex())
          .setStatus(ResolutionStatus.RESOLUTION_STATUS_ERROR)
          .setFailure(failure)
          .build();
    }

    private static RelationResolution foundResolution(int inputIndex, RelationInfo relation) {
      return RelationResolution.newBuilder()
          .setInputIndex(inputIndex)
          .setStatus(ResolutionStatus.RESOLUTION_STATUS_FOUND)
          .setRelation(relation)
          .build();
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
      warmChunkStats(chunkItems);
      QueryContext liveCtx = queryStore.get(ctx.getQueryId()).orElse(ctx);

      // Driver pre-pass: everything cheap and order/state-sensitive stays here — passthrough
      // resolutions, cache hits, and the identity-only fast path (which reads the shared
      // knownBlobVersions and timings). Relations needing a full build are collected for the
      // parallel stage; their pin identity, computed here for the slim check, is carried forward so
      // buildOne does not recompute it. slots keeps every resolution in chunk order.
      RelationResolution[] slots = new RelationResolution[chunkItems.size()];
      List<PendingFound> toBuild = new ArrayList<>();
      List<Integer> buildSlots = new ArrayList<>();
      List<Optional<RelationPinIdentity>> buildIdentities = new ArrayList<>();
      for (int i = 0; i < chunkItems.size(); i++) {
        PendingItem item = chunkItems.get(i);
        if (item instanceof PendingResolved resolved) {
          slots[i] = resolved.resolution();
          continue;
        }
        PendingFound found = (PendingFound) item;
        RelationInfo cachedInfo = relationInfoCache.get(relationCacheKey(found.relation()));
        if (cachedInfo != null) {
          slots[i] = foundResolution(found.inputIndex(), cachedInfo);
          continue;
        }
        long statsBeforeNanos = timings.statsLookupNanos();
        long buildStartNs = System.nanoTime();
        // Compute the pin identity at most once per relation: the identity-only match consults it
        // when the client sent hints, and the full-build stamp reuses it — so a cache miss under a
        // populated hint set does not hash the relation twice. Computed for EVERY pinned relation
        // (not only full-schema ones): the stamp preserves the data identity even on a projected
        // reply, merely blanking the possession token there.
        Optional<RelationPinIdentity> scopedIdentity =
            scopedPinIdentity(
                correlationId, found.relation(), liveCtx, resolutionContext.engineContext());
        // Identity-only fast path: never cached — the info cache must only ever hold full payloads,
        // or a later request that did NOT prove possession would be served a payload-less relation.
        RelationInfo slim =
            identityOnlyOrNull(
                found.relation(), scopedIdentity, statsProvider, knownBlobVersions, timings);
        if (slim != null) {
          // Its stats time already landed in timings via identityOnlyOrNull; fold the remaining
          // (identity-build) time into relationBuildNanos so slim replies are not invisible.
          long buildNanos = System.nanoTime() - buildStartNs;
          long statsDeltaNanos = timings.statsLookupNanos() - statsBeforeNanos;
          relationBuildNanos += Math.max(0L, buildNanos - statsDeltaNanos);
          slots[i] = foundResolution(found.inputIndex(), slim);
          continue;
        }
        toBuild.add(found);
        buildSlots.add(i);
        buildIdentities.add(scopedIdentity);
      }

      // Build the remaining relations concurrently; each task times itself into its own
      // accumulator so the summary math needs no shared-counter deltas.
      List<Integer> indices = java.util.stream.IntStream.range(0, toBuild.size()).boxed().toList();
      List<BuildOutcome> outcomes =
          BoundedFanout.mapOrdered(
              indices,
              MAX_PARALLEL_RELATION_TASKS,
              blockingExecutor,
              j -> buildOne(toBuild.get(j), liveCtx, buildIdentities.get(j)));

      // Driver gather: fold each task's timings in, cache full payloads, and place resolutions in
      // chunk order. A build that failed becomes an ERROR for that one relation (it was counted
      // FOUND at selection, so undo that — an ERROR counts toward neither found nor not_found).
      for (int j = 0; j < outcomes.size(); j++) {
        BuildOutcome outcome = outcomes.get(j);
        timings.mergeFrom(outcome.taskTimings());
        relationBuildNanos += outcome.relationBuildNanos();
        decorationNanos += outcome.decorationNanos();
        PendingFound found = outcome.source();
        if (outcome.info() != null) {
          relationInfoCache.put(relationCacheKey(found.relation()), outcome.info());
          slots[buildSlots.get(j)] = foundResolution(found.inputIndex(), outcome.info());
        } else {
          foundCount--;
          slots[buildSlots.get(j)] = outcome.error();
        }
      }

      List<RelationResolution> resolutions = List.of(slots);
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
      diagnostics.nanos("select_relation", selectRelationNanos.sum());
      diagnostics.nanos("default_catalog", defaultCatalogNanos);
      diagnostics.nanos("name_resolve", nameResolveNanos.sum());
      diagnostics.nanos("node_resolve", nodeResolveNanos.sum());
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
      diagnostics.put("name_cache_hits", nameResolutionCacheHits.sum());
      diagnostics.put("name_cache_misses", nameResolutionCacheMisses.sum());
      diagnostics.put("node_cache_hits", nodeResolutionCacheHits.sum());
      diagnostics.put("node_cache_misses", nodeResolutionCacheMisses.sum());
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
      // Engine captured at iterator construction is passed through: re-reading it from the request
      // context per lookup is fragile across executor hops, and an empty engine silently
      // un-resolves engine-gated system objects (eng-floe/floecat#361).
      Memoized<ResourceId> m =
          memoize(
              nameResolutionCache,
              normalizedNameRef(ref),
              () -> overlay.resolveName(correlationId, ref, resolutionContext.engineContext()),
              nameResolveNanos::add);
      if (m.resolved()) {
        nameResolutionCacheMisses.increment();
      } else {
        nameResolutionCacheHits.increment();
      }
      return m.value();
    }

    private Optional<GraphNode> resolveNodeCached(ResourceId id) {
      Memoized<GraphNode> m =
          memoize(
              nodeResolutionCache,
              id,
              () -> overlay.resolve(id, resolutionContext.engineContext()),
              nodeResolveNanos::add);
      if (m.resolved()) {
        nodeResolutionCacheMisses.increment();
      } else {
        nodeResolutionCacheHits.increment();
      }
      return m.value();
    }

    /**
     * A memoized value together with whether this call resolved it (a miss) vs. found it cached.
     */
    private record Memoized<V>(Optional<V> value, boolean resolved) {}

    /**
     * Return {@code cache.get(key)}, resolving and storing it once if absent. The resolve runs at
     * most once per key even under concurrent callers (computeIfAbsent single-flight); its elapsed
     * time is reported to {@code addNanos}. {@link Memoized#resolved()} is true when this call ran
     * the resolve.
     */
    private <K, V> Memoized<V> memoize(
        Map<K, Optional<V>> cache, K key, Supplier<Optional<V>> resolve, LongConsumer addNanos) {
      boolean[] resolvedHere = {false};
      Optional<V> value =
          cache.computeIfAbsent(
              key,
              k -> {
                resolvedHere[0] = true;
                long startNs = System.nanoTime();
                try {
                  return resolve.get();
                } finally {
                  addNanos.accept(System.nanoTime() - startNs);
                }
              });
      return new Memoized<>(value, resolvedHere[0]);
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

    /** A planning or selection exception deferred until its request-order position is emitted. */
    private static final class PendingFailure implements PendingItem {
      private final int inputIndex;
      private final RuntimeException failure;

      private PendingFailure(int inputIndex, RuntimeException failure) {
        this.inputIndex = inputIndex;
        this.failure = failure;
      }

      @Override
      public int inputIndex() {
        return inputIndex;
      }

      RuntimeException failure() {
        return failure;
      }
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
