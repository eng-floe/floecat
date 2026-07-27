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
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.FlightEndpointRef;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.RelationPinIdentity;
import ai.floedb.floecat.query.rpc.RelationResolution;
import ai.floedb.floecat.query.rpc.ResolutionFailure;
import ai.floedb.floecat.query.rpc.ResolutionStatus;
import ai.floedb.floecat.query.rpc.TableReferenceCandidate;
import ai.floedb.floecat.query.rpc.UserObjectsBundleChunk;
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
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecoratorProvider;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
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
  // Mints the pin identity/possession token and serves the identity-only decision. Stateless per
  // call; reused on the driver thread across every chunk.
  private final PossessionGate possessionGate;

  @Inject Observability observability;

  // Caps how many of a chunk's relations resolve concurrently, across the select and build
  // fan-outs. Each is an independent, mostly store-bound resolution; a small fan-out overlaps their
  // round-trips without flooding the store. Per-request bound only: the virtual-thread executor has
  // no shared-pool ceiling, so total store concurrency scales with concurrent requests times this
  // value (upstream gRPC concurrency bounds the request count).
  private final int maxParallelRelations;

  // Runs per-relation resolution off the request thread on virtual threads, deliberately NOT the
  // shared Quarkus worker pool: the stream driver runs on that pool and blocks joining this
  // fan-out, so submitting the fan-out back to the same pool can starve it under load (all workers
  // parked on joins, none left to run the joined tasks). Virtual threads park cheaply while blocked
  // on the store and BoundedFanout's semaphore bounds real concurrency; OTel context is
  // re-established per task inside BoundedFanout.
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
      @ConfigProperty(name = "floecat.rpc.log.slow-ms", defaultValue = "250") long slowRpcMs,
      @ConfigProperty(name = "floecat.catalog.bundle.max_parallel_relations", defaultValue = "8")
          int maxParallelRelations) {
    this.overlay = overlay;
    this.inputResolver = inputResolver;
    this.queryStore = queryStore;
    this.statsFactory = statsFactory;
    this.engineContext = engineContext;
    this.decorationEpoch = safe(decorationEpoch);
    this.slowRpcMs = Math.max(0L, slowRpcMs);
    // A permit count of 0 or negative would wedge every GetUserObjects request forever in the
    // fan-out's permit wait, so reject it at startup: clamp to serial and warn rather than serve
    // in a permanently-hanging state.
    if (maxParallelRelations < 1) {
      LOG.warnf(
          "floecat.catalog.bundle.max_parallel_relations=%d is invalid; clamping to 1 (serial)",
          maxParallelRelations);
    }
    this.maxParallelRelations = Math.max(1, maxParallelRelations);
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
    this.possessionGate = new PossessionGate(relationBuilder, this.decorationEpoch);
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
        250L,
        8);
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
                  .onCancellation()
                  .invoke(iterator::markCancelled)
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
      Function<ResourceId, Optional<GraphNode>> nodeResolver,
      Function<RelationNode, NameRef> canonicalNameResolver) {
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
      return Optional.of(
          new ResolvedRelation(
              candidate, relationId, rel, input, canonicalNameResolver.apply(rel)));
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

  /**
   * The single in-flight telemetry tally for one GetUserObjects request: every phase timer plus
   * every found/not-found and cache counter. The request-level instance lives on the driver; each
   * parallel build task keeps its own instance (decoration/stats sub-phases) that the driver folds
   * back in via {@link #mergeFrom} once the task has joined. Every slot is a {@link LongAdder} so
   * the concurrent select-stage updates, the driver-stage updates, and the per-task merges are all
   * lock-free and thread-safe. Cache-entry sizes are not held here; they are read live at emit.
   */
  static final class TimingAccumulator {
    // Decoration / stats sub-phases, accumulated by each build task then merged on the driver.
    private final LongAdder statsLookupNanos = new LongAdder();
    private final LongAdder decorateRelationNanos = new LongAdder();
    private final LongAdder decorateViewNanos = new LongAdder();
    private final LongAdder decorateColumnsNanos = new LongAdder();
    private final LongAdder decorateColumnInvokeNanos = new LongAdder();
    private final LongAdder decorateCompleteNanos = new LongAdder();
    private final LongAdder decoratePersistRelationNanos = new LongAdder();
    private final LongAdder decoratePersistColumnsNanos = new LongAdder();
    private final LongAdder decorateColumnWarmHits = new LongAdder();
    // Driver-stage wall-clock phase timers (single-thread writers on the driver).
    private final LongAdder resolveNanos = new LongAdder();
    private final LongAdder normalizeNanos = new LongAdder();
    private final LongAdder defaultCatalogNanos = new LongAdder();
    private final LongAdder baseInjectNanos = new LongAdder();
    private final LongAdder pinCollectNanos = new LongAdder();
    private final LongAdder pinCommitNanos = new LongAdder();
    private final LongAdder relationBuildNanos = new LongAdder();
    private final LongAdder decorationNanos = new LongAdder();
    // Driver-only: wall-clock of the chunk's batch stats WARM pass. Kept distinct from
    // statsLookupNanos so the one-shot warm fetch is not double-counted against the per-relation
    // stats reads it turns into cache hits during build.
    private final LongAdder statsWarmNanos = new LongAdder();
    // Aggregate sub-totals written from the parallel select tasks.
    private final LongAdder selectRelationNanos = new LongAdder();
    private final LongAdder nameResolveNanos = new LongAdder();
    private final LongAdder nodeResolveNanos = new LongAdder();
    // Found/not-found and per-request cache counters.
    private final LongAdder foundCount = new LongAdder();
    private final LongAdder notFoundCount = new LongAdder();
    private final LongAdder defaultCatalogLookups = new LongAdder();
    private final LongAdder nameResolutionCacheHits = new LongAdder();
    private final LongAdder nameResolutionCacheMisses = new LongAdder();
    private final LongAdder nodeResolutionCacheHits = new LongAdder();
    private final LongAdder nodeResolutionCacheMisses = new LongAdder();

    void addStatsLookupNanos(long nanos) {
      statsLookupNanos.add(nanos);
    }

    void addStatsWarmNanos(long nanos) {
      statsWarmNanos.add(nanos);
    }

    long statsLookupNanos() {
      return statsLookupNanos.sum();
    }

    void addDecorateRelationNanos(long nanos) {
      decorateRelationNanos.add(nanos);
    }

    void addDecorateViewNanos(long nanos) {
      decorateViewNanos.add(nanos);
    }

    void addDecorateColumnsNanos(long nanos) {
      decorateColumnsNanos.add(nanos);
    }

    void addDecorateColumnInvokeNanos(long nanos) {
      decorateColumnInvokeNanos.add(nanos);
    }

    void addDecorateCompleteNanos(long nanos) {
      decorateCompleteNanos.add(nanos);
    }

    void addDecoratePersistRelationNanos(long nanos) {
      decoratePersistRelationNanos.add(nanos);
    }

    void addDecoratePersistColumnsNanos(long nanos) {
      decoratePersistColumnsNanos.add(nanos);
    }

    void addDecorateColumnWarmHits(long warmHits) {
      decorateColumnWarmHits.add(warmHits);
    }

    long decorationTotalNanos() {
      return decorateRelationNanos.sum()
          + decorateViewNanos.sum()
          + decorateColumnsNanos.sum()
          + decorateCompleteNanos.sum();
    }

    void addResolveNanos(long nanos) {
      resolveNanos.add(nanos);
    }

    void addNormalizeNanos(long nanos) {
      normalizeNanos.add(nanos);
    }

    void addDefaultCatalogNanos(long nanos) {
      defaultCatalogNanos.add(nanos);
    }

    void addBaseInjectNanos(long nanos) {
      baseInjectNanos.add(nanos);
    }

    void addPinCollectNanos(long nanos) {
      pinCollectNanos.add(nanos);
    }

    void addPinCommitNanos(long nanos) {
      pinCommitNanos.add(nanos);
    }

    void addRelationBuildNanos(long nanos) {
      relationBuildNanos.add(nanos);
    }

    void addDecorationNanos(long nanos) {
      decorationNanos.add(nanos);
    }

    void addSelectRelationNanos(long nanos) {
      selectRelationNanos.add(nanos);
    }

    void addNameResolveNanos(long nanos) {
      nameResolveNanos.add(nanos);
    }

    void addNodeResolveNanos(long nanos) {
      nodeResolveNanos.add(nanos);
    }

    void recordFound() {
      foundCount.increment();
    }

    /** Undo a FOUND count: a relation counted FOUND at selection later built into an ERROR. */
    void unrecordFound() {
      foundCount.decrement();
    }

    int found() {
      return (int) foundCount.sum();
    }

    void recordNotFound() {
      notFoundCount.increment();
    }

    int notFound() {
      return (int) notFoundCount.sum();
    }

    void recordDefaultCatalogLookup() {
      defaultCatalogLookups.increment();
    }

    void recordNameCacheHit() {
      nameResolutionCacheHits.increment();
    }

    void recordNameCacheMiss() {
      nameResolutionCacheMisses.increment();
    }

    void recordNodeCacheHit() {
      nodeResolutionCacheHits.increment();
    }

    void recordNodeCacheMiss() {
      nodeResolutionCacheMisses.increment();
    }

    long resolveNanos() {
      return resolveNanos.sum();
    }

    long baseInjectNanos() {
      return baseInjectNanos.sum();
    }

    long relationBuildNanos() {
      return relationBuildNanos.sum();
    }

    long decorationNanos() {
      return decorationNanos.sum();
    }

    /** Sum of the two pin sub-phases; the {@code pin_ms} derived metric. */
    long pinNanos() {
      return pinCollectNanos.sum() + pinCommitNanos.sum();
    }

    /**
     * Scheduling time: the request wall-clock left over once every measured phase is subtracted.
     * Never negative. Keep this arithmetic in one place. stats_warm is subtracted alongside
     * stats_lookup: the warm pass is its own wall-clock interval, so leaving it in would count it
     * twice -- once as {@code stats_warm_ms} and again in the residual. (Pre-consolidation the warm
     * time lived inside stats_lookup and was already excluded here; splitting it out must not put
     * it back into the residual.)
     */
    long schedulingNanos(long totalNanos) {
      return Math.max(
          0L,
          totalNanos
              - resolveNanos.sum()
              - baseInjectNanos.sum()
              - pinCollectNanos.sum()
              - pinCommitNanos.sum()
              - relationBuildNanos.sum()
              - decorationNanos.sum()
              - statsLookupNanos.sum()
              - statsWarmNanos.sum());
    }

    /**
     * Add every total from {@code other} into this accumulator. Used to fold a build task's own
     * accumulator back into the request's on the driver thread once the task has joined.
     */
    void mergeFrom(TimingAccumulator other) {
      statsLookupNanos.add(other.statsLookupNanos.sum());
      decorateRelationNanos.add(other.decorateRelationNanos.sum());
      decorateViewNanos.add(other.decorateViewNanos.sum());
      decorateColumnsNanos.add(other.decorateColumnsNanos.sum());
      decorateColumnInvokeNanos.add(other.decorateColumnInvokeNanos.sum());
      decorateCompleteNanos.add(other.decorateCompleteNanos.sum());
      decoratePersistRelationNanos.add(other.decoratePersistRelationNanos.sum());
      decoratePersistColumnsNanos.add(other.decoratePersistColumnsNanos.sum());
      decorateColumnWarmHits.add(other.decorateColumnWarmHits.sum());
      resolveNanos.add(other.resolveNanos.sum());
      normalizeNanos.add(other.normalizeNanos.sum());
      defaultCatalogNanos.add(other.defaultCatalogNanos.sum());
      baseInjectNanos.add(other.baseInjectNanos.sum());
      pinCollectNanos.add(other.pinCollectNanos.sum());
      pinCommitNanos.add(other.pinCommitNanos.sum());
      relationBuildNanos.add(other.relationBuildNanos.sum());
      decorationNanos.add(other.decorationNanos.sum());
      statsWarmNanos.add(other.statsWarmNanos.sum());
      selectRelationNanos.add(other.selectRelationNanos.sum());
      nameResolveNanos.add(other.nameResolveNanos.sum());
      nodeResolveNanos.add(other.nodeResolveNanos.sum());
      foundCount.add(other.foundCount.sum());
      notFoundCount.add(other.notFoundCount.sum());
      defaultCatalogLookups.add(other.defaultCatalogLookups.sum());
      nameResolutionCacheHits.add(other.nameResolutionCacheHits.sum());
      nameResolutionCacheMisses.add(other.nameResolutionCacheMisses.sum());
      nodeResolutionCacheHits.add(other.nodeResolutionCacheHits.sum());
      nodeResolutionCacheMisses.add(other.nodeResolutionCacheMisses.sum());
    }

    /**
     * Write every summary metric onto {@code diagnostics} and emit the summary event. The request
     * context ({@link SummaryContext}) carries the non-tally values (ids, chunk/candidate counts,
     * live cache sizes, outcome) and the three derived durations. Every key and its write verb
     * ({@code nanos} vs {@code put}) matches the docs/telemetry/diagnostics.md contract.
     */
    void flushInto(PhaseDiagnostics diagnostics, SummaryContext ctx) {
      diagnostics.put("query_id", ctx.queryId());
      diagnostics.put("correlation_id", ctx.correlationId());
      diagnostics.put("candidates", ctx.candidates());
      diagnostics.put("chunks", ctx.chunks());
      diagnostics.put("found", found());
      diagnostics.put("not_found", notFound());
      diagnostics.put("total_ms", ctx.totalMs());
      diagnostics.nanos("resolve", resolveNanos.sum());
      diagnostics.nanos("normalize", normalizeNanos.sum());
      diagnostics.nanos("select_relation", selectRelationNanos.sum());
      diagnostics.nanos("default_catalog", defaultCatalogNanos.sum());
      diagnostics.nanos("name_resolve", nameResolveNanos.sum());
      diagnostics.nanos("node_resolve", nodeResolveNanos.sum());
      diagnostics.nanos("base_inject", baseInjectNanos.sum());
      diagnostics.nanos("pin_collect", pinCollectNanos.sum());
      diagnostics.nanos("pin_commit", pinCommitNanos.sum());
      diagnostics.put("pin_ms", ctx.pinMs());
      diagnostics.nanos("relation_build", relationBuildNanos.sum());
      diagnostics.nanos("decoration", decorationNanos.sum());
      diagnostics.nanos("stats_lookup", statsLookupNanos.sum());
      diagnostics.nanos("stats_warm", statsWarmNanos.sum());
      diagnostics.nanos("decorate_relation", decorateRelationNanos.sum());
      diagnostics.nanos("decorate_view", decorateViewNanos.sum());
      diagnostics.nanos("decorate_columns", decorateColumnsNanos.sum());
      diagnostics.nanos("decorate_column_invoke", decorateColumnInvokeNanos.sum());
      diagnostics.nanos("decorate_complete", decorateCompleteNanos.sum());
      diagnostics.put("scheduling_ms", ctx.schedulingMs());
      diagnostics.put("decorator_warm_hits", decorateColumnWarmHits.sum());
      diagnostics.nanos(
          "hint_persist", decoratePersistRelationNanos.sum() + decoratePersistColumnsNanos.sum());
      diagnostics.put("default_catalog_lookups", defaultCatalogLookups.sum());
      diagnostics.put("name_cache_hits", nameResolutionCacheHits.sum());
      diagnostics.put("name_cache_misses", nameResolutionCacheMisses.sum());
      diagnostics.put("node_cache_hits", nodeResolutionCacheHits.sum());
      diagnostics.put("node_cache_misses", nodeResolutionCacheMisses.sum());
      diagnostics.put("name_cache_entries", ctx.nameCacheEntries());
      diagnostics.put("node_cache_entries", ctx.nodeCacheEntries());
      diagnostics.put("relation_cache_entries", ctx.relationCacheEntries());
      diagnostics.put("outcome", ctx.outcome());
      diagnostics.emit("floecat.get_user_objects.summary");
    }
  }

  /**
   * The non-tally context a {@link TimingAccumulator#flushInto} needs: request ids, candidate/chunk
   * counts, the three derived durations, live cache-entry sizes, and the outcome label.
   */
  record SummaryContext(
      String queryId,
      String correlationId,
      int candidates,
      int chunks,
      double totalMs,
      double pinMs,
      double schedulingMs,
      int nameCacheEntries,
      int nodeCacheEntries,
      int relationCacheEntries,
      String outcome) {}

  private static String safe(String value) {
    return value == null ? "" : value;
  }

  record ResolvedRelation(
      TableReferenceCandidate candidate,
      ResourceId relationId,
      RelationNode node,
      QueryInput selectedInput,
      // Resolved once here (through the request node memo) so the concurrent build fan-out does not
      // each re-walk the shared namespace/catalog; see RelationResolutionCache#canonicalName.
      NameRef canonicalName) {}

  /** A requested input paired with its normalized candidates, ready to select against. */
  record PlannedInput(
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

  private record RelationCacheKey(
      ResourceId relationId,
      boolean wantsAllColumns,
      List<String> initialColumns,
      String engineKind,
      String engineVersion,
      SnapshotRef snapshotOverride) {}

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
     * one of these get an identity-only response (see PossessionGate#identityOnly). */
    private final Set<String> knownBlobVersions;

    // Maintains the order inputs were resolved so the emitted chunk mirrors the request order.
    private final List<PendingItem> pending = new ArrayList<>(MAX_RESOLUTIONS_PER_CHUNK);
    // Per-request name/node resolution memo, shared (thread-safe) across the concurrent select
    // stage; records its hit/miss and resolve-nanos into the request tally below.
    private final RelationResolutionCache resolutionCache;
    private final ArrayDeque<EagerBaseCursor> eagerBaseQueue = new ArrayDeque<>();
    private final Set<String> eagerBaseSeen = new HashSet<>();
    // Requested inputs selected for a chunk that filled before they could be emitted (a view ahead
    // of them expanded into enough base tables to reach the cap). Emitted, in order, ahead of newly
    // selected inputs in the next chunk — so a resolution's position never depends on chunk size.
    private final ArrayDeque<PendingItem> resolvedSpillover = new ArrayDeque<>();
    // Read via size() from the Mutiny termination/failure callback (transport thread) while the
    // driver may still be put()-ing mid-build on a cancelled stream; ConcurrentHashMap makes that
    // concurrent size() well-defined, so a cancelled stream reports partial-but-not-torn telemetry.
    private final Map<RelationCacheKey, RelationInfo> relationInfoCache = new ConcurrentHashMap<>();
    private final TimingAccumulator timings = new TimingAccumulator();
    private final PhaseDiagnostics diagnostics = diagnostics("get_user_objects");
    private final long streamStartNs = System.nanoTime();
    private final Span parentSpan = Span.current();
    // Owns the per-request pin state and drives the collect→commit pin-durability transaction.
    private final ChunkPinBarrier pinBarrier;

    private final BundleChunkStream stream;
    private int nextInputIndex = 0;
    // Read from the Mutiny termination/failure callback (transport thread) while the driver may
    // still be incrementing it mid-build on a cancelled stream; volatile makes that read see a
    // well-defined value, so a cancelled stream reports partial-but-not-torn telemetry.
    private volatile int emittedResolutionChunks = 0;
    private final AtomicBoolean telemetryPublished = new AtomicBoolean(false);
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private boolean defaultCatalogResolved = false;
    private String defaultCatalogName = "";
    // Set when the subscriber cancels; polled by the select/build fan-outs so a cancelled stream
    // stops draining in-flight tasks promptly instead of running the whole chunk to completion.
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    void markCancelled() {
      cancelled.set(true);
    }

    boolean isCancelled() {
      return cancelled.get();
    }

    UserObjectBundleIterator(
        String correlationId,
        QueryContext ctx,
        List<TableReferenceCandidate> tables,
        Set<String> knownBlobVersions) {
      this.correlationId = correlationId;
      this.ctx = ctx;
      this.stream = new BundleChunkStream(ctx.getQueryId(), MAX_RESOLUTIONS_PER_CHUNK);
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
      this.resolutionCache =
          new RelationResolutionCache(overlay, correlationId, requestEngine, timings);
      this.pinBarrier = new ChunkPinBarrier(inputResolver, queryStore, ctx, correlationId, timings);
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
      return stream.isOpen();
    }

    @Override
    public UserObjectsBundleChunk next() {
      throwIfCancelled(this::isCancelled);
      if (stream.headerPending()) {
        if (LOG.isDebugEnabled()) {
          LOG.debugf("Emitting header chunk query_id=%s seq=%d", ctx.getQueryId(), stream.seq());
        }
        return stream.header();
      }

      // Pump the pipeline into the framer only when it has nothing left to slice, so a batch's
      // pin/stats/build barrier stays aligned with the chunk it produces.
      if (!stream.hasBufferedResolutions()) {
        if (pending.isEmpty()
            && (nextInputIndex < resolutionCount
                || !eagerBaseQueue.isEmpty()
                || !resolvedSpillover.isEmpty())) {
          fillPending();
        }
        if (!pending.isEmpty()) {
          buildChunkIntoStream();
        }
      }

      if (stream.hasBufferedResolutions()) {
        emittedResolutionChunks++;
        return stream.nextResolutionChunk();
      }

      if (stream.isOpen()) {
        publishStreamTelemetry("completed");
        if (LOG.isDebugEnabled()) {
          LOG.debugf(
              "Emitting end chunk query_id=%s seq=%d resolutions=%d found=%d not_found=%d",
              ctx.getQueryId(), stream.seq(), resolutionCount, timings.found(), timings.notFound());
        }
        return stream.end(resolutionCount, timings.found(), timings.notFound());
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
              maxParallelRelations,
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
          timings.addResolveNanos(System.nanoTime() - selectStageStartNs);
        }
      }
      if (!toPin.isEmpty()) {
        pinBarrier.accumulate(toPin, diagnostics, this::isCancelled);
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
        timings.recordFound();
        toPin.add(found.relation());
        if (found.relation().node() instanceof ViewNode view && !view.baseRelations().isEmpty()) {
          eagerBaseQueue.addLast(new EagerBaseCursor(view));
          drainEagerBaseTables(toPin);
        }
      } else if (item instanceof PendingResolved resolved
          && resolved.resolution().getStatus() == ResolutionStatus.RESOLUTION_STATUS_NOT_FOUND) {
        timings.recordNotFound();
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
          Optional<ResourceId> baseIdOpt = resolutionCache.resolveName(enriched);
          if (baseIdOpt.isEmpty()) {
            continue;
          }
          ResourceId baseId = baseIdOpt.get();
          String baseKey = QueryPins.pinKey(baseId);
          if (eagerBaseSeen.contains(baseKey)) {
            continue; // deduplicate
          }
          Optional<GraphNode> nodeOpt = resolutionCache.resolveNode(baseId);
          if (nodeOpt.isEmpty() || !(nodeOpt.get() instanceof RelationNode rel)) {
            continue;
          }
          eagerBaseSeen.add(baseKey);
          QueryInput syntheticInput = QueryInput.newBuilder().setTableId(baseId).build();
          ResolvedRelation syntheticRelation =
              new ResolvedRelation(
                  TableReferenceCandidate.getDefaultInstance(),
                  baseId,
                  rel,
                  syntheticInput,
                  resolutionCache.canonicalName(rel));
          // Base-table pins are already derived from the parent view candidate (including AS-OF
          // overrides). Avoid re-adding a synthetic TABLE_ID pin here, which would otherwise
          // resolve to CURRENT and can overwrite AS-OF pins in the same batch.
          pending.add(new PendingFound(-1, syntheticRelation));
        } finally {
          timings.addBaseInjectNanos(System.nanoTime() - resolveStartNs);
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
        timings.addNormalizeNanos(System.nanoTime() - normalizeStartNs);
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
                  resolutionCache::resolveName,
                  resolutionCache::resolveNode,
                  resolutionCache::canonicalName);
        } finally {
          timings.addSelectRelationNanos(System.nanoTime() - selectStartNs);
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
      if (isCancelled()) {
        throw new java.util.concurrent.CancellationException("GetUserObjects cancelled");
      }
      List<ResourceId> tableIds = new ArrayList<>(chunkItems.size());
      Set<ResourceId> seenTableIds = new HashSet<>();
      for (PendingItem item : chunkItems) {
        if (item instanceof PendingFound found
            && found.relation().node().kind() == GraphNodeKind.TABLE
            && seenTableIds.add(found.relation().relationId())) {
          tableIds.add(found.relation().relationId());
        }
      }
      if (tableIds.isEmpty()) {
        return;
      }
      long startNs = System.nanoTime();
      try {
        statsProvider.tableStatsBatch(tableIds, this::isCancelled);
      } catch (java.util.concurrent.CancellationException e) {
        throw e;
      } catch (RuntimeException e) {
        LOG.debugf(
            e,
            "stats batch warm failed query_id=%s; build will resolve stats per relation",
            ctx.getQueryId());
      } finally {
        // The batch fetch happens here; the per-relation tableStats during build then hits the
        // cache. Record under stats_warm, NOT stats_lookup: charging both this warm fetch and the
        // build-time cache-hit reads to stats_lookup would double-count the same logical fetch.
        timings.addStatsWarmNanos(System.nanoTime() - startNs);
      }
      if (isCancelled()) {
        throw new java.util.concurrent.CancellationException("GetUserObjects cancelled");
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

    private void buildChunkIntoStream() {
      List<PendingItem> chunkItems = new ArrayList<>(pending);
      pending.clear();
      if (LOG.isDebugEnabled()) {
        LOG.debugf(
            "Flushing resolution chunk query_id=%s seq=%d pending_items=%d pending_pins=%d",
            ctx.getQueryId(), stream.seq(), chunkItems.size(), pinBarrier.pendingPinCount());
      }
      // Ensure pins are durable before accessing stats (which expect the QueryContext to be
      // pinned).
      pinBarrier.commit(this::isCancelled);
      if (isCancelled()) {
        throw new java.util.concurrent.CancellationException("GetUserObjects cancelled");
      }
      warmChunkStats(chunkItems);
      if (isCancelled()) {
        throw new java.util.concurrent.CancellationException("GetUserObjects cancelled");
      }
      QueryContext liveCtx = queryStore.get(ctx.getQueryId()).orElse(ctx);

      // Driver pre-pass: everything cheap and order/state-sensitive stays here — passthrough
      // resolutions, cache hits, and the identity-only fast path (which reads the shared
      // knownBlobVersions and timings). Relations needing a full build are collected for the
      // parallel stage; their pin identity, computed here for the slim check, is carried forward so
      // buildOne does not recompute it. slots keeps every resolution in chunk order.
      RelationResolution[] slots = new RelationResolution[chunkItems.size()];
      List<PendingFound> toBuild = new ArrayList<>();
      List<List<PendingFound>> buildFoundGroups = new ArrayList<>();
      List<List<Integer>> buildSlotGroups = new ArrayList<>();
      List<Optional<RelationPinIdentity>> buildIdentities = new ArrayList<>();
      Map<RelationCacheKey, Integer> buildIndexByKey = new HashMap<>();
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
        RelationCacheKey cacheKey = relationCacheKey(found.relation());
        Integer existingBuildIndex = buildIndexByKey.get(cacheKey);
        if (existingBuildIndex != null) {
          // The first full build produces an immutable payload that every same-key slot can share.
          // Keep all slots so the emitted response still mirrors the requested inputs exactly.
          buildFoundGroups.get(existingBuildIndex).add(found);
          buildSlotGroups.get(existingBuildIndex).add(i);
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
            possessionGate.scopedIdentity(
                correlationId, found.relation(), liveCtx, resolutionContext.engineContext());
        // Identity-only fast path: never cached — the info cache must only ever hold full payloads,
        // or a later request that did NOT prove possession would be served a payload-less relation.
        RelationInfo slim =
            possessionGate.identityOnly(
                found.relation(), scopedIdentity, statsProvider, knownBlobVersions, timings);
        if (slim != null) {
          // Its stats time already landed in timings via identityOnly; fold the remaining
          // (identity-build) time into relationBuildNanos so slim replies are not invisible.
          long buildNanos = System.nanoTime() - buildStartNs;
          long statsDeltaNanos = timings.statsLookupNanos() - statsBeforeNanos;
          timings.addRelationBuildNanos(Math.max(0L, buildNanos - statsDeltaNanos));
          slots[i] = foundResolution(found.inputIndex(), slim);
          continue;
        }
        toBuild.add(found);
        buildFoundGroups.add(new ArrayList<>(List.of(found)));
        buildSlotGroups.add(new ArrayList<>(List.of(i)));
        buildIdentities.add(scopedIdentity);
        buildIndexByKey.put(cacheKey, toBuild.size() - 1);
      }

      // Build the remaining relations concurrently; each task times itself into its own
      // accumulator so the summary math needs no shared-counter deltas.
      List<Integer> indices = java.util.stream.IntStream.range(0, toBuild.size()).boxed().toList();
      List<BuildOutcome> outcomes =
          BoundedFanout.mapOrdered(
              indices,
              maxParallelRelations,
              blockingExecutor,
              j -> buildOne(toBuild.get(j), liveCtx, buildIdentities.get(j)),
              this::isCancelled);

      // Driver gather: fold each task's timings in, cache full payloads, and place resolutions in
      // chunk order. A build that failed becomes an ERROR for that one relation (it was counted
      // FOUND at selection, so undo that — an ERROR counts toward neither found nor not_found).
      for (int j = 0; j < outcomes.size(); j++) {
        BuildOutcome outcome = outcomes.get(j);
        timings.mergeFrom(outcome.taskTimings());
        timings.addRelationBuildNanos(outcome.relationBuildNanos());
        timings.addDecorationNanos(outcome.decorationNanos());
        PendingFound found = outcome.source();
        List<PendingFound> groupedFound = buildFoundGroups.get(j);
        List<Integer> groupedSlots = buildSlotGroups.get(j);
        if (outcome.info() != null) {
          relationInfoCache.put(relationCacheKey(found.relation()), outcome.info());
          for (int k = 0; k < groupedFound.size(); k++) {
            slots[groupedSlots.get(k)] =
                foundResolution(groupedFound.get(k).inputIndex(), outcome.info());
          }
        } else {
          for (int k = 0; k < groupedFound.size(); k++) {
            PendingFound grouped = groupedFound.get(k);
            if (grouped.isRequestedInput()) {
              timings.unrecordFound();
            }
            slots[groupedSlots.get(k)] =
                outcome.error().toBuilder().setInputIndex(grouped.inputIndex()).build();
          }
        }
      }

      List<RelationResolution> resolutions = List.of(slots);
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
            ctx.getQueryId(),
            stream.seq(),
            resolutions.size(),
            chunkFound,
            chunkNotFound,
            chunkError);
      }
      stream.offer(resolutions);
    }

    // The GetUserObjects RPC has many internal sub-phases (resolve, decoration, ...). We do NOT
    // emit a span per phase -- they are not RPCs and only add noise to the trace. Per-phase
    // timings are attached as one summary event on the GetUserObjects RPC span (the single tally's
    // flushInto), so Jaeger stays readable for small catalog lookups.
    private void publishStreamTelemetry(String outcome) {
      if (!telemetryPublished.compareAndSet(false, true)) {
        return;
      }
      long totalNanos = System.nanoTime() - streamStartNs;
      long schedulingNanos = timings.schedulingNanos(totalNanos);
      double totalMs = totalNanos / 1_000_000.0;
      double pinMs = timings.pinNanos() / 1_000_000.0;
      double schedulingMs = schedulingNanos / 1_000_000.0;
      timings.flushInto(
          diagnostics,
          new SummaryContext(
              ctx.getQueryId(),
              correlationId,
              resolutionCount,
              emittedResolutionChunks,
              totalMs,
              pinMs,
              schedulingMs,
              resolutionCache.nameEntries(),
              resolutionCache.nodeEntries(),
              relationInfoCache.size(),
              safe(outcome)));
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
            timings.resolveNanos() / 1_000_000.0,
            timings.baseInjectNanos() / 1_000_000.0,
            pinMs,
            timings.relationBuildNanos() / 1_000_000.0,
            timings.decorationNanos() / 1_000_000.0,
            timings.statsLookupNanos() / 1_000_000.0,
            schedulingMs,
            resolutionCount,
            emittedResolutionChunks,
            timings.found(),
            timings.notFound(),
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
            timings.found(),
            timings.notFound(),
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
      parentSpan.setAttribute("floecat.get_user_objects.found", timings.found());
      parentSpan.setAttribute("floecat.get_user_objects.not_found", timings.notFound());
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
          timings.recordDefaultCatalogLookup();
        } finally {
          timings.addDefaultCatalogNanos(System.nanoTime() - startNs);
        }
      }
      return defaultCatalogName;
    }

    // Reads the plain defaultCatalog* fields from selectOne failure builders, which run on fan-out
    // threads. Safe without volatile: the driver resolves the default catalog in the serial
    // planInput loop before mapOrdered submits any task, and task submission happens-before the
    // task body -- so a fan-out thread sees the resolved value or the initial false/"" (never a
    // torn write). Diagnostic-only; do not read these fields on a path that can precede the loop.
    private String defaultCatalogForDiagnostics() {
      return defaultCatalogResolved ? defaultCatalogName : "";
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

      boolean isRequestedInput() {
        return inputIndex >= 0;
      }
    }

    private static final class EagerBaseCursor {
      private final ViewNode view;
      private int nextBaseIndex;

      private EagerBaseCursor(ViewNode view) {
        this.view = view;
      }
    }
  }
}
