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
import ai.floedb.floecat.query.rpc.RelationPinSet;
import ai.floedb.floecat.query.rpc.RelationResolution;
import ai.floedb.floecat.query.rpc.ResolutionFailure;
import ai.floedb.floecat.query.rpc.ResolutionStatus;
import ai.floedb.floecat.query.rpc.TableReferenceCandidate;
import ai.floedb.floecat.query.rpc.UserObjectsBundleChunk;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.scanner.spi.MetadataResolutionContext;
import ai.floedb.floecat.scanner.spi.StatsProvider;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.concurrent.MetadataFanout;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.context.PropagatedContext;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
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

  private final CatalogGraphView graphView;
  private final QueryInputResolver inputResolver;
  private final QueryContextStore queryStore;
  private final EngineContextProvider engineContext;
  // Bumped when the engine decorator's behavior changes WITHOUT moving the engine version; folded
  // into the identity-only payload token so a decorator change invalidates cached decoration.
  private final String decorationEpoch;
  private final StatsProviderFactory statsFactory;
  private final long slowRpcMs;
  private final RelationBundleBuilder relationBuilder;
  private final EngineRelationDecorator engineRelationDecorator;
  private final CancelledQueryPinCleanup cancelledQueryPinCleanup;

  // Mints the pin identity/payload token and serves the identity-only decision. Stateless per
  // call; reused on the driver thread across every chunk.
  private final RelationPayloadPolicy relationPayloadPolicy;

  @Inject Observability observability;

  // Selection is graph-view-only; payload builds also depend on a decorator's callback affinity.
  private final MetadataFanout selectionFanout;
  private final int maxParallelRelations;

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
      CatalogGraphView graphView,
      QueryInputResolver inputResolver,
      QueryContextStore queryStore,
      CancelledQueryPinCleanup cancelledQueryPinCleanup,
      StatsProviderFactory statsFactory,
      EngineMetadataDecoratorProvider decoratorProvider,
      EngineContextProvider engineContext,
      PinValidator pinValidator,
      @ConfigProperty(name = "floecat.catalog.bundle.emit_engine_specific", defaultValue = "true")
          boolean engineSpecificEnabled,
      // Epoch 2 covers the typed SchemaColumn payload migration and must never move backward.
      @ConfigProperty(name = "floecat.catalog.bundle.decoration_epoch", defaultValue = "2")
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
    this.graphView = graphView;
    this.inputResolver = inputResolver;
    this.queryStore = queryStore;
    this.cancelledQueryPinCleanup = cancelledQueryPinCleanup;
    this.statsFactory = statsFactory;
    this.engineContext = engineContext;
    this.decorationEpoch = safe(decorationEpoch);
    this.slowRpcMs = Math.max(0L, slowRpcMs);
    // A permit count of 0 or negative would wedge every GetUserObjects request forever in the
    // fan-out's permit wait, so clamp to serial and warn rather than serve in a permanently-hanging
    // state.
    if (maxParallelRelations < 1) {
      LOG.warnf(
          "floecat.catalog.bundle.max_parallel_relations=%d is invalid; clamping to 1 (serial)",
          maxParallelRelations);
    }
    this.maxParallelRelations = Math.max(1, maxParallelRelations);
    this.selectionFanout =
        graphView.supportsConcurrentResolution()
            ? MetadataFanout.concurrent(this.maxParallelRelations)
            : MetadataFanout.serial();
    FlightEndpointRef advertisedFlightEndpoint =
        FlightEndpointRef.newBuilder()
            .setHost(flightHost)
            .setPort(flightPort)
            .setTls(!grpcPlainText)
            .build();
    SystemExecutionResolver systemExecutionResolver =
        new SystemExecutionResolver(advertisedFlightEndpoint);
    this.engineRelationDecorator =
        new EngineRelationDecorator(decoratorProvider, engineSpecificEnabled);
    this.relationBuilder =
        new RelationBundleBuilder(
            graphView, engineRelationDecorator, systemExecutionResolver, pinValidator);
    this.relationPayloadPolicy =
        new RelationPayloadPolicy(
            relationBuilder,
            systemExecutionResolver,
            engineRelationDecorator,
            this.decorationEpoch);
    warnFlightHost(flightHost, quarkusProfile);
  }

  UserObjectBundleService(
      CatalogGraphView graphView,
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
    // the fake graph view). Fail explicitly if one ever does, rather than NPE-ing on null repos.
    this(
        graphView,
        inputResolver,
        queryStore,
        new CancelledQueryPinCleanup(queryStore, Runnable::run),
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

  /** {@link #stream(String, QueryContext, List, Set)} with no payload hint. */
  public Multi<UserObjectsBundleChunk> stream(
      String correlationId, QueryContext ctx, List<TableReferenceCandidate> tables) {
    return stream(correlationId, ctx, tables, Set.of());
  }

  public Multi<UserObjectsBundleChunk> stream(
      String correlationId,
      QueryContext ctx,
      List<TableReferenceCandidate> tables,
      Set<String> knownPayloadTokens) {
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
                  new UserObjectBundleIterator(correlationId, ctx, candidates, knownPayloadTokens);
              return Multi.createFrom()
                  .iterable(() -> iterator)
                  .onFailure()
                  .invoke(
                      failure -> {
                        if (!(failure instanceof CancellationException)) {
                          iterator.publishStreamTelemetry("failed");
                        }
                        iterator.cancel();
                      })
                  .onCancellation()
                  .invoke(iterator::cancel);
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

  private static String safe(String value) {
    return value == null ? "" : value;
  }

  /** Select a build scheduler without letting graph-view capability change decorator affinity. */
  private MetadataFanout buildFanout(EngineRelationDecorator.Selection decorationSelection) {
    if (!graphView.supportsConcurrentResolution()) {
      return MetadataFanout.serial();
    }
    return decorationSelection.supportsWorkerThreadCallbacks()
        ? MetadataFanout.concurrent(maxParallelRelations)
        : MetadataFanout.serial();
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
    private final EngineRelationDecorator.Selection decorationSelection;
    private final MetadataFanout buildFanout;
    private final String engineKind;
    private final String engineVersion;
    /* Content versions the request proved it holds; relations resolving to
     * one of these get an identity-only response (see RelationPayloadPolicy#identityOnly). */
    private final Set<String> knownPayloadTokens;

    // Maintains the order inputs were resolved so the emitted chunk mirrors the request order.
    private final List<PendingItem> pending = new ArrayList<>(MAX_RESOLUTIONS_PER_CHUNK);
    // Per-request name/node resolution memo, shared (thread-safe) across the concurrent select
    // stage; records its hit/miss and resolve-nanos into the request tally below.
    private final RelationResolutionMemo resolutionMemo;
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
    private final QueryPinCommitter pinCommitter;
    // A teardown may publish only after the active next() call has finished mutating iterator
    // diagnostics and caches; a real failure wins when cancellation races that final step.
    private final StreamTelemetryState telemetryState = new StreamTelemetryState();

    private final BundleStreamFramer framer;
    private int nextInputIndex = 0;
    private int emittedResolutionChunks = 0;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private boolean defaultCatalogResolved = false;
    private String defaultCatalogName = "";

    UserObjectBundleIterator(
        String correlationId,
        QueryContext ctx,
        List<TableReferenceCandidate> tables,
        Set<String> knownPayloadTokens) {
      this.correlationId = correlationId;
      this.ctx = ctx;
      this.framer = new BundleStreamFramer(ctx.getQueryId(), MAX_RESOLUTIONS_PER_CHUNK);
      this.tables = tables;
      // Read-only for the life of the iterator (consulted by the identity-only fast path); copy so
      // that stays true regardless of what the caller does with its set afterwards.
      this.knownPayloadTokens = Set.copyOf(knownPayloadTokens);
      this.resolutionCount = tables.size();
      this.defaultCatalogId = ctx.getQueryDefaultCatalogId();
      this.statsProvider = statsFactory.forQuery(ctx, correlationId);
      EngineContext requestEngine = engineContext.engineContext();
      this.engineKind = requestEngine.normalizedKind();
      this.engineVersion = requestEngine.normalizedVersion();
      this.resolutionContext =
          MetadataResolutionContext.of(
              graphView,
              Objects.requireNonNull(ctx.getQueryDefaultCatalogId(), "query default catalog id"),
              requestEngine,
              statsProvider);
      this.decorationSelection = engineRelationDecorator.select(requestEngine);
      this.buildFanout = buildFanout(decorationSelection);
      this.resolutionMemo =
          new RelationResolutionMemo(graphView, correlationId, requestEngine, timings);
      this.pinCommitter =
          new QueryPinCommitter(inputResolver, queryStore, ctx, correlationId, timings);
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
      return framer.isOpen();
    }

    @Override
    public UserObjectsBundleChunk next() {
      beginProducerStep();
      StreamTelemetryState.Publication terminalOutcome = StreamTelemetryState.Publication.NONE;
      try (var cancellationScope = PropagatedContext.bindCancellation(this::isCancelled)) {
        throwIfCancelled(this::isCancelled);
        if (framer.headerPending()) {
          if (LOG.isDebugEnabled()) {
            LOG.debugf("Emitting header chunk query_id=%s seq=%d", ctx.getQueryId(), framer.seq());
          }
          return framer.header();
        }

        // Pump the pipeline into the framer only when it has nothing left to slice, so a batch's
        // pin commit and stats/build work stay aligned with the chunk it produces.
        if (!framer.hasBufferedResolutions()) {

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

        if (framer.hasBufferedResolutions()) {
          emittedResolutionChunks++;
          return framer.nextResolutionChunk();
        }

        if (framer.isOpen()) {
          terminalOutcome = StreamTelemetryState.Publication.COMPLETION;
          if (LOG.isDebugEnabled()) {
            LOG.debugf(
                "Emitting end chunk query_id=%s seq=%d resolutions=%d found=%d not_found=%d",
                ctx.getQueryId(),
                framer.seq(),
                resolutionCount,
                timings.found(),
                timings.notFound());
          }
          return framer.end(resolutionCount, timings.found(), timings.notFound());
        }

        throw new NoSuchElementException();
      } catch (RuntimeException | Error failure) {
        if (!(failure instanceof CancellationException)) {
          terminalOutcome = StreamTelemetryState.Publication.FAILURE;
        }
        throw failure;
      } finally {
        finishProducerStep(terminalOutcome);
      }
    }

    private void fillPending() {
      throwIfCancelled(this::isCancelled);
      List<ResolvedRelation> toPin = new ArrayList<>(MAX_RESOLUTIONS_PER_CHUNK);
      // Carry-over first, in emit order: a prior view's undrained base tables, then requested
      // inputs a prior chunk selected but could not fit.
      drainEagerBaseTables(toPin);
      while (!resolvedSpillover.isEmpty() && pending.size() < MAX_RESOLUTIONS_PER_CHUNK) {
        throwIfCancelled(this::isCancelled);
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
        // Consume in request order so an earlier deferred failure precedes a later task failure,
        // while still letting an eager view fill this chunk and spill later requested inputs.
        Consumer<PendingItem> consumeSelected =
            item -> {
              // A view gathered earlier in this loop can fill the chunk via its base tables; the
              // remaining already-selected inputs wait for the next chunk (their nodes are cached).
              if (pending.size() >= MAX_RESOLUTIONS_PER_CHUNK) {
                resolvedSpillover.addLast(item);
              } else {
                gather(item, toPin);
              }
            };
        long selectStageStartNs = System.nanoTime();
        try {
          selectionFanout.forEachOrdered(plan, this::selectOne, consumeSelected, this::isCancelled);
        } finally {
          timings.addResolveNanos(System.nanoTime() - selectStageStartNs);
        }
      }
      if (!toPin.isEmpty()) {
        pinCommitter.accumulate(toPin, diagnostics, this::isCancelled);
      }
    }

    private boolean isCancelled() {
      return cancelled.get();
    }

    private Optional<ResourceId> resolveNameCached(NameRef ref) {
      return resolutionMemo.resolveName(ref);
    }

    private Optional<GraphNode> resolveNodeCached(ResourceId id) {
      return resolutionMemo.resolveNode(id);
    }

    private NameRef canonicalNameCached(RelationNode relation) {
      return resolutionMemo.canonicalName(relation);
    }

    /**
     * Mark the iterator cancelled, detach pending pins, publish only stable telemetry, and offload
     * root release so a transport/event-loop termination callback never performs store I/O.
     */
    private void cancel() {
      StreamTelemetryState.CancellationDecision cancellation = telemetryState.cancel(cancelled);
      if (cancellation != StreamTelemetryState.CancellationDecision.IGNORED) {
        RelationPinSet toRelease = pinCommitter.detachPendingPins();
        if (cancellation == StreamTelemetryState.CancellationDecision.PUBLISH) {
          // No producer is mutating diagnostics or caches, but the RPC span may end as soon as
          // this termination callback returns. Emit while it is still recording.
          publishClaimedTelemetrySafely("cancelled");
        }
        // onTermination may run on a transport/event-loop thread. Root release can perform store
        // I/O, so teardown runs on a managed executor. Telemetry is published only after the
        // producer reports that no mutable iterator state remains active.
        cancelledQueryPinCleanup.release(ctx.getQueryId(), toRelease);
      }
    }

    /** Claim mutable iterator state for one producer step unless cancellation already won. */
    private void beginProducerStep() {
      telemetryState.begin(this::isCancelled);
    }

    /**
     * Release producer ownership and publish exactly one terminal outcome, giving a real failure
     * precedence over cancellation that raced the active step.
     */
    private void finishProducerStep(StreamTelemetryState.Publication terminalOutcome) {
      StreamTelemetryState.Publication publication = telemetryState.finish(terminalOutcome);
      switch (publication) {
        case COMPLETION -> publishClaimedTelemetrySafely("completed");
        case FAILURE -> publishClaimedTelemetrySafely("failed");
        case CANCELLATION -> publishClaimedTelemetrySafely("cancelled");
        case NONE -> {}
      }
    }

    /** Publish a claimed outcome without allowing telemetry failure to mask stream termination. */
    private void publishClaimedTelemetrySafely(String outcome) {
      try {
        publishClaimedStreamTelemetry(outcome);
      } catch (RuntimeException telemetryFailure) {
        LOG.warnf(
            telemetryFailure,
            "Failed to publish %s stream telemetry query_id=%s",
            outcome,
            ctx.getQueryId());
      }
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
        throwIfCancelled(this::isCancelled);
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
        throwIfCancelled(this::isCancelled);
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
                  TableReferenceCandidate.getDefaultInstance(),
                  baseId,
                  rel,
                  syntheticInput,
                  canonicalNameCached(rel));
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
      throwIfCancelled(this::isCancelled);
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
      throwIfCancelled(this::isCancelled);
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
                  this::resolveNodeCached,
                  this::canonicalNameCached);
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
     * the pin committer. The returned immutable lookup is carried into relation assembly so worker
     * tasks never re-enter the request-affine stats provider. Views carry no table stats and are
     * skipped. A batch failure is best-effort and leaves stats absent for this chunk.
     */
    private Map<ResourceId, Optional<StatsProvider.TableStatsView>> warmChunkStats(
        List<PendingItem> chunkItems) {
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
        return Map.of();
      }
      long startNs = System.nanoTime();
      Map<ResourceId, Optional<StatsProvider.TableStatsView>> statsByTable = Map.of();
      try {
        statsByTable = Map.copyOf(statsProvider.tableStatsBatch(tableIds, this::isCancelled));
      } catch (java.util.concurrent.CancellationException e) {
        throw e;
      } catch (RuntimeException e) {
        LOG.debugf(
            e,
            "stats batch warm failed query_id=%s; this chunk will omit table stats",
            ctx.getQueryId());
      } finally {
        // The batch fetch happens here and its values are carried into assembly. Record under
        // stats_warm, not stats_lookup: charging both the fetch and value attachment as lookup work
        // would double-count the same logical operation.
        timings.addStatsWarmNanos(System.nanoTime() - startNs);
      }
      if (isCancelled()) {
        throw new java.util.concurrent.CancellationException("GetUserObjects cancelled");
      }
      return statsByTable;
    }

    /** A built relation's outcome: exactly one of {@code info} / {@code error} is non-null. */
    private record BuildOutcome(
        PendingFound source,
        RelationInfo info,
        RelationResolution error,
        long relationBuildNanos,
        long decorationNanos,
        TimingAccumulator taskTimings) {}

    /** One requested relation and the ordered response slot that receives its build result. */
    private record BuildTarget(PendingFound found, int slot) {}

    /** One unique full build and every response slot that can share its immutable result. */
    private static final class BuildPlan {
      private final List<BuildTarget> targets;
      private final Optional<RelationBundleBuilder.BuildError> validationError;
      private final Optional<RelationPinIdentity> payloadIdentity;

      private BuildPlan(
          PendingFound source,
          int slot,
          Optional<RelationBundleBuilder.BuildError> validationError,
          Optional<RelationPinIdentity> payloadIdentity) {
        this.targets = new ArrayList<>(List.of(new BuildTarget(source, slot)));
        this.validationError = validationError;
        this.payloadIdentity = payloadIdentity;
      }

      private void addDuplicate(PendingFound duplicate, int slot) {
        targets.add(new BuildTarget(duplicate, slot));
      }

      private PendingFound source() {
        return targets.getFirst().found();
      }
    }

    /**
     * Build one relation's full payload, timing into a task-local accumulator so parallel builds
     * need no shared-counter deltas. A build failure is isolated to this relation as an ERROR
     * resolution — one relation's decoration/schema/stats fault must not sink the whole bundle.
     */
    private BuildOutcome buildOne(
        BuildPlan plan, QueryContext liveCtx, Optional<StatsProvider.TableStatsView> tableStats) {
      PendingFound found = plan.source();
      if (plan.validationError.isPresent()) {
        return new BuildOutcome(
            found,
            null,
            buildErrorResolution(found, plan.validationError.get()),
            0L,
            0L,
            new TimingAccumulator());
      }
      long buildStartNs = System.nanoTime();
      RelationBundleBuilder.BuildResult result =
          relationBuilder.build(
              correlationId,
              found.relation(),
              liveCtx,
              resolutionContext,
              decorationSelection,
              tableStats,
              plan.payloadIdentity);
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
      throwIfCancelled(this::isCancelled);

      List<PendingItem> chunkItems = new ArrayList<>(pending);
      pending.clear();
      if (LOG.isDebugEnabled()) {
        LOG.debugf(
            "Flushing resolution chunk query_id=%s seq=%d pending_items=%d pending_pins=%d",
            ctx.getQueryId(), framer.seq(), chunkItems.size(), pinCommitter.pendingPinCount());
      }
      // Ensure pins are durable before accessing stats (which expect the QueryContext to be
      // pinned).
      pinCommitter.commit(this::isCancelled);
      throwIfCancelled(this::isCancelled);

      Map<ResourceId, Optional<StatsProvider.TableStatsView>> statsByTable =
          warmChunkStats(chunkItems);
      throwIfCancelled(this::isCancelled);

      QueryContext liveCtx = queryStore.get(ctx.getQueryId()).orElse(ctx);

      // Driver pre-pass: everything cheap and order/state-sensitive stays here — passthrough
      // resolutions, cache hits, and the identity-only fast path (which reads the shared
      // knownPayloadTokens and timings). Relations needing a full build are collected for the
      // parallel stage; their pin identity, computed here for the slim check, is carried forward so
      // buildOne does not recompute it. slots keeps every resolution in chunk order.
      RelationResolution[] slots = new RelationResolution[chunkItems.size()];
      List<BuildPlan> buildPlans = new ArrayList<>();
      Map<RelationCacheKey, Integer> buildIndexByKey = new HashMap<>();
      for (int i = 0; i < chunkItems.size(); i++) {
        throwIfCancelled(this::isCancelled);
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
          buildPlans.get(existingBuildIndex).addDuplicate(found, i);
          continue;
        }
        long statsBeforeNanos = timings.statsLookupNanos();
        long buildStartNs = System.nanoTime();
        // Compute the pin identity at most once per relation: the identity-only match consults it
        // when the client sent hints, and the full-build stamp reuses it — so a cache miss under a
        // populated hint set does not hash the relation twice. Computed for EVERY pinned relation
        // (not only full-schema ones): the stamp preserves the data identity even on a projected
        // reply, merely blanking the payload token there.
        Optional<RelationPinIdentity> payloadIdentity =
            relationPayloadPolicy.payloadIdentity(
                correlationId, found.relation(), liveCtx, resolutionContext.engineContext());
        // Identity-only fast path: never cached — the info cache must only ever hold full payloads,
        // or a later request that does not already have the payload would be served a payload-less
        // relation.
        RelationInfo slim =
            relationPayloadPolicy.identityOnly(
                found.relation(),
                payloadIdentity,
                statsByTable.getOrDefault(found.relation().relationId(), Optional.empty()),
                knownPayloadTokens,
                timings);
        throwIfCancelled(this::isCancelled);

        if (slim != null) {
          // Its stats time already landed in timings via identityOnly; fold the remaining
          // (identity-build) time into relationBuildNanos so slim replies are not invisible.
          long buildNanos = System.nanoTime() - buildStartNs;
          long statsDeltaNanos = timings.statsLookupNanos() - statsBeforeNanos;
          timings.addRelationBuildNanos(Math.max(0L, buildNanos - statsDeltaNanos));
          slots[i] = foundResolution(found.inputIndex(), slim);
          continue;
        }
        Optional<RelationBundleBuilder.BuildError> validationError =
            relationBuilder.validatePin(correlationId, found.relation(), liveCtx);
        buildPlans.add(new BuildPlan(found, i, validationError, payloadIdentity));
        buildIndexByKey.put(cacheKey, buildPlans.size() - 1);
      }

      // Build the remaining relations concurrently; each task times itself into its own
      // accumulator so the summary math needs no shared-counter deltas.
      long buildFanoutStartNs = System.nanoTime();
      List<BuildOutcome> outcomes =
          buildFanout.mapOrdered(
              buildPlans,
              plan ->
                  buildOne(
                      plan,
                      liveCtx,
                      statsByTable.getOrDefault(
                          plan.source().relation().relationId(), Optional.empty())),
              this::isCancelled);
      timings.addBuildFanoutNanos(System.nanoTime() - buildFanoutStartNs);

      // Driver gather: fold each task's timings in, cache full payloads, and place resolutions in
      // chunk order. A build that failed becomes an ERROR for that one relation (it was counted
      // FOUND at selection, so undo that — an ERROR counts toward neither found nor not_found).
      for (int j = 0; j < outcomes.size(); j++) {
        BuildOutcome outcome = outcomes.get(j);
        timings.mergeFrom(outcome.taskTimings());
        timings.addRelationBuildNanos(outcome.relationBuildNanos());
        timings.addDecorationNanos(outcome.decorationNanos());
        PendingFound found = outcome.source();
        BuildPlan plan = buildPlans.get(j);
        if (outcome.info() != null) {
          relationInfoCache.put(relationCacheKey(found.relation()), outcome.info());
          for (BuildTarget target : plan.targets) {
            slots[target.slot()] = foundResolution(target.found().inputIndex(), outcome.info());
          }
        } else {
          for (BuildTarget target : plan.targets) {
            if (target.found().isRequestedInput()) {
              timings.unrecordFound();
            }
            slots[target.slot()] =
                outcome.error().toBuilder().setInputIndex(target.found().inputIndex()).build();
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
            framer.seq(),
            resolutions.size(),
            chunkFound,
            chunkNotFound,
            chunkError);
      }
      framer.offer(resolutions);
    }

    // The GetUserObjects RPC has many internal sub-phases (resolve, decoration, ...). We do NOT
    // emit a span per phase -- they are not RPCs and only add noise to the trace. Per-phase
    // timings are attached as one summary event on the GetUserObjects RPC span (the single tally's
    // flushInto), so Jaeger stays readable for small catalog lookups.
    private void publishStreamTelemetry(String outcome) {
      if (!telemetryState.claim()) {
        return;
      }
      publishClaimedStreamTelemetry(outcome);
    }

    /** Publish after telemetry state has atomically selected this terminal outcome. */
    private void publishClaimedStreamTelemetry(String outcome) {
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
              resolutionMemo.nameEntries(),
              resolutionMemo.nodeEntries(),
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
              graphView.catalog(defaultCatalogId).map(CatalogNode::displayName).orElse("");
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
