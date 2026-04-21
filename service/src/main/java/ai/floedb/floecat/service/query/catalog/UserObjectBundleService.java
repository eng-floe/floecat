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
import ai.floedb.floecat.query.rpc.RelationResolution;
import ai.floedb.floecat.query.rpc.RelationResolutions;
import ai.floedb.floecat.query.rpc.ResolutionFailure;
import ai.floedb.floecat.query.rpc.ResolutionStatus;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.query.rpc.SqlDefinition;
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
import ai.floedb.floecat.service.query.QueryContextStore;
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
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeFormat;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
  private final StatsProviderFactory statsFactory;
  private final long slowRpcMs;
  private final LogicalSchemaMapper logicalSchemaMapper = new LogicalSchemaMapper();
  private final FlightEndpointRef floecatFlightEndpoint;

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

  @Inject
  public UserObjectBundleService(
      CatalogOverlay overlay,
      QueryInputResolver inputResolver,
      QueryContextStore queryStore,
      StatsProviderFactory statsFactory,
      EngineMetadataDecoratorProvider decoratorProvider,
      EngineContextProvider engineContext,
      @ConfigProperty(name = "floecat.catalog.bundle.emit_engine_specific", defaultValue = "true")
          boolean engineSpecificEnabled,
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
    this.engineSpecificEnabled = engineSpecificEnabled;
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
    this(
        overlay,
        inputResolver,
        queryStore,
        statsFactory,
        decoratorProvider,
        engineContext,
        engineSpecificEnabled,
        flightHost,
        flightPort,
        grpcPlainText,
        quarkusProfile,
        250L);
  }

  public Multi<UserObjectsBundleChunk> stream(
      String correlationId, QueryContext ctx, List<TableReferenceCandidate> tables) {
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
                  new UserObjectBundleIterator(correlationId, ctx, candidates);
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

  private SnapshotSet collectChunkPins(
      String correlationId,
      QueryContext ctx,
      List<ResolvedRelation> relations,
      Map<ResourceId, SnapshotPin> currentSnapshotPinCache) {
    if (relations == null || relations.isEmpty()) {
      return SnapshotSet.getDefaultInstance();
    }
    List<QueryInput> inputs = new ArrayList<>(relations.size());
    for (ResolvedRelation relation : relations) {
      QueryInput input = buildCanonicalQueryInput(relation);
      if (input != null) {
        inputs.add(input);
      }
    }
    if (inputs.isEmpty()) {
      return SnapshotSet.getDefaultInstance();
    }
    var asOfDefault = ctx.parseAsOfDefault(correlationId);
    var resolution =
        inputResolver.resolveInputs(
            correlationId,
            inputs,
            asOfDefault,
            Optional.of(ctx.getQueryDefaultCatalogId()),
            currentSnapshotPinCache);
    SnapshotSet incoming = resolution.snapshotSet();
    return incoming == null ? SnapshotSet.getDefaultInstance() : incoming;
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

  private RelationInfo buildRelation(
      String correlationId,
      ResolvedRelation relation,
      QueryContext queryContext,
      MetadataResolutionContext resolutionContext,
      StatsProvider statsProvider,
      TimingAccumulator timings) {
    if (LOG.isTraceEnabled()) {
      LOG.tracef(
          "Building relation bundle query_id=%s relation=%s kind=%s origin=%s",
          queryContext.getQueryId(),
          relation.relationId(),
          relation.node().kind(),
          relation.node().origin());
    }

    RelationKind kind = mapKind(relation.node().kind(), relation.node().origin());
    Origin origin = mapOrigin(relation.node().origin());
    NameRef name = canonicalName(relation.relationId(), relation.node());

    List<SchemaColumn> schemaColumns =
        relation.node() instanceof ViewNode view
            ? view.outputColumns()
            : relation.node() instanceof UserTableNode userTable
                ? logicalSchemaMapper.map(userTable).getColumnsList()
                : overlay.tableSchema(relation.node().id());

    List<SchemaColumn> pruned =
        UserObjectBundleUtils.pruneSchema(schemaColumns, relation.candidate(), correlationId);

    List<ColumnInfo> columns =
        UserObjectBundleUtils.columnsFor(schemaColumns, pruned, origin, correlationId);

    RelationInfo.Builder builder =
        RelationInfo.newBuilder()
            .setRelationId(relation.relationId())
            .setName(name)
            .setKind(kind)
            .setOrigin(origin);

    /*
     * Populate the bundled endpoint metadata so workers know how to reach the table. FLOECAT
     * tables always use our built-in Flight server, and STORAGE tables can either point at their
     * own Flight endpoint, use an endpoint key resolved from service config, or expose a storage
     * path fallback. ENGINE tables never set an endpoint.
     */
    if (relation.node() instanceof SystemTableNode systemTableNode) {
      builder.setBackendKind(systemTableNode.backendKind());
      if (systemTableNode instanceof SystemTableNode.FloeCatSystemTableNode) {
        builder.setFlightEndpoint(floecatFlightEndpoint);
      } else if (systemTableNode instanceof SystemTableNode.StorageSystemTableNode storage) {
        if (storage.flightEndpoint() != null) {
          builder.setFlightEndpoint(storage.flightEndpoint());
        } else {
          Optional<FlightEndpointRef> configuredEndpoint =
              configuredEndpointForKey(storage.storageEndpointKey());
          if (configuredEndpoint.isPresent()) {
            builder.setFlightEndpoint(configuredEndpoint.get());
          } else if (!storage.storagePath().isBlank()) {
            builder.setStoragePath(storage.storagePath());
          }
        }
      }
    }

    long statsLookupStartNs = System.nanoTime();
    statsProvider
        .tableStats(relation.relationId())
        .map(StatsProviderFactory::toRelationStats)
        .ifPresent(builder::setStats);
    timings.addStatsLookupNanos(System.nanoTime() - statsLookupStartNs);

    // If this is a view, keep a mutable builder around for decoration.
    ViewDefinition.Builder viewBuilder = null;
    if (relation.node() instanceof ViewNode view) {
      viewBuilder = viewDefinitionBuilder(view);
      builder.setViewDefinition(viewBuilder);
    }

    EngineContext ctx = engineContext.engineContext();
    boolean decorationRequired = decorationRequired(ctx);
    Optional<EngineMetadataDecorator> decorator = currentDecorator(ctx);
    RelationDecoration relationDecoration = null;
    boolean relationDecorationSucceeded = true;
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

  private QueryContext mergeSnapshotSet(
      QueryContext existing, SnapshotSet incoming, String correlationId) {
    if (incoming == null || incoming.getPinsCount() == 0) {
      return existing;
    }
    SnapshotSet current = parseSnapshotSet(existing, correlationId);
    SnapshotSet merged = mergeSnapshotSets(current, incoming);
    if (current.equals(merged)) {
      return existing;
    }
    return existing.toBuilder().snapshotSet(merged.toByteArray()).build();
  }

  private SnapshotSet parseSnapshotSet(QueryContext ctx, String correlationId) {
    return ctx.parseSnapshotSet(correlationId);
  }

  private SnapshotSet mergeSnapshotSets(SnapshotSet existing, SnapshotSet incoming) {
    if (existing.getPinsCount() == 0 && incoming.getPinsCount() == 0) {
      return existing;
    }
    Map<String, SnapshotPin> merged = new LinkedHashMap<>();
    for (SnapshotPin pin : existing.getPinsList()) {
      merged.put(pinKey(pin.getTableId()), pin);
    }
    for (SnapshotPin pin : incoming.getPinsList()) {
      merged.merge(pinKey(pin.getTableId()), pin, UserObjectBundleService::mergePin);
    }
    return SnapshotSet.newBuilder().addAllPins(merged.values()).build();
  }

  private static SnapshotPin mergePin(SnapshotPin current, SnapshotPin incoming) {
    if (incoming == null) {
      return current;
    }
    if (current == null) {
      return incoming;
    }
    if (current.hasSnapshotId()) {
      return current;
    }
    if (incoming.hasSnapshotId()) {
      return incoming;
    }
    if (current.hasAsOf()) {
      return current;
    }
    return incoming;
  }

  private static String pinKey(ResourceId rid) {
    return String.join(":", rid.getAccountId(), rid.getKind().name(), rid.getId());
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

    // Maintains the order inputs were resolved so the emitted chunk mirrors the request order.
    private final List<PendingItem> pending = new ArrayList<>(MAX_RESOLUTIONS_PER_CHUNK);
    private final Map<NormalizedNameRef, Optional<ResourceId>> nameResolutionCache =
        new HashMap<>();
    private final Map<ResourceId, Optional<GraphNode>> nodeResolutionCache = new HashMap<>();
    private final ArrayDeque<EagerBaseCursor> eagerBaseQueue = new ArrayDeque<>();
    private final Set<String> eagerBaseSeen = new HashSet<>();
    private final Map<RelationCacheKey, RelationInfo> relationInfoCache = new HashMap<>();
    private final Map<ResourceId, SnapshotPin> currentSnapshotPinCache = new HashMap<>();
    private final TimingAccumulator timings = new TimingAccumulator();
    private final long streamStartNs = System.nanoTime();
    private final long streamStartEpochNs =
        TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
    private final Span parentSpan = Span.current();
    private SnapshotSet pendingChunkPins = SnapshotSet.getDefaultInstance();

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
    private long baseInjectNanos = 0L;
    private long pinCollectNanos = 0L;
    private long pinCommitNanos = 0L;
    private long relationBuildNanos = 0L;
    private long decorationNanos = 0L;

    UserObjectBundleIterator(
        String correlationId, QueryContext ctx, List<TableReferenceCandidate> tables) {
      this.correlationId = correlationId;
      this.ctx = ctx;
      this.tables = tables;
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
          accumulateChunkPins(collectChunkPins(correlationId, ctx, toPin, currentSnapshotPinCache));
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
          String baseKey = pinKey(baseId);
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
        List<QueryInput> normalized =
            normalizeCandidates(correlationId, candidate, this::defaultCatalogName);
        try {
          Optional<ResolvedRelation> resolved =
              selectResolvedRelation(
                  correlationId,
                  candidate,
                  normalized,
                  this::resolveNameCached,
                  this::resolveNodeCached);
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
        RelationInfo info =
            buildRelation(
                correlationId,
                found.relation(),
                liveCtx,
                resolutionContext,
                statsProvider,
                timings);
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
      emitPhaseSpans(totalNanos, schedulingNanos, outcome);

      double totalMs = totalNanos / 1_000_000.0;
      if (totalMs >= slowRpcMs) {
        double pinMs = (pinCollectNanos + pinCommitNanos) / 1_000_000.0;
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

    private void emitPhaseSpans(long totalNanos, long schedulingNanos, String outcome) {
      if (!parentSpan.getSpanContext().isValid()) {
        return;
      }
      long cursorNs = 0L;
      cursorNs = emitPhaseSpan(parentSpan, "resolve", 0L, cursorNs, resolveNanos, outcome, "");
      cursorNs =
          emitPhaseSpan(parentSpan, "base_inject", 0L, cursorNs, baseInjectNanos, outcome, "");
      cursorNs =
          emitPhaseSpan(parentSpan, "pin_collect", 0L, cursorNs, pinCollectNanos, outcome, "");
      cursorNs = emitPhaseSpan(parentSpan, "pin_commit", 0L, cursorNs, pinCommitNanos, outcome, "");
      cursorNs =
          emitPhaseSpan(
              parentSpan, "relation_build", 0L, cursorNs, relationBuildNanos, outcome, "");

      long decorationStartNs = cursorNs;
      Span decorationSpan =
          startPhaseSpan(parentSpan, "decoration", 0L, decorationStartNs, outcome, "");
      long decorationCursorNs = 0L;
      decorationCursorNs =
          emitPhaseSpan(
              decorationSpan,
              "decoration_relation",
              decorationStartNs,
              decorationCursorNs,
              timings.decorateRelationNanos(),
              outcome,
              "decoration");
      decorationCursorNs =
          emitPhaseSpan(
              decorationSpan,
              "decoration_view",
              decorationStartNs,
              decorationCursorNs,
              timings.decorateViewNanos(),
              outcome,
              "decoration");
      long columnsStartNs = decorationCursorNs;
      Span decorationColumnsSpan =
          startPhaseSpan(
              decorationSpan,
              "decoration_columns",
              decorationStartNs,
              columnsStartNs,
              outcome,
              "decoration");
      long columnsInvokeNanos = timings.decorateColumnInvokeNanos();
      long columnsPostprocessNanos =
          Math.max(0L, timings.decorateColumnsNanos() - timings.decorateColumnInvokeNanos());
      long columnsCursorNs = 0L;
      columnsCursorNs =
          emitPhaseSpan(
              decorationColumnsSpan,
              "decoration_columns_invoke",
              decorationStartNs + columnsStartNs,
              columnsCursorNs,
              columnsInvokeNanos,
              outcome,
              "decoration_columns");
      columnsCursorNs =
          emitPhaseSpan(
              decorationColumnsSpan,
              "decoration_columns_postprocess",
              decorationStartNs + columnsStartNs,
              columnsCursorNs,
              columnsPostprocessNanos,
              outcome,
              "decoration_columns");
      endPhaseSpan(
          decorationColumnsSpan, decorationStartNs, columnsStartNs, timings.decorateColumnsNanos());
      decorationCursorNs += timings.decorateColumnsNanos();
      long completeStartNs = decorationCursorNs;
      Span completeSpan =
          startPhaseSpan(
              decorationSpan,
              "decoration_complete",
              decorationStartNs,
              completeStartNs,
              outcome,
              "decoration");
      long completeCursorNs = 0L;
      long relationPersistNanos = timings.decoratePersistRelationNanos();
      long columnPersistNanos = timings.decoratePersistColumnsNanos();
      completeCursorNs =
          emitPhaseSpan(
              completeSpan,
              "decoration_persist_relation",
              decorationStartNs + completeStartNs,
              completeCursorNs,
              relationPersistNanos,
              outcome,
              "decoration_complete");
      completeCursorNs =
          emitPhaseSpan(
              completeSpan,
              "decoration_persist_columns",
              decorationStartNs + completeStartNs,
              completeCursorNs,
              columnPersistNanos,
              outcome,
              "decoration_complete");
      long decorationCompleteOtherNanos =
          Math.max(0L, timings.decorateCompleteNanos() - relationPersistNanos - columnPersistNanos);
      completeCursorNs =
          emitPhaseSpan(
              completeSpan,
              "decoration_complete_other",
              decorationStartNs + completeStartNs,
              completeCursorNs,
              decorationCompleteOtherNanos,
              outcome,
              "decoration_complete");
      endPhaseSpan(
          completeSpan, decorationStartNs, completeStartNs, timings.decorateCompleteNanos());
      decorationCursorNs += timings.decorateCompleteNanos();
      endPhaseSpan(decorationSpan, 0L, decorationStartNs, decorationNanos);
      cursorNs += decorationNanos;

      cursorNs =
          emitPhaseSpan(
              parentSpan, "stats_lookup", 0L, cursorNs, timings.statsLookupNanos(), outcome, "");
      cursorNs =
          emitPhaseSpan(parentSpan, "scheduling", 0L, cursorNs, schedulingNanos, outcome, "");

      parentSpan.addEvent(
          "floecat.get_user_objects.summary",
          Attributes.builder()
              .put("query_id", ctx.getQueryId())
              .put("correlation_id", correlationId)
              .put("candidates", resolutionCount)
              .put("chunks", emittedResolutionChunks)
              .put("found", foundCount)
              .put("not_found", notFoundCount)
              .put("decorator_warm_hits", timings.decorateColumnWarmHits())
              .put(
                  "hint_persist_ms",
                  (timings.decoratePersistRelationNanos() + timings.decoratePersistColumnsNanos())
                      / 1_000_000.0)
              .put("outcome", safe(outcome))
              .build());
    }

    private Span startPhaseSpan(
        Span parent,
        String phase,
        long baseOffsetNs,
        long startOffsetNs,
        String outcome,
        String parentPhase) {
      if (parent == null || !parent.getSpanContext().isValid()) {
        return Span.getInvalid();
      }
      long safeBaseOffset = Math.max(0L, baseOffsetNs);
      long safeStartOffset = Math.max(0L, startOffsetNs);
      long startTimestampNs = streamStartEpochNs + safeBaseOffset + safeStartOffset;
      var builder =
          GlobalOpenTelemetry.getTracer("floecat.service")
              .spanBuilder("floecat.get_user_objects.phase." + phase)
              .setParent(Context.root().with(parent))
              .setStartTimestamp(startTimestampNs, TimeUnit.NANOSECONDS)
              .setAttribute("query_id", ctx.getQueryId())
              .setAttribute("correlation_id", correlationId)
              .setAttribute("phase", phase)
              .setAttribute("outcome", safe(outcome));
      if (parentPhase != null && !parentPhase.isBlank()) {
        builder.setAttribute("parent_phase", parentPhase);
      }
      return builder.startSpan();
    }

    private void endPhaseSpan(
        Span phaseSpan, long baseOffsetNs, long startOffsetNs, long elapsedNanos) {
      if (phaseSpan == null || !phaseSpan.getSpanContext().isValid()) {
        return;
      }
      long safeBaseOffset = Math.max(0L, baseOffsetNs);
      long safeStartOffset = Math.max(0L, startOffsetNs);
      long safeElapsed = Math.max(0L, elapsedNanos);
      long endTimestampNs = streamStartEpochNs + safeBaseOffset + safeStartOffset + safeElapsed;
      phaseSpan.setAttribute("elapsed_ns", safeElapsed);
      phaseSpan.setAttribute("elapsed_ms", safeElapsed / 1_000_000.0);
      phaseSpan.end(endTimestampNs, TimeUnit.NANOSECONDS);
    }

    private long emitPhaseSpan(
        Span parent,
        String phase,
        long baseOffsetNs,
        long startOffsetNs,
        long elapsedNanos,
        String outcome,
        String parentPhase) {
      Span phaseSpan =
          startPhaseSpan(parent, phase, baseOffsetNs, startOffsetNs, outcome, parentPhase);
      endPhaseSpan(phaseSpan, baseOffsetNs, startOffsetNs, elapsedNanos);
      return Math.max(0L, startOffsetNs) + Math.max(0L, elapsedNanos);
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
        defaultCatalogName =
            overlay.catalog(defaultCatalogId).map(CatalogNode::displayName).orElse("");
        defaultCatalogResolved = true;
      }
      return defaultCatalogName;
    }

    private String defaultCatalogForDiagnostics() {
      return defaultCatalogResolved ? defaultCatalogName : "";
    }

    private Optional<ResourceId> resolveNameCached(NameRef ref) {
      NormalizedNameRef key = normalizedNameRef(ref);
      return nameResolutionCache.computeIfAbsent(
          key, ignored -> overlay.resolveName(correlationId, ref));
    }

    private Optional<GraphNode> resolveNodeCached(ResourceId id) {
      return nodeResolutionCache.computeIfAbsent(id, overlay::resolve);
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
    private void accumulateChunkPins(SnapshotSet incomingPins) {
      if (incomingPins == null || incomingPins.getPinsCount() == 0) {
        return;
      }
      pendingChunkPins = mergeSnapshotSets(pendingChunkPins, incomingPins);
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
      var updated =
          queryStore.update(
              ctx.getQueryId(),
              existing -> mergeSnapshotSet(existing, pendingChunkPins, correlationId));
      pendingChunkPins = SnapshotSet.getDefaultInstance();
      if (updated.isEmpty()) {
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
