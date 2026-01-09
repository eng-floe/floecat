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

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeKind;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.CatalogBundleChunk;
import ai.floedb.floecat.query.rpc.CatalogBundleEnd;
import ai.floedb.floecat.query.rpc.CatalogBundleHeader;
import ai.floedb.floecat.query.rpc.ColumnInfo;
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
import ai.floedb.floecat.query.rpc.TableReferenceCandidate;
import ai.floedb.floecat.query.rpc.ViewDefinition;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.query.resolver.QueryInputResolver;
import ai.floedb.floecat.systemcatalog.spi.decorator.ColumnDecoration;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecorator;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecoratorProvider;
import ai.floedb.floecat.systemcatalog.spi.decorator.RelationDecoration;
import ai.floedb.floecat.systemcatalog.spi.scanner.CatalogOverlay;
import ai.floedb.floecat.systemcatalog.spi.scanner.MetadataResolutionContext;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeFormat;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class CatalogBundleService {

  private static final int MAX_RESOLUTIONS_PER_CHUNK = 25;
  private static final Logger LOG = Logger.getLogger(CatalogBundleService.class);

  private final CatalogOverlay overlay;
  private final QueryInputResolver inputResolver;
  private final QueryContextStore queryStore;
  private final EngineMetadataDecoratorProvider decoratorProvider;
  private final EngineContextProvider engineContext;
  private final boolean engineSpecificEnabled;

  @Inject
  public CatalogBundleService(
      CatalogOverlay overlay,
      QueryInputResolver inputResolver,
      QueryContextStore queryStore,
      EngineMetadataDecoratorProvider decoratorProvider,
      EngineContextProvider engineContext,
      @ConfigProperty(name = "floecat.catalog.bundle.emit_engine_specific", defaultValue = "false")
          boolean engineSpecificEnabled) {
    this.overlay = overlay;
    this.inputResolver = inputResolver;
    this.queryStore = queryStore;
    this.decoratorProvider = decoratorProvider;
    this.engineContext = engineContext;
    this.engineSpecificEnabled = engineSpecificEnabled;
  }

  public Multi<CatalogBundleChunk> stream(
      String correlationId, QueryContext ctx, List<TableReferenceCandidate> tables) {
    String defaultCatalog =
        overlay.catalog(ctx.getQueryDefaultCatalogId()).map(CatalogNode::displayName).orElse("");
    List<TableReferenceCandidate> candidates = List.copyOf(tables);
    return Multi.createFrom()
        .<CatalogBundleChunk>deferred(
            () ->
                Multi.createFrom()
                    .iterable(
                        () ->
                            new CatalogBundleIterator(
                                correlationId, ctx, candidates, defaultCatalog)));
  }

  private List<QueryInput> normalizeCandidates(
      String correlationId, TableReferenceCandidate candidate, String defaultCatalog) {
    if (candidate.getCandidatesCount() == 0) {
      throw GrpcErrors.invalidArgument(correlationId, "catalog_bundle.candidate.missing", Map.of());
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
        NameRef adjusted = CatalogBundleUtils.applyDefaultCatalog(name, defaultCatalog);
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
      List<QueryInput> normalizedCandidates) {
    for (QueryInput input : normalizedCandidates) {
      ResourceId relationId = extractResourceId(correlationId, input);
      if (relationId == null) {
        continue;
      }
      Optional<GraphNode> node = overlay.resolve(relationId);
      if (node.isEmpty()) {
        if (input.getTargetCase() == QueryInput.TargetCase.NAME) {
          throw new GraphNodeMissingException(relationId);
        }
        continue;
      }
      return Optional.of(new ResolvedRelation(candidate, relationId, node.get(), input));
    }
    return Optional.empty();
  }

  private ResourceId extractResourceId(String correlationId, QueryInput input) {
    switch (input.getTargetCase()) {
      case TABLE_ID:
        return input.getTableId();
      case VIEW_ID:
        return input.getViewId();
      case NAME:
        try {
          return overlay.resolveName(correlationId, input.getName());
        } catch (StatusRuntimeException e) {
          if (e.getStatus().getCode() == Status.NOT_FOUND.getCode()) {
            return null;
          }
          throw e;
        }
      default:
        return null;
    }
  }

  private SnapshotSet collectSnapshotPins(
      String correlationId, QueryContext ctx, ResolvedRelation relation) {
    QueryInput input = buildCanonicalQueryInput(relation);
    if (input == null) {
      return SnapshotSet.getDefaultInstance();
    }
    var asOfDefault = ctx.parseAsOfDefault(correlationId);
    var resolution = inputResolver.resolveInputs(correlationId, List.of(input), asOfDefault);
    SnapshotSet incoming = resolution.snapshotSet();
    return incoming == null ? SnapshotSet.getDefaultInstance() : incoming;
  }

  private CatalogBundleChunk headerChunk(String queryId, int seq) {
    CatalogBundleHeader header = CatalogBundleHeader.newBuilder().build();
    return CatalogBundleChunk.newBuilder()
        .setQueryId(queryId)
        .setSeq(seq)
        .setHeader(header)
        .build();
  }

  private CatalogBundleChunk resolutionsChunk(
      String queryId, int seq, List<RelationResolution> resolutions) {
    RelationResolutions chunk = RelationResolutions.newBuilder().addAllItems(resolutions).build();
    return CatalogBundleChunk.newBuilder()
        .setQueryId(queryId)
        .setSeq(seq)
        .setResolutions(chunk)
        .build();
  }

  private CatalogBundleChunk endChunk(
      String queryId, int seq, int resolutionCount, int foundCount, int notFoundCount) {
    CatalogBundleEnd end =
        CatalogBundleEnd.newBuilder()
            .setResolutionCount(resolutionCount)
            .setFoundCount(foundCount)
            .setNotFoundCount(notFoundCount)
            .setCustomObjectCount(0)
            .build();
    return CatalogBundleChunk.newBuilder().setQueryId(queryId).setSeq(seq).setEnd(end).build();
  }

  private RelationInfo buildRelation(
      String correlationId, ResolvedRelation relation, QueryContext queryContext) {
    RelationKind kind = mapKind(relation.node().kind(), relation.node().origin());
    Origin origin = mapOrigin(relation.node().origin());
    NameRef name = canonicalName(relation.relationId(), relation.node());
    List<SchemaColumn> schemaColumns =
        relation.node() instanceof ViewNode view
            ? view.outputColumns()
            : overlay.tableSchema(relation.node().id());
    List<SchemaColumn> pruned =
        CatalogBundleUtils.pruneSchema(schemaColumns, relation.candidate(), correlationId);
    List<ColumnInfo> columns =
        CatalogBundleUtils.columnsFor(schemaColumns, pruned, origin, correlationId);
    RelationInfo.Builder builder =
        RelationInfo.newBuilder()
            .setRelationId(relation.relationId())
            .setName(name)
            .setKind(kind)
            .setOrigin(origin);
    List<ColumnInfo> decoratedColumns = columns;
    EngineContext ctx = engineContext.engineContext();
    Optional<EngineMetadataDecorator> decorator = currentDecorator(ctx);
    if (decorator.isPresent()) {
      MetadataResolutionContext resolutionContext =
          MetadataResolutionContext.of(
              overlay,
              Objects.requireNonNull(
                  queryContext.getQueryDefaultCatalogId(), "query default catalog id"),
              ctx);
      RelationDecoration relationDecoration =
          new RelationDecoration(
              builder,
              relation.relationId(),
              relation.node(),
              requireSchema(schemaColumns),
              requireSchema(pruned),
              resolutionContext);
      try {
        decorator
            .get()
            .decorateRelation(ctx.normalizedKind(), ctx.normalizedVersion(), relationDecoration);
      } catch (RuntimeException e) {
        LOG.debugf(
            e,
            "Decorator threw while decorating relation %s (engine=%s)",
            relation.relationId(),
            ctx.normalizedKind());
      }
      decoratedColumns = decorateColumns(columns, pruned, relationDecoration, decorator.get(), ctx);
    }
    builder.addAllColumns(decoratedColumns);
    if (relation.node() instanceof ViewNode view) {
      builder.setViewDefinition(viewDefinition(view));
    }
    return builder.build();
  }

  private List<ColumnInfo> decorateColumns(
      List<ColumnInfo> columns,
      List<SchemaColumn> pruned,
      RelationDecoration relationDecoration,
      EngineMetadataDecorator decorator,
      EngineContext ctx) {

    if (pruned == null || pruned.size() != columns.size()) {
      LOG.debugf(
          "Skip engine metadata: column count mismatch columns=%d pruned=%s",
          columns.size(), pruned == null ? "null" : Integer.toString(pruned.size()));
      return columns;
    }

    List<ColumnInfo> decorated = new ArrayList<>(columns.size());
    for (int i = 0; i < columns.size(); i++) {
      ColumnInfo column = columns.get(i);
      SchemaColumn schema = pruned.get(i);
      ColumnInfo.Builder builder = column.toBuilder();
      LogicalType logicalType = parseLogicalType(schema);
      ColumnDecoration columnDecoration =
          new ColumnDecoration(
              builder, schema, logicalType, column.getOrdinal(), relationDecoration);
      try {
        decorator.decorateColumn(ctx.normalizedKind(), ctx.normalizedVersion(), columnDecoration);
      } catch (RuntimeException e) {
        LOG.debugf(
            e,
            "Decorator threw while decorating column %s.%s (engine=%s)",
            relationDecoration.relationId(),
            column.getName(),
            ctx.normalizedKind());
      }
      decorated.add(columnDecoration.builder().build());
    }
    return decorated;
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
    if (!engineSpecificEnabled || ctx == null || !ctx.hasEngineKind()) {
      return Optional.empty();
    }
    return decoratorProvider.decorator(ctx);
  }

  private static List<SchemaColumn> requireSchema(List<SchemaColumn> schema) {
    if (schema == null) {
      return List.of();
    }
    return List.copyOf(schema);
  }

  private QueryInput buildCanonicalQueryInput(ResolvedRelation relation) {
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

  private ViewDefinition viewDefinition(ViewNode view) {
    ViewDefinition.Builder builder =
        ViewDefinition.newBuilder().setCanonicalSql(view.sql()).setDialect(view.dialect());
    builder.addAllBaseRelations(view.baseRelations());
    builder.addAllCreationSearchPath(view.creationSearchPath());
    return builder.build();
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
    byte[] payload = ctx.getSnapshotSet();
    if (payload == null || payload.length == 0) {
      return SnapshotSet.getDefaultInstance();
    }
    try {
      return SnapshotSet.parseFrom(payload);
    } catch (InvalidProtocolBufferException e) {
      throw GrpcErrors.internal(
          correlationId,
          "catalog_bundle.snapshot.parse_failed",
          Map.of("query_id", ctx.getQueryId(), "error", e.getMessage()));
    }
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
      merged.merge(pinKey(pin.getTableId()), pin, CatalogBundleService::mergePin);
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
      GraphNode node,
      QueryInput selectedInput) {}

  private static final class GraphNodeMissingException extends RuntimeException {
    private final ResourceId relationId;

    private GraphNodeMissingException(ResourceId relationId) {
      this.relationId = relationId;
    }

    private ResourceId relationId() {
      return relationId;
    }
  }

  private final class CatalogBundleIterator implements Iterator<CatalogBundleChunk> {

    private final String correlationId;
    private final QueryContext ctx;
    private final List<TableReferenceCandidate> tables;
    private final int resolutionCount;
    private final String defaultCatalog;

    private final List<RelationResolution> pending = new ArrayList<>(MAX_RESOLUTIONS_PER_CHUNK);
    private SnapshotSet pendingChunkPins = SnapshotSet.getDefaultInstance();

    private int seq = 1;
    private int nextInputIndex = 0;
    private int foundCount = 0;
    private int notFoundCount = 0;
    private boolean headerEmitted = false;
    private boolean endEmitted = false;

    CatalogBundleIterator(
        String correlationId,
        QueryContext ctx,
        List<TableReferenceCandidate> tables,
        String defaultCatalog) {
      this.correlationId = correlationId;
      this.ctx = ctx;
      this.tables = tables;
      this.resolutionCount = tables.size();
      this.defaultCatalog = defaultCatalog;
    }

    @Override
    public boolean hasNext() {
      return !endEmitted;
    }

    @Override
    public CatalogBundleChunk next() {
      if (!headerEmitted) {
        headerEmitted = true;
        return headerChunk(ctx.getQueryId(), seq++);
      }

      if (pending.isEmpty() && nextInputIndex < resolutionCount) {
        fillPending();
      }

      if (!pending.isEmpty()) {
        return flushResolutionChunk();
      }

      if (!endEmitted) {
        endEmitted = true;
        return endChunk(ctx.getQueryId(), seq++, resolutionCount, foundCount, notFoundCount);
      }

      throw new NoSuchElementException();
    }

    private void fillPending() {
      while (nextInputIndex < resolutionCount && pending.size() < MAX_RESOLUTIONS_PER_CHUNK) {
        pending.add(resolveNextResolution());
      }
    }

    private RelationResolution resolveNextResolution() {
      TableReferenceCandidate candidate = tables.get(nextInputIndex);
      int inputIndex = nextInputIndex;
      nextInputIndex++;
      List<QueryInput> normalized = normalizeCandidates(correlationId, candidate, defaultCatalog);
      try {
        Optional<ResolvedRelation> resolved =
            selectResolvedRelation(correlationId, candidate, normalized);
        if (resolved.isPresent()) {
          ResolvedRelation relation = resolved.get();
          SnapshotSet pins = collectSnapshotPins(correlationId, ctx, relation);
          accumulateChunkPins(pins);
          RelationInfo info = buildRelation(correlationId, relation, ctx);
          foundCount++;
          return RelationResolution.newBuilder()
              .setInputIndex(inputIndex)
              .setStatus(ResolutionStatus.RESOLUTION_STATUS_FOUND)
              .setRelation(info)
              .build();
        }
      } catch (GraphNodeMissingException e) {
        ResolutionFailure failure =
            ResolutionFailure.newBuilder()
                .setCode("catalog_bundle.graph.missing_node")
                .setMessage("relation resolved but missing from graph")
                .putDetails("resource_id", e.relationId().getId())
                .putDetails("default_catalog", defaultCatalog)
                .addAllAttempted(normalized)
                .build();
        return RelationResolution.newBuilder()
            .setInputIndex(inputIndex)
            .setStatus(ResolutionStatus.RESOLUTION_STATUS_ERROR)
            .setFailure(failure)
            .build();
      }
      notFoundCount++;
      ResolutionFailure failure =
          ResolutionFailure.newBuilder()
              .setCode("catalog_bundle.relation_not_found")
              .setMessage("relation not found")
              .putDetails("candidate_count", Integer.toString(normalized.size()))
              .putDetails("default_catalog", defaultCatalog)
              .addAllAttempted(normalized)
              .build();
      return RelationResolution.newBuilder()
          .setInputIndex(inputIndex)
          .setStatus(ResolutionStatus.RESOLUTION_STATUS_NOT_FOUND)
          .setFailure(failure)
          .build();
    }

    private CatalogBundleChunk flushResolutionChunk() {
      List<RelationResolution> chunkItems = new ArrayList<>(pending);
      pending.clear();
      commitChunkPins();
      return resolutionsChunk(ctx.getQueryId(), seq++, chunkItems);
    }

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
      var updated =
          queryStore.update(
              ctx.getQueryId(),
              existing -> mergeSnapshotSet(existing, pendingChunkPins, correlationId));
      pendingChunkPins = SnapshotSet.getDefaultInstance();
      if (updated.isEmpty()) {
        throw GrpcErrors.notFound(
            correlationId, "query.not_found", Map.of("query_id", ctx.getQueryId()));
      }
    }
  }
}
