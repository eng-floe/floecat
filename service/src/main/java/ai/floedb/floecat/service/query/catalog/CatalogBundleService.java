package ai.floedb.floecat.service.query.catalog;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeKind;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.CatalogBundleChunk;
import ai.floedb.floecat.query.rpc.CatalogBundleEnd;
import ai.floedb.floecat.query.rpc.CatalogBundleHeader;
import ai.floedb.floecat.query.rpc.Origin;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.RelationKind;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.TableReferenceCandidate;
import ai.floedb.floecat.query.rpc.ViewDefinition;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.systemcatalog.spi.scanner.CatalogOverlay;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class CatalogBundleService {

  private final CatalogOverlay overlay;

  public CatalogBundleService(CatalogOverlay overlay) {
    this.overlay = overlay;
  }

  public Multi<CatalogBundleChunk> stream(
      String correlationId, QueryContext ctx, List<TableReferenceCandidate> tables) {
    String defaultCatalog =
        overlay.catalog(ctx.getQueryDefaultCatalogId()).map(CatalogNode::displayName).orElse("");
    return Multi.createFrom()
        .iterable(buildChunks(ctx.getQueryId(), correlationId, tables, defaultCatalog));
  }

  private List<CatalogBundleChunk> buildChunks(
      String queryId,
      String correlationId,
      List<TableReferenceCandidate> tables,
      String defaultCatalog) {
    List<CatalogBundleChunk> chunks = new ArrayList<>();
    int seq = 1;
    chunks.add(headerChunk(queryId, seq++));

    int relationCount = 0;
    for (TableReferenceCandidate candidate : tables) {
      RelationInfo relation = buildRelation(correlationId, candidate, defaultCatalog);
      chunks.add(relationChunk(queryId, seq++, relation));
      relationCount++;
    }

    chunks.add(endChunk(queryId, seq, relationCount));
    return chunks;
  }

  private CatalogBundleChunk headerChunk(String queryId, int seq) {
    CatalogBundleHeader header = CatalogBundleHeader.newBuilder().build();
    return CatalogBundleChunk.newBuilder()
        .setQueryId(queryId)
        .setSeq(seq)
        .setHeader(header)
        .build();
  }

  private CatalogBundleChunk relationChunk(String queryId, int seq, RelationInfo relation) {
    return CatalogBundleChunk.newBuilder()
        .setQueryId(queryId)
        .setSeq(seq)
        .setRelation(relation)
        .build();
  }

  private CatalogBundleChunk endChunk(String queryId, int seq, int relationCount) {
    CatalogBundleEnd end =
        CatalogBundleEnd.newBuilder()
            .setRelationCount(relationCount)
            .setCustomObjectCount(0)
            .build();
    return CatalogBundleChunk.newBuilder().setQueryId(queryId).setSeq(seq).setEnd(end).build();
  }

  private RelationInfo buildRelation(
      String correlationId, TableReferenceCandidate candidate, String defaultCatalog) {
    ResourceId resolved = resolveCandidate(correlationId, candidate, defaultCatalog);
    GraphNode node =
        overlay
            .resolve(resolved)
            .orElseThrow(
                () ->
                    GrpcErrors.internal(
                        correlationId,
                        "catalog_bundle.graph.missing_node",
                        Map.of(
                            "resource_id", resolved.getId(),
                            "kind", resolved.getKind().name())));

    RelationKind kind = mapKind(node.kind(), node.origin());
    Origin origin = mapOrigin(node.origin());
    NameRef name = canonicalName(resolved, node);

    List<SchemaColumn> schemaColumns =
        node instanceof ViewNode view ? view.outputColumns() : overlay.tableSchema(node.id());
    List<SchemaColumn> pruned =
        CatalogBundleUtils.pruneSchema(schemaColumns, candidate, correlationId);

    RelationInfo.Builder builder =
        RelationInfo.newBuilder()
            .setRelationId(resolved)
            .setName(name)
            .setKind(kind)
            .setOrigin(origin)
            .addAllColumns(
                CatalogBundleUtils.columnsFor(schemaColumns, pruned, origin, correlationId));

    if (node instanceof ViewNode view) {
      builder.setViewDefinition(viewDefinition(view));
    }

    return builder.build();
  }

  private ResourceId resolveCandidate(
      String correlationId, TableReferenceCandidate candidate, String defaultCatalog) {
    if (candidate.getFqCandidatesCount() == 0) {
      throw GrpcErrors.invalidArgument(correlationId, "catalog_bundle.candidate.missing", Map.of());
    }
    for (String fq : candidate.getFqCandidatesList()) {
      NameRef ref = CatalogBundleUtils.parseNameRef(fq, defaultCatalog, correlationId);
      try {
        return overlay.resolveName(correlationId, ref);
      } catch (StatusRuntimeException e) {
        if (e.getStatus().getCode() == Status.NOT_FOUND.getCode()) {
          continue;
        }
        throw e;
      }
    }
    throw GrpcErrors.notFound(
        correlationId,
        "catalog_bundle.relation_not_found",
        Map.of(
            "candidate_count",
            Integer.toString(candidate.getFqCandidatesCount()),
            "default_catalog",
            defaultCatalog));
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
}
