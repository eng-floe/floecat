package ai.floedb.floecat.service.query.impl;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.query.rpc.DescribeInputsRequest;
import ai.floedb.floecat.query.rpc.DescribeInputsResponse;
import ai.floedb.floecat.query.rpc.QuerySchemaService;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.graph.MetadataGraph;
import ai.floedb.floecat.service.query.graph.model.TableNode;
import ai.floedb.floecat.service.query.graph.model.ViewNode;
import ai.floedb.floecat.service.query.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.service.query.resolver.ObligationsResolver;
import ai.floedb.floecat.service.query.resolver.QueryInputResolver;
import ai.floedb.floecat.service.query.resolver.SnapshotResolver;
import ai.floedb.floecat.service.query.resolver.ViewExpansionResolver;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.List;
import org.jboss.logging.Logger;

/**
 * Implements DescribeInputs:
 *
 * <p>- resolves inputs - resolves snapshot pins - loads schemas (in order of inputs) - computes
 * expansions + obligations (stored, not returned)
 *
 * <p>Response: repeated SchemaDescriptor schemas (one per input, in order)
 */
@Singleton
@GrpcService
public class QuerySchemaServiceImpl extends BaseServiceImpl implements QuerySchemaService {

  private static final Logger LOG = Logger.getLogger(QuerySchemaServiceImpl.class);

  @Inject QueryInputResolver inputResolver;
  @Inject SnapshotResolver snapshotResolver;
  @Inject LogicalSchemaMapper schemaMapper;
  @Inject ObligationsResolver obligations;
  @Inject ViewExpansionResolver expansions;
  @Inject QueryContextStore queryStore;
  @Inject MetadataGraph metadataGraph;

  @Override
  public Uni<DescribeInputsResponse> describeInputs(DescribeInputsRequest request) {
    var L = LogHelper.start(LOG, "DescribeInputs");

    return mapFailures(
            run(
                () -> {
                  String queryId = mustNonEmpty(request.getQueryId(), "query_id", correlationId());

                  var ctxOpt = queryStore.get(queryId);
                  if (ctxOpt.isEmpty()) {
                    throw GrpcErrors.notFound(
                        correlationId(), "query.not_found", java.util.Map.of("query_id", queryId));
                  }
                  var ctx = ctxOpt.get();

                  var asOfDefault = ctx.parseAsOfDefault(correlationId());

                  // Resolve inputs → snapshot pins
                  var rr =
                      inputResolver.resolveInputs(
                          correlationId(), request.getInputsList(), asOfDefault);

                  List<SnapshotPin> pins = rr.snapshotSet().getPinsList();

                  DescribeInputsResponse.Builder out = DescribeInputsResponse.newBuilder();

                  for (SnapshotPin pin : pins) {
                    SchemaDescriptor descriptor = schemaForPin(correlationId(), pin);
                    out.addSchemas(descriptor);
                  }

                  // Compute expansions + obligations and store updated context
                  byte[] expansionBytes =
                      expansions.computeExpansion(correlationId(), request.getInputsList());

                  byte[] obligationsBytes = obligations.resolveObligations(correlationId(), pins);

                  var updated =
                      ctx.toBuilder()
                          .snapshotSet(rr.snapshotSet().toByteArray())
                          .expansionMap(expansionBytes)
                          .obligations(obligationsBytes)
                          .version(ctx.getVersion() + 1)
                          .build();

                  queryStore.replace(updated);

                  return out.build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private SnapshotRef snapshotRefFrom(SnapshotPin pin) {
    if (pin.hasSnapshotId()) {
      return SnapshotRef.newBuilder().setSnapshotId(pin.getSnapshotId()).build();
    }
    if (pin.hasAsOf()) {
      return SnapshotRef.newBuilder().setAsOf(pin.getAsOf()).build();
    }
    return null;
  }

  private SchemaDescriptor schemaForPin(String correlationId, SnapshotPin pin) {
    ResourceId rid = pin.getTableId();
    return switch (rid.getKind()) {
      case RK_TABLE -> describeTable(correlationId, rid, pin);
      case RK_VIEW -> describeView(correlationId, rid);
      default ->
          throw GrpcErrors.invalidArgument(
              correlationId, "query.input.invalid", java.util.Map.of("resource_id", rid.getId()));
    };
  }

  private SchemaDescriptor describeTable(String correlationId, ResourceId rid, SnapshotPin pin) {
    TableNode tableNode =
        metadataGraph
            .table(rid)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(
                        correlationId, "table", java.util.Map.of("id", rid.getId())));

    SnapshotRef snapshotRef = snapshotRefFrom(pin);
    String schemaJson = metadataGraph.schemaJsonFor(correlationId, tableNode, snapshotRef);
    return schemaMapper.map(tableNode, schemaJson);
  }

  private SchemaDescriptor describeView(String correlationId, ResourceId rid) {
    ViewNode viewNode =
        metadataGraph
            .view(rid)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(
                        correlationId, "view", java.util.Map.of("id", rid.getId())));

    return SchemaDescriptor.newBuilder().addAllColumns(viewNode.outputColumns()).build();
  }
}
