package ai.floedb.metacat.service.query.impl;

import ai.floedb.metacat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.GetTableRequest;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.query.rpc.DescribeInputsRequest;
import ai.floedb.metacat.query.rpc.DescribeInputsResponse;
import ai.floedb.metacat.query.rpc.QuerySchemaService;
import ai.floedb.metacat.query.rpc.SchemaDescriptor;
import ai.floedb.metacat.query.rpc.SnapshotPin;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.common.LogHelper;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.query.QueryContextStore;
import ai.floedb.metacat.service.query.resolver.LogicalSchemaMapper;
import ai.floedb.metacat.service.query.resolver.ObligationsResolver;
import ai.floedb.metacat.service.query.resolver.QueryInputResolver;
import ai.floedb.metacat.service.query.resolver.SnapshotResolver;
import ai.floedb.metacat.service.query.resolver.ViewExpansionResolver;
import io.quarkus.grpc.GrpcClient;
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

  @GrpcClient("metacat")
  TableServiceGrpc.TableServiceBlockingStub tables;

  @GrpcClient("metacat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshots;

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

                    ResourceId rid = pin.getTableId();

                    // Load table metadata
                    Table t =
                        tables
                            .getTable(GetTableRequest.newBuilder().setTableId(rid).build())
                            .getTable();

                    String schemaJson = t.getSchemaJson();

                    // Apply snapshot override if snapshot_id > 0
                    long snapId = pin.getSnapshotId();
                    if (snapId > 0) {

                      var snap =
                          snapshots
                              .getSnapshot(
                                  GetSnapshotRequest.newBuilder()
                                      .setTableId(rid)
                                      .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapId))
                                      .build())
                              .getSnapshot();

                      if (!snap.getSchemaJson().isBlank()) {
                        schemaJson = snap.getSchemaJson();
                      }
                    }

                    SchemaDescriptor desc = schemaMapper.map(t, schemaJson);
                    out.addSchemas(desc);
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
}
