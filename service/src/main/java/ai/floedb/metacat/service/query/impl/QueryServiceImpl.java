package ai.floedb.metacat.service.query.impl;

import ai.floedb.metacat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.metacat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.ResolveTableRequest;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.common.rpc.SpecialSnapshot;
import ai.floedb.metacat.query.rpc.BeginQueryRequest;
import ai.floedb.metacat.query.rpc.BeginQueryResponse;
import ai.floedb.metacat.query.rpc.EndQueryRequest;
import ai.floedb.metacat.query.rpc.EndQueryResponse;
import ai.floedb.metacat.query.rpc.ExpansionMap;
import ai.floedb.metacat.query.rpc.GetQueryRequest;
import ai.floedb.metacat.query.rpc.GetQueryResponse;
import ai.floedb.metacat.query.rpc.QueryDescriptor;
import ai.floedb.metacat.query.rpc.QueryService;
import ai.floedb.metacat.query.rpc.QueryServiceGrpc;
import ai.floedb.metacat.query.rpc.RenewQueryRequest;
import ai.floedb.metacat.query.rpc.RenewQueryResponse;
import ai.floedb.metacat.query.rpc.SnapshotPin;
import ai.floedb.metacat.query.rpc.SnapshotSet;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.common.LogHelper;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.query.QueryContextStore;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@GrpcService
public class QueryServiceImpl extends BaseServiceImpl implements QueryService {
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;

  @GrpcClient("metacat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @GrpcClient("metacat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @GrpcClient("metacat")
  TableServiceGrpc.TableServiceBlockingStub tables;

  @GrpcClient("metacat")
  TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub stats;

  @Inject QueryContextStore queryStore;

  @Inject
  @ConfigProperty(name = "metacat.query.default-ttl-ms", defaultValue = "60000")
  long defaultTtlMs;

  private static final Logger LOG = Logger.getLogger(QueryServiceGrpc.class);

  @Override
  public Uni<BeginQueryResponse> beginQuery(BeginQueryRequest request) {
    var L = LogHelper.start(LOG, "BeginQuery");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  var correlationId = principalContext.getCorrelationId();

                  authz.require(principalContext, "catalog.read");

                  if (request.getInputsCount() == 0) {
                    throw GrpcErrors.invalidArgument(
                        correlationId, "query.inputs.required", Map.of());
                  }

                  final long ttlMs =
                      (request.getTtlSeconds() > 0
                              ? request.getTtlSeconds()
                              : (int) (defaultTtlMs / 1000))
                          * 1000L;

                  final Optional<Timestamp> asOfDefault =
                      request.hasAsOfDefault()
                          ? Optional.of(request.getAsOfDefault())
                          : Optional.empty();

                  final List<Resolved> resolvedInputs = new ArrayList<>();
                  for (var in : request.getInputsList()) {
                    switch (in.getTargetCase()) {
                      case NAME -> {
                        var nr = in.getName();
                        checkNameRef(nr);

                        ResourceId rid;
                        boolean isTable;
                        if (nr.getName() != null && !nr.getName().isBlank()) {
                          rid =
                              directory
                                  .resolveTable(ResolveTableRequest.newBuilder().setRef(nr).build())
                                  .getResourceId();
                          isTable = true;
                        } else {
                          rid =
                              directory
                                  .resolveNamespace(
                                      ResolveNamespaceRequest.newBuilder().setRef(nr).build())
                                  .getResourceId();
                          isTable = false;
                        }
                        long snapId =
                            computeSnapshotPin(in.getSnapshot(), asOfDefault, isTable, rid);
                        resolvedInputs.add(new Resolved(rid, isTable, snapId));
                      }
                      case TABLE_ID -> {
                        var rid = in.getTableId();
                        ensureKind(rid, ResourceKind.RK_TABLE, "table_id", correlationId);
                        long snapId = computeSnapshotPin(in.getSnapshot(), asOfDefault, true, rid);
                        resolvedInputs.add(new Resolved(rid, true, snapId));
                      }
                      case VIEW_ID -> {
                        var rid = in.getViewId();
                        ensureKind(rid, ResourceKind.RK_OVERLAY, "view_id", correlationId);
                        resolvedInputs.add(new Resolved(rid, false, 0L));
                      }

                      default ->
                          throw GrpcErrors.invalidArgument(
                              correlationId, "query.target.required", Map.of());
                    }
                  }

                  var expansion = ExpansionMap.newBuilder().build();

                  var snapshots = SnapshotSet.newBuilder();
                  for (var input : resolvedInputs) {
                    if (input.isTable && input.snapshotId > 0) {
                      snapshots.addPins(
                          SnapshotPin.newBuilder()
                              .setTableId(input.rid)
                              .setSnapshotId(input.snapshotId));
                    } else if (input.isTable && input.snapshotId == 0 && asOfDefault.isPresent()) {
                      snapshots.addPins(
                          SnapshotPin.newBuilder()
                              .setTableId(input.rid)
                              .setAsOf(asOfDefault.get()));
                    }
                  }

                  String queryId = UUID.randomUUID().toString();
                  byte[] expansionBytes = expansion.toByteArray();
                  byte[] snapshotBytes = snapshots.build().toByteArray();

                  var queryContext =
                      QueryContext.newActive(
                          queryId, principalContext, expansionBytes, snapshotBytes, ttlMs, 1L);
                  queryStore.put(queryContext);

                  var scanBundle = queryContext.fetchScanBundle(tables, stats);

                  try {
                    return BeginQueryResponse.newBuilder()
                        .setQuery(
                            QueryDescriptor.newBuilder()
                                .setQueryId(queryId)
                                .setTenantId(principalContext.getTenantId())
                                .setQueryStatus(queryContext.getQueryStatus())
                                .setCreatedAt(ts(queryContext.getCreatedAtMs()))
                                .setExpiresAt(ts(queryContext.getExpiresAtMs()))
                                .setSnapshots(SnapshotSet.parseFrom(snapshotBytes))
                                .setExpansion(ExpansionMap.parseFrom(expansionBytes))
                                .addAllDataFiles(
                                    scanBundle != null ? scanBundle.dataFiles() : List.of())
                                .addAllDeleteFiles(
                                    scanBundle != null ? scanBundle.deleteFiles() : List.of()))
                        .build();
                  } catch (InvalidProtocolBufferException e) {
                    throw GrpcErrors.internal(
                        correlationId, "query.expansion.parse_failed", Map.of("query_id", queryId));
                  }
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<RenewQueryResponse> renewQuery(RenewQueryRequest request) {
    var L = LogHelper.start(LOG, "RenewQuery");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  var correlationId = principalContext.getCorrelationId();

                  authz.require(principalContext, "catalog.read");

                  String queryId = mustNonEmpty(request.getQueryId(), "query_id", correlationId);
                  final long ttlMs =
                      (request.getTtlSeconds() > 0
                              ? request.getTtlSeconds()
                              : (int) (defaultTtlMs / 1000))
                          * 1000L;
                  final long requestedExp = clock.millis() + ttlMs;

                  var updated = queryStore.extendLease(queryId, requestedExp);
                  if (updated.isEmpty()) {
                    throw GrpcErrors.notFound(
                        correlationId, "query.not_found", Map.of("query_id", queryId));
                  }
                  return RenewQueryResponse.newBuilder()
                      .setQueryId(queryId)
                      .setExpiresAt(ts(updated.get().getExpiresAtMs()))
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<EndQueryResponse> endQuery(EndQueryRequest request) {
    var L = LogHelper.start(LOG, "EndQuery");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  var correlationId = principalContext.getCorrelationId();

                  authz.require(principalContext, "catalog.read");

                  String queryId = mustNonEmpty(request.getQueryId(), "query_id", correlationId);
                  var ended = queryStore.end(queryId, request.getCommit());
                  if (ended.isEmpty()) {
                    throw GrpcErrors.notFound(
                        correlationId, "query.not_found", Map.of("query_id", queryId));
                  }
                  return EndQueryResponse.newBuilder().setQueryId(queryId).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<GetQueryResponse> getQuery(GetQueryRequest request) {
    var L = LogHelper.start(LOG, "GetQuery");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  var correlationId = principalContext.getCorrelationId();
                  authz.require(principalContext, "catalog.read");

                  String queryId = mustNonEmpty(request.getQueryId(), "query_id", correlationId);
                  var queryContextOpt = queryStore.get(queryId);
                  if (queryContextOpt.isEmpty()) {
                    throw GrpcErrors.notFound(
                        correlationId, "query.not_found", Map.of("query_id", queryId));
                  }
                  var queryContext = queryContextOpt.get();

                  var queryDescriptor =
                      QueryDescriptor.newBuilder()
                          .setQueryId(queryContext.getQueryId())
                          .setTenantId(principalContext.getTenantId())
                          .setQueryStatus(queryContext.getQueryStatus())
                          .setCreatedAt(ts(queryContext.getCreatedAtMs()))
                          .setExpiresAt(ts(queryContext.getExpiresAtMs()));

                  if (queryContext.getSnapshotSet() != null) {
                    try {
                      queryDescriptor.setSnapshots(
                          SnapshotSet.parseFrom(queryContext.getSnapshotSet()));
                    } catch (InvalidProtocolBufferException e) {
                      throw GrpcErrors.internal(
                          correlationId,
                          "query.snapshot.parse_failed",
                          Map.of("query_id", queryId));
                    }
                  }
                  if (queryContext.getExpansionMap() != null) {
                    try {
                      queryDescriptor.setExpansion(
                          ExpansionMap.parseFrom(queryContext.getExpansionMap()));
                    } catch (InvalidProtocolBufferException e) {
                      throw GrpcErrors.internal(
                          correlationId,
                          "query.expansion.parse_failed",
                          Map.of("query_id", queryId));
                    }
                  }

                  return GetQueryResponse.newBuilder().setQuery(queryDescriptor.build()).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private static Timestamp ts(long millis) {
    long s = Math.floorDiv(millis, 1000);
    int n = (int) ((millis % 1000) * 1_000_000);
    return Timestamp.newBuilder().setSeconds(s).setNanos(n).build();
  }

  private void checkNameRef(NameRef nameRef) {
    if (nameRef.getCatalog() == null || nameRef.getCatalog().isBlank()) {
      throw GrpcErrors.invalidArgument(correlationId(), "catalog.missing", Map.of());
    }
    for (String pathSegment : nameRef.getPathList()) {
      if (pathSegment == null || pathSegment.isBlank()) {
        throw GrpcErrors.invalidArgument(correlationId(), "path.segment.blank", Map.of());
      }
    }
  }

  private long computeSnapshotPin(
      SnapshotRef snapshotRef,
      Optional<Timestamp> asOfDefault,
      boolean isTable,
      ResourceId tableId) {

    if (!isTable) {
      return 0L;
    }

    if (snapshotRef == null || snapshotRef.getWhichCase() == SnapshotRef.WhichCase.WHICH_NOT_SET) {
      var cur =
          snapshot.getSnapshot(
              GetSnapshotRequest.newBuilder()
                  .setTableId(tableId)
                  .setSnapshot(
                      SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build())
                  .build());
      return cur.hasSnapshot() ? cur.getSnapshot().getSnapshotId() : 0L;
    }

    return snapshot
        .getSnapshot(
            GetSnapshotRequest.newBuilder().setTableId(tableId).setSnapshot(snapshotRef).build())
        .getSnapshot()
        .getSnapshotId();
  }

  private record Resolved(ResourceId rid, boolean isTable, long snapshotId) {}
}
