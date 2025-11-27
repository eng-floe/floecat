package ai.floedb.metacat.service.query.impl;

import ai.floedb.metacat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.metacat.catalog.rpc.GetSchemaRequest;
import ai.floedb.metacat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.ResolveTableRequest;
import ai.floedb.metacat.catalog.rpc.SchemaServiceGrpc;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.common.rpc.SpecialSnapshot;
import ai.floedb.metacat.execution.rpc.ScanBundle;
import ai.floedb.metacat.query.rpc.BeginQueryRequest;
import ai.floedb.metacat.query.rpc.BeginQueryResponse;
import ai.floedb.metacat.query.rpc.EndQueryRequest;
import ai.floedb.metacat.query.rpc.EndQueryResponse;
import ai.floedb.metacat.query.rpc.ExpansionMap;
import ai.floedb.metacat.query.rpc.FetchScanBundleRequest;
import ai.floedb.metacat.query.rpc.FetchScanBundleResponse;
import ai.floedb.metacat.query.rpc.GetQueryRequest;
import ai.floedb.metacat.query.rpc.GetQueryResponse;
import ai.floedb.metacat.query.rpc.QueryDescriptor;
import ai.floedb.metacat.query.rpc.QueryService;
import ai.floedb.metacat.query.rpc.QueryServiceGrpc;
import ai.floedb.metacat.query.rpc.RenewQueryRequest;
import ai.floedb.metacat.query.rpc.RenewQueryResponse;
import ai.floedb.metacat.query.rpc.SchemaDescriptor;
import ai.floedb.metacat.query.rpc.SnapshotPin;
import ai.floedb.metacat.query.rpc.SnapshotSet;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.common.LogHelper;
import ai.floedb.metacat.service.common.ScanPruningUtils;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.execution.impl.ScanBundleService;
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
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * gRPC implementation of the {@link QueryService} API.
 *
 * <p>This service manages the lifecycle of an analytical query, including:
 *
 * <ul>
 *   <li>creating and renewing a query context,
 *   <li>snapshot pinning for all referenced tables,
 *   <li>optional schema resolution at query start,
 *   <li>exposing a dedicated RPC for retrieving and pruning scan bundles.
 * </ul>
 *
 * <p>Execution is intentionally not performed here; the service focuses on metadata handling and
 * request coordination.
 */
@GrpcService
public class QueryServiceImpl extends BaseServiceImpl implements QueryService {

  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;

  @GrpcClient("metacat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @GrpcClient("metacat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @GrpcClient("metacat")
  SchemaServiceGrpc.SchemaServiceBlockingStub schemas;

  @Inject QueryContextStore queryStore;
  @Inject ScanBundleService scanBundles;

  @Inject
  @ConfigProperty(name = "metacat.query.default-ttl-ms", defaultValue = "60000")
  long defaultTtlMs;

  private static final Logger LOG = Logger.getLogger(QueryServiceGrpc.class);

  /**
   * -------------------------------------------------------------------- BeginQuery
   *
   * <p>Creates a new query context and resolves all referenced inputs. Snapshot selection still
   * occurs here, as well as optional schema retrieval. The actual file listing (scan bundle) has
   * been moved to {@code fetchScanBundle}.
   * -------------------------------------------------------------------
   */
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

                  // Resolve all referenced tables and views.
                  var resolved = new ArrayList<Resolved>();
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
                        resolved.add(new Resolved(rid, isTable, snapId));
                      }
                      case TABLE_ID -> {
                        var rid = in.getTableId();
                        ensureKind(rid, ResourceKind.RK_TABLE, "table_id", correlationId);
                        long snapId = computeSnapshotPin(in.getSnapshot(), asOfDefault, true, rid);
                        resolved.add(new Resolved(rid, true, snapId));
                      }
                      case VIEW_ID -> {
                        var rid = in.getViewId();
                        ensureKind(rid, ResourceKind.RK_OVERLAY, "view_id", correlationId);
                        resolved.add(new Resolved(rid, false, 0L));
                      }
                      default ->
                          throw GrpcErrors.invalidArgument(
                              correlationId, "query.target.required", Map.of());
                    }
                  }

                  var expansion = ExpansionMap.newBuilder().build();
                  var snapshots = SnapshotSet.newBuilder();

                  for (var r : resolved) {
                    if (r.isTable() && r.snapshotId() > 0) {
                      snapshots.addPins(
                          SnapshotPin.newBuilder()
                              .setTableId(r.rid())
                              .setSnapshotId(r.snapshotId()));
                    } else if (r.isTable() && r.snapshotId() == 0 && asOfDefault.isPresent()) {
                      snapshots.addPins(
                          SnapshotPin.newBuilder().setTableId(r.rid()).setAsOf(asOfDefault.get()));
                    }
                  }

                  String queryId = UUID.randomUUID().toString();
                  byte[] expansionBytes = expansion.toByteArray();
                  byte[] snapshotBytes = snapshots.build().toByteArray();

                  var queryContext =
                      QueryContext.newActive(
                          queryId, principalContext, expansionBytes, snapshotBytes, ttlMs, 1L);
                  queryStore.put(queryContext);

                  SchemaDescriptor schema = SchemaDescriptor.getDefaultInstance();

                  // Optional schema retrieval for the first pinned table.
                  if (request.getIncludeSchema() && snapshots.getPinsCount() > 0) {
                    var pin = snapshots.getPins(0);
                    var schemaReq =
                        GetSchemaRequest.newBuilder()
                            .setTableId(pin.getTableId())
                            .setSnapshot(
                                pin.getSnapshotId() > 0
                                    ? SnapshotRef.newBuilder().setSnapshotId(pin.getSnapshotId())
                                    : (pin.hasAsOf()
                                        ? SnapshotRef.newBuilder().setAsOf(pin.getAsOf())
                                        : SnapshotRef.newBuilder()
                                            .setSpecial(SpecialSnapshot.SS_CURRENT)))
                            .build();
                    schema = schemas.getSchema(schemaReq).getSchema();
                  }

                  try {
                    QueryDescriptor descriptor =
                        QueryDescriptor.newBuilder()
                            .setQueryId(queryId)
                            .setTenantId(principalContext.getTenantId())
                            .setQueryStatus(queryContext.getQueryStatus())
                            .setCreatedAt(ts(queryContext.getCreatedAtMs()))
                            .setExpiresAt(ts(queryContext.getExpiresAtMs()))
                            .setSnapshots(SnapshotSet.parseFrom(snapshotBytes))
                            .setExpansion(ExpansionMap.parseFrom(expansionBytes))
                            .build();

                    return BeginQueryResponse.newBuilder()
                        .setQuery(descriptor)
                        .setSchema(schema)
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

  /**
   * -------------------------------------------------------------------- RenewQuery
   *
   * <p>Extends the TTL of an existing active query. Expires the query if renewal occurs after its
   * TTL has elapsed. -------------------------------------------------------------------
   */
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

                  long requestedExp = clock.millis() + ttlMs;

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

  /**
   * -------------------------------------------------------------------- EndQuery
   *
   * <p>Marks the query as committed or aborted and applies a grace period before cleanup. No
   * further renewal is possible after this point.
   * -------------------------------------------------------------------
   */
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

  /**
   * -------------------------------------------------------------------- GetQuery
   *
   * <p>Returns metadata for an existing query context, including TTL, status, snapshot pins, and
   * expansion map. -------------------------------------------------------------------
   */
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

                  var builder =
                      QueryDescriptor.newBuilder()
                          .setQueryId(queryContext.getQueryId())
                          .setTenantId(principalContext.getTenantId())
                          .setQueryStatus(queryContext.getQueryStatus())
                          .setCreatedAt(ts(queryContext.getCreatedAtMs()))
                          .setExpiresAt(ts(queryContext.getExpiresAtMs()));

                  if (queryContext.getSnapshotSet() != null) {
                    try {
                      builder.setSnapshots(SnapshotSet.parseFrom(queryContext.getSnapshotSet()));
                    } catch (InvalidProtocolBufferException e) {
                      throw GrpcErrors.internal(
                          correlationId,
                          "query.snapshot.parse_failed",
                          Map.of("query_id", queryId));
                    }
                  }

                  if (queryContext.getExpansionMap() != null) {
                    try {
                      builder.setExpansion(ExpansionMap.parseFrom(queryContext.getExpansionMap()));
                    } catch (InvalidProtocolBufferException e) {
                      throw GrpcErrors.internal(
                          correlationId,
                          "query.expansion.parse_failed",
                          Map.of("query_id", queryId));
                    }
                  }

                  return GetQueryResponse.newBuilder().setQuery(builder.build()).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  /**
   * -------------------------------------------------------------------- FetchScanBundle
   *
   * <p>Retrieves the list of data/delete files for a pinned table and applies pruning based on
   * projection and predicate information. This replaces file listing previously done during
   * BeginQuery. -------------------------------------------------------------------
   */
  @Override
  public Uni<FetchScanBundleResponse> fetchScanBundle(FetchScanBundleRequest request) {
    var L = LogHelper.start(LOG, "FetchScanBundle");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  var correlationId = principalContext.getCorrelationId();

                  authz.require(principalContext, "catalog.read");

                  String queryId = mustNonEmpty(request.getQueryId(), "query_id", correlationId);

                  if (!request.hasTableId()) {
                    throw GrpcErrors.invalidArgument(
                        correlationId, "query.table_id.required", Map.of("query_id", queryId));
                  }

                  var ctxOpt = queryStore.get(queryId);
                  if (ctxOpt.isEmpty()) {
                    throw GrpcErrors.notFound(
                        correlationId, "query.not_found", Map.of("query_id", queryId));
                  }
                  var ctx = ctxOpt.get();

                  var pin = ctx.requireSnapshotPin(request.getTableId(), correlationId);

                  try {
                    var raw = scanBundles.fetch(correlationId, request.getTableId(), pin);

                    ScanBundle pruned =
                        ScanPruningUtils.pruneBundle(
                            raw, request.getRequiredColumnsList(), request.getPredicatesList());

                    ctx.markPlanningCompleted();

                    return FetchScanBundleResponse.newBuilder().setBundle(pruned).build();

                  } catch (RuntimeException e) {
                    ctx.markPlanningFailed();
                    throw e;
                  }
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  /** Converts epoch milliseconds into a protobuf {@link Timestamp}. */
  private static Timestamp ts(long millis) {
    long s = Math.floorDiv(millis, 1000);
    int n = (int) ((millis % 1000) * 1_000_000);
    return Timestamp.newBuilder().setSeconds(s).setNanos(n).build();
  }

  /** Validates that a {@link NameRef} has all required components. */
  private void checkNameRef(NameRef nr) {
    if (nr.getCatalog() == null || nr.getCatalog().isBlank()) {
      throw GrpcErrors.invalidArgument(correlationId(), "catalog.missing", Map.of());
    }
    for (String p : nr.getPathList()) {
      if (p == null || p.isBlank()) {
        throw GrpcErrors.invalidArgument(correlationId(), "path.segment.blank", Map.of());
      }
    }
  }

  /**
   * Resolves the correct snapshot ID for a table reference, applying per-input snapshot overrides
   * or the query-level "as of" default.
   */
  private long computeSnapshotPin(
      SnapshotRef ref, Optional<Timestamp> asOfDefault, boolean isTable, ResourceId tableId) {

    if (!isTable) {
      return 0L;
    }

    if (ref == null || ref.getWhichCase() == SnapshotRef.WhichCase.WHICH_NOT_SET) {
      var cur =
          snapshot.getSnapshot(
              GetSnapshotRequest.newBuilder()
                  .setTableId(tableId)
                  .setSnapshot(SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT))
                  .build());
      return cur.hasSnapshot() ? cur.getSnapshot().getSnapshotId() : 0L;
    }

    return snapshot
        .getSnapshot(GetSnapshotRequest.newBuilder().setTableId(tableId).setSnapshot(ref).build())
        .getSnapshot()
        .getSnapshotId();
  }

  /** Internal structure used during input resolution. */
  private record Resolved(ResourceId rid, boolean isTable, long snapshotId) {}
}
