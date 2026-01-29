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

package ai.floedb.floecat.service.query.impl;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.SchemaServiceGrpc;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.BeginQueryResponse;
import ai.floedb.floecat.query.rpc.EndQueryRequest;
import ai.floedb.floecat.query.rpc.EndQueryResponse;
import ai.floedb.floecat.query.rpc.ExpansionMap;
import ai.floedb.floecat.query.rpc.GetQueryRequest;
import ai.floedb.floecat.query.rpc.GetQueryResponse;
import ai.floedb.floecat.query.rpc.QueryDescriptor;
import ai.floedb.floecat.query.rpc.QueryService;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.query.rpc.RenewQueryRequest;
import ai.floedb.floecat.query.rpc.RenewQueryResponse;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Map;
import java.util.UUID;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * gRPC implementation of the {@link QueryService} API.
 *
 * <p>This service manages the lifecycle of an analytical query. It performs:
 *
 * <ul>
 *   <li>creating and renewing a query context,
 *   <li>managing TTL,
 *   <li>exposing snapshot/expansion metadata after DescribeInputs() runs.
 * </ul>
 *
 * <p>Scan bundle retrieval is implemented separately in {@code QueryScanServiceImpl}.
 *
 * <p><b>Important:</b> BeginQuery DOES NOT accept inputs. All input resolution happens in
 * DescribeInputs() and GetUserObjects().
 */
@Singleton
@GrpcService
public class QueryServiceImpl extends BaseServiceImpl implements QueryService {

  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;

  @GrpcClient("floecat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @GrpcClient("floecat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @GrpcClient("floecat")
  SchemaServiceGrpc.SchemaServiceBlockingStub schemas;

  @GrpcClient("floecat")
  TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub stats;

  @Inject QueryContextStore queryStore;

  @Inject
  @ConfigProperty(name = "floecat.query.default-ttl-ms", defaultValue = "60000")
  long defaultTtlMs;

  private static final Logger LOG = Logger.getLogger(QueryServiceGrpc.class);

  /**
   * BeginQuery
   *
   * <p>Creates a new empty query context with a TTL.
   *
   * <p>NO INPUTS ARE ACCEPTED HERE.
   *
   * <p>All resolution (tables, views, snapshot pins, schemas, obligations, expansions) happens
   * later in DescribeInputs(), GetUserObjects(), and FetchScanBundle().
   */
  @Override
  public Uni<BeginQueryResponse> beginQuery(BeginQueryRequest request) {
    var L = LogHelper.start(LOG, "BeginQuery");

    return mapFailures(
            run(
                () -> {
                  var pc = principal.get();
                  String correlationId = pc.getCorrelationId();

                  authz.require(pc, "catalog.read");

                  // TTL in ms
                  final long ttlMs =
                      (request.getTtlSeconds() > 0
                              ? request.getTtlSeconds()
                              : (int) (defaultTtlMs / 1000))
                          * 1000L;

                  // Default catalog scope of the query
                  if (!request.hasDefaultCatalogId()) {
                    throw GrpcErrors.invalidArgument(
                        correlationId, QUERY_CATALOG_REQUIRED, Map.of());
                  }

                  ResourceId catalogId = request.getDefaultCatalogId();

                  // Empty metadata blobs
                  byte[] emptyExpansion = ExpansionMap.getDefaultInstance().toByteArray();
                  byte[] emptySnapshots = SnapshotSet.getDefaultInstance().toByteArray();
                  byte[] emptyObligations = new byte[0];
                  byte[] asOfDefaultBytes =
                      request.hasAsOfDefault() ? request.getAsOfDefault().toByteArray() : null;

                  // Build new query context
                  String queryId = UUID.randomUUID().toString();

                  var ctx =
                      QueryContext.newActive(
                          queryId,
                          pc,
                          emptyExpansion,
                          emptySnapshots,
                          emptyObligations,
                          asOfDefaultBytes,
                          ttlMs,
                          1L,
                          catalogId);

                  queryStore.put(ctx);

                  // Build descriptor
                  QueryDescriptor descriptor =
                      QueryDescriptor.newBuilder()
                          .setQueryId(queryId)
                          .setAccountId(pc.getAccountId())
                          .setQueryStatus(ctx.getQueryStatus())
                          .setCreatedAt(ts(ctx.getCreatedAtMs()))
                          .setExpiresAt(ts(ctx.getExpiresAtMs()))
                          .setSnapshots(SnapshotSet.getDefaultInstance())
                          .setExpansion(ExpansionMap.getDefaultInstance())
                          .build();

                  return BeginQueryResponse.newBuilder().setQuery(descriptor).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  /** Extends the TTL of an existing query context. */
  @Override
  public Uni<RenewQueryResponse> renewQuery(RenewQueryRequest request) {
    var L = LogHelper.start(LOG, "RenewQuery");

    return mapFailures(
            run(
                () -> {
                  var pc = principal.get();
                  var correlationId = pc.getCorrelationId();

                  authz.require(pc, "catalog.read");

                  String queryId = mustNonEmpty(request.getQueryId(), "query_id", correlationId);

                  long ttlMs =
                      (request.getTtlSeconds() > 0
                              ? request.getTtlSeconds()
                              : (int) (defaultTtlMs / 1000))
                          * 1000L;

                  long requestedExp = clock.millis() + ttlMs;

                  var updated = queryStore.extendLease(queryId, requestedExp);
                  if (updated.isEmpty()) {
                    throw GrpcErrors.notFound(
                        correlationId, QUERY_NOT_FOUND, Map.of("query_id", queryId));
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

  /** Marks a query complete (commit or abort). */
  @Override
  public Uni<EndQueryResponse> endQuery(EndQueryRequest request) {
    var L = LogHelper.start(LOG, "EndQuery");

    return mapFailures(
            run(
                () -> {
                  var pc = principal.get();
                  var correlationId = pc.getCorrelationId();

                  authz.require(pc, "catalog.read");

                  String queryId = mustNonEmpty(request.getQueryId(), "query_id", correlationId);

                  var ended = queryStore.end(queryId, request.getCommit());
                  if (ended.isEmpty()) {
                    throw GrpcErrors.notFound(
                        correlationId, QUERY_NOT_FOUND, Map.of("query_id", queryId));
                  }

                  return EndQueryResponse.newBuilder().setQueryId(queryId).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  /** Returns the full lifecycle metadata stored in the query context. */
  @Override
  public Uni<GetQueryResponse> getQuery(GetQueryRequest request) {
    var L = LogHelper.start(LOG, "GetQuery");

    return mapFailures(
            run(
                () -> {
                  var pc = principal.get();
                  var correlationId = pc.getCorrelationId();

                  authz.require(pc, "catalog.read");

                  String queryId = mustNonEmpty(request.getQueryId(), "query_id", correlationId);

                  var ctxOpt = queryStore.get(queryId);
                  if (ctxOpt.isEmpty()) {
                    throw GrpcErrors.notFound(
                        correlationId, QUERY_NOT_FOUND, Map.of("query_id", queryId));
                  }
                  var ctx = ctxOpt.get();

                  var builder =
                      QueryDescriptor.newBuilder()
                          .setQueryId(ctx.getQueryId())
                          .setAccountId(pc.getAccountId())
                          .setQueryStatus(ctx.getQueryStatus())
                          .setCreatedAt(ts(ctx.getCreatedAtMs()))
                          .setExpiresAt(ts(ctx.getExpiresAtMs()));

                  if (ctx.getSnapshotSet() != null) {
                    try {
                      builder.setSnapshots(SnapshotSet.parseFrom(ctx.getSnapshotSet()));
                    } catch (InvalidProtocolBufferException e) {
                      throw GrpcErrors.internal(
                          correlationId, QUERY_SNAPSHOT_PARSE_FAILED, Map.of("query_id", queryId));
                    }
                  }

                  if (ctx.getExpansionMap() != null) {
                    try {
                      builder.setExpansion(ExpansionMap.parseFrom(ctx.getExpansionMap()));
                    } catch (InvalidProtocolBufferException e) {
                      throw GrpcErrors.internal(
                          correlationId, QUERY_EXPANSION_PARSE_FAILED, Map.of("query_id", queryId));
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

  // ========================================================================
  // HELPERS
  // ========================================================================

  private static Timestamp ts(long millis) {
    long s = Math.floorDiv(millis, 1000);
    int n = (int) ((millis % 1000) * 1_000_000);
    return Timestamp.newBuilder().setSeconds(s).setNanos(n).build();
  }
}
