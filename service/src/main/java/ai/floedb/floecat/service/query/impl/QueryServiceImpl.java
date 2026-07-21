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
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.QueryPins;
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
import java.util.Optional;
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
 * <p><b>Important:</b> BeginQuery may optionally accept inputs for deterministic replay; schema
 * resolution still happens in DescribeInputs() and GetUserObjects().
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

  @Inject QueryInputMetadataAssembler metadataAssembler;

  @Inject
  @ConfigProperty(name = "floecat.query.default-ttl-ms", defaultValue = "60000")
  long defaultTtlMs;

  private static final Logger LOG = Logger.getLogger(QueryServiceGrpc.class);

  /**
   * BeginQuery
   *
   * <p>Creates a new query context with a TTL. Clients may optionally supply a `query_id` to pin
   * the identifier and/or a set of {@link ai.floedb.floecat.common.rpc.QueryInput} records to
   * resolve the inputs immediately.
   *
   * <p>Resolution results (pins, expansions, obligations) are stored for downstream services but
   * schema resolution and planning remain downstream responsibilities.
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

                  String queryId;
                  if (request.hasQueryId()) {
                    queryId = mustNonEmpty(request.getQueryId().trim(), "query_id", correlationId);
                  } else {
                    queryId = UUID.randomUUID().toString();
                  }

                  Optional<Timestamp> asOfDefault =
                      request.hasAsOfDefault()
                          ? Optional.of(request.getAsOfDefault())
                          : Optional.empty();
                  byte[] asOfDefaultBytes =
                      request.hasAsOfDefault()
                          ? request.getAsOfDefault().toByteArray()
                          : new byte[0];

                  var metadata =
                      metadataAssembler.assemble(
                          queryId, correlationId, request.getInputsList(), asOfDefault, catalogId);

                  byte[] expansionBytes = metadata.expansionMap().toByteArray();
                  byte[] relationPinBytes = metadata.relationPinSet().toByteArray();
                  byte[] obligationsBytes = metadata.obligationsBytes();

                  var ctx =
                      QueryContext.newActive(
                          queryId,
                          pc,
                          expansionBytes,
                          relationPinBytes,
                          obligationsBytes,
                          asOfDefaultBytes,
                          ttlMs,
                          1L,
                          catalogId);

                  // The resolved pin blobs are already transient GC roots (the resolver registered
                  // them at construction, protected through resolution and until this commit), so
                  // storing the context — a durable GC root — needs no lease here.
                  // Always branch on the insert result: putIfAbsent converts this context's pins
                  // from transient resolving roots to durable ones (dropResolvingPinsRootedBy)
                  // ONLY when it actually inserts. A silently-ignored no-op insert would serve a
                  // context whose pins were never rooted — so surface it either way.
                  boolean clientProvidedId = request.hasQueryId();
                  boolean inserted = queryStore.putIfAbsent(ctx);
                  if (!inserted) {
                    // A context already owns this query id (an incumbent). Do NOT try to release
                    // this rejected context's resolving-pin roots: they were registered under the
                    // shared query id and unioned into the incumbent's resolving entry, so dropping
                    // them by URI would also unroot blobs the incumbent may still be resolving — a
                    // GC sweep in that window could then delete a live blob. The rejected
                    // registration is already bounded (the map is size-capped, and the entry is
                    // released by the incumbent's own commit or the fail-safe grace), so leaving it
                    // in place is the safe choice.
                    if (clientProvidedId) {
                      throw GrpcErrors.alreadyExists(
                          correlationId,
                          ALREADY_EXISTS,
                          Map.of("resource", "query", "name", queryId),
                          new IllegalStateException("query_id already exists: " + queryId));
                    }
                    // A server-generated query id collided — effectively impossible, but never
                    // serve a context that was not the one that rooted its pins.
                    throw GrpcErrors.internal(
                        correlationId,
                        null,
                        null,
                        new IllegalStateException(
                            "server-generated query id collided: " + queryId));
                  }

                  // Build descriptor
                  QueryDescriptor descriptor =
                      QueryDescriptor.newBuilder()
                          .setQueryId(queryId)
                          .setAccountId(pc.getAccountId())
                          .setQueryStatus(ctx.getQueryStatus())
                          .setCreatedAt(ts(ctx.getCreatedAtMs()))
                          .setExpiresAt(ts(ctx.getExpiresAtMs()))
                          .setSnapshots(metadata.snapshotSet())
                          .setExpansion(metadata.expansionMap())
                          .addAllObligations(metadata.obligations())
                          .addAllRelationPins(QueryPins.identities(metadata.relationPinSet()))
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

                  if (ctx.getRelationPins() != null) {
                    // Expose both descriptor views of the stored pins: the snapshot-selector
                    // projection and the opaque per-relation identities BeginQuery advertises, so a
                    // caller that polls GetQuery keeps the cache/change-detection contract.
                    var pins = ctx.parseRelationPins(correlationId);
                    builder.setSnapshots(QueryPins.toSnapshotSet(pins));
                    builder.addAllRelationPins(QueryPins.identities(pins));
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
