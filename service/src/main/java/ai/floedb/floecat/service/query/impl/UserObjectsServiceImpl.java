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

import ai.floedb.floecat.query.rpc.GetUserObjectsRequest;
import ai.floedb.floecat.query.rpc.UserObjectsBundleChunk;
import ai.floedb.floecat.query.rpc.UserObjectsService;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.GrpcContextUtil;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.catalog.UserObjectBundleService;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.control.ActivateRequestContext;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.jboss.logging.Logger;

@GrpcService
public class UserObjectsServiceImpl extends BaseServiceImpl implements UserObjectsService {

  @Inject PrincipalProvider principal;

  @Inject Authorizer authz;

  @Inject QueryContextStore queryStore;

  @Inject UserObjectBundleService bundles;

  private static final Logger LOG = Logger.getLogger(UserObjectsServiceImpl.class);

  @ActivateRequestContext
  @Override
  public Multi<UserObjectsBundleChunk> getUserObjects(GetUserObjectsRequest request) {
    var L = LogHelper.start(LOG, "GetUserObjects");
    long startNs = System.nanoTime();
    AtomicLong workStartNs = new AtomicLong(0L);
    AtomicBoolean completed = new AtomicBoolean(false);
    AtomicBoolean failed = new AtomicBoolean(false);
    AtomicReference<String> correlationRef = new AtomicReference<>("");
    // Capture the incoming gRPC context so the principal/correlation-id stays available
    // throughout the entire streaming response — including lazy item emission from the
    // UserObjectBundleIterator, which calls principal.get() for name resolution.
    GrpcContextUtil grpcCtx = GrpcContextUtil.capture();

    return Multi.createFrom()
        .<UserObjectsBundleChunk>emitter(
            emitter -> {
              grpcCtx.run(
                  () -> {
                    workStartNs.compareAndSet(0L, System.nanoTime());
                    var principalContext = principal.get();
                    var contextCorrelationId = InboundContextInterceptor.CORR_KEY.get();
                    var principalCorrelationId = principalContext.getCorrelationId();
                    var correlationId =
                        contextCorrelationId != null && !contextCorrelationId.isBlank()
                            ? contextCorrelationId
                            : principalCorrelationId;
                    correlationRef.set(correlationId == null ? "" : correlationId);
                    authz.require(principalContext, "catalog.read");

                    String queryId = mustNonEmpty(request.getQueryId(), "query_id", correlationId);
                    var ctxOpt = queryStore.get(queryId);
                    if (ctxOpt.isEmpty()) {
                      emitter.fail(
                          GrpcErrors.notFound(
                              correlationId, QUERY_NOT_FOUND, Map.of("query_id", queryId)));
                      return;
                    }

                    QueryContext ctx = ctxOpt.get();
                    if (!ctx.isActive()) {
                      emitter.fail(
                          GrpcErrors.preconditionFailed(
                              correlationId,
                              QUERY_NOT_ACTIVE,
                              Map.of("query_id", queryId, "state", ctx.getState().name())));
                      return;
                    }

                    var subscription =
                        bundles.stream(correlationId, ctx, request.getTablesList())
                            .subscribe()
                            .with(emitter::emit, emitter::fail, emitter::complete);

                    emitter.onTermination(subscription::cancel);
                    emitter.onCancellation(subscription::cancel);
                  });
            })
        .runSubscriptionOn(Infrastructure.getDefaultExecutor())
        .onCompletion()
        .invoke(() -> completed.set(true))
        .onFailure()
        .invoke(
            t -> {
              failed.set(true);
              L.fail(t);
            })
        .onTermination()
        .invoke(
            () -> {
              long workNs = workStartNs.get();
              double dispatchMs = workNs <= 0L ? 0.0 : (workNs - startNs) / 1_000_000.0;
              if (failed.get()) {
                LOG.warnf(
                    "op=GetUserObjects terminated query_id=%s correlation_id=%s tables=%d"
                        + " dispatchMs=%.1f outcome=failed",
                    request.getQueryId(),
                    correlationRef.get(),
                    request.getTablesCount(),
                    dispatchMs);
                return;
              }
              if (!completed.get()) {
                LOG.warnf(
                    "op=GetUserObjects terminated query_id=%s correlation_id=%s tables=%d"
                        + " dispatchMs=%.1f outcome=cancelled",
                    request.getQueryId(),
                    correlationRef.get(),
                    request.getTablesCount(),
                    dispatchMs);
                return;
              }
              L.okf(
                  "query_id=%s correlation_id=%s tables=%d dispatchMs=%.1f outcome=completed",
                  request.getQueryId(), correlationRef.get(), request.getTablesCount(), dispatchMs);
            });
  }
}
