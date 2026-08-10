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

import ai.floedb.floecat.query.rpc.RelationPinSet;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.QueryPins;
import io.smallrye.context.api.ManagedExecutorConfig;
import io.smallrye.context.api.NamedInstance;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.eclipse.microprofile.context.ThreadContext;
import org.jboss.logging.Logger;

/**
 * Releases cancelled streams' transient pin roots away from transport threads. The container-owned
 * executor bounds both concurrent releases and queued teardown work; request context is cleared
 * because cleanup needs only the explicit query id and pin set.
 */
@ApplicationScoped
final class CancellationRootReleaser {
  private static final Logger LOG = Logger.getLogger(CancellationRootReleaser.class);
  private static final int MAX_CONCURRENT_RELEASES = 4;
  private static final int MAX_QUEUED_RELEASES = 256;

  private final QueryContextStore queryStore;
  private final Executor executor;

  @Inject
  CancellationRootReleaser(
      QueryContextStore queryStore,
      @NamedInstance("bundle-cancellation-root-release")
          @ManagedExecutorConfig(
              maxAsync = MAX_CONCURRENT_RELEASES,
              maxQueued = MAX_QUEUED_RELEASES,
              propagated = {},
              cleared = ThreadContext.ALL_REMAINING)
          ManagedExecutor executor) {
    this.queryStore = queryStore;
    this.executor = executor;
  }

  CancellationRootReleaser(QueryContextStore queryStore, Executor executor) {
    this.queryStore = queryStore;
    this.executor = executor;
  }

  /** Schedule release without making the transport termination callback perform store I/O. */
  void release(String queryId, RelationPinSet pins) {
    if (pins.getPinsCount() == 0) {
      return;
    }
    try {
      executor.execute(
          () -> {
            try {
              queryStore.releaseResolvingPinBlobs(queryId, QueryPins.gcRootUris(pins));
            } catch (RuntimeException releaseFailure) {
              LOG.warnf(
                  releaseFailure,
                  "Failed to release cancelled stream pin roots query_id=%s",
                  queryId);
            }
          });
    } catch (RejectedExecutionException saturatedOrStopped) {
      LOG.warnf(
          saturatedOrStopped,
          "Cancellation teardown executor rejected pin-root release query_id=%s",
          queryId);
    }
  }
}
