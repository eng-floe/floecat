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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;

/**
 * Releases cancelled streams' transient pin roots away from transport threads and owns the worker
 * lifecycle used for that teardown.
 */
final class CancellationRootReleaser implements AutoCloseable {
  private static final Logger LOG = Logger.getLogger(CancellationRootReleaser.class);
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

  private final QueryContextStore queryStore;
  private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

  CancellationRootReleaser(QueryContextStore queryStore) {
    this.queryStore = queryStore;
  }

  /** Schedule release without making the transport termination callback perform store I/O. */
  void release(String queryId, RelationPinSet pins) {
    try {
      executor.submit(
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
    } catch (RejectedExecutionException shutdown) {
      LOG.warnf(
          shutdown,
          "Cancellation teardown executor stopped before pin-root release query_id=%s",
          queryId);
    }
  }

  /** Bound shutdown even if a store call ignores interruption. */
  @Override
  public void close() {
    executor.shutdownNow();
    boolean terminated;
    try {
      terminated = executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      terminated = false;
    }
    if (!terminated) {
      LOG.warn("bundle cancellation teardown executor did not terminate before shutdown timeout");
    }
  }
}
