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

package ai.floedb.floecat.service.query;

import ai.floedb.floecat.query.rpc.ScanHandle;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.query.impl.ScanSession;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * Storage abstraction for server-side QueryContext.
 *
 * <p>Contexts are immutable values updated atomically. A context may be evicted once it is terminal
 * or idle past {@link #maxLeaseMs}; an active query never outlives it.
 */
public interface QueryContextStore extends AutoCloseable {

  /** Retrieve an existing context and perform expiration checks. */
  Optional<QueryContext> get(String queryId);

  /** Insert a new context only if one does not already exist. */
  void put(QueryContext ctx);

  /**
   * Insert a new context only if one does not already exist.
   *
   * @return true if the context was inserted, false if the id already exists
   */
  boolean putIfAbsent(QueryContext ctx);

  /** Extend TTL of an existing active context. */
  Optional<QueryContext> extendLease(String queryId, long requestedExpiresAtMs);

  /** Move context into END_COMMIT or END_ABORT state. */
  Optional<QueryContext> end(String queryId, boolean commit);

  /** Remove the context entirely. */
  boolean delete(String queryId);

  /** Return approximate cache size. */
  long size();

  /** The longest lease a context can keep; requested leases are capped to it. */
  default long maxLeaseMs() {
    return Long.MAX_VALUE;
  }

  /**
   * Atomically update a stored context, using the provided function.
   *
   * <p>If the function returns the same reference or throws, the store remains unchanged. The
   * resulting context version is bumped automatically. An expired active context is atomically
   * transitioned to its terminal state without invoking the function and is returned so callers can
   * distinguish {@code QUERY_NOT_ACTIVE} from a missing query.
   */
  Optional<QueryContext> update(String queryId, UnaryOperator<QueryContext> fn);

  // ---------------------------------------------------------------------
  //  Scan session helpers
  // ---------------------------------------------------------------------

  ScanHandle createScanSession(String correlationId, ScanSession session);

  Optional<ScanSession> getScanSession(ScanHandle handle);

  void removeScanSession(ScanHandle handle);

  @Override
  void close();
}
