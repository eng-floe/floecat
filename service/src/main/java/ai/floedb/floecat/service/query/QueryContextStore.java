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

import ai.floedb.floecat.service.query.impl.QueryContext;
import java.util.Optional;

/**
 * Storage abstraction for server-side QueryContext.
 *
 * <p>Implementations must: - preserve immutability of QueryContext - ensure updates are monotonic
 * and consistent - never silently drop or overwrite contexts without explicit calls
 */
public interface QueryContextStore extends AutoCloseable {

  /** Retrieve an existing context and perform expiration checks. */
  Optional<QueryContext> get(String queryId);

  /** Insert a new context only if one does not already exist. */
  void put(QueryContext ctx);

  /** Extend TTL of an existing active context. */
  Optional<QueryContext> extendLease(String queryId, long requestedExpiresAtMs);

  /** Move context into END_COMMIT or END_ABORT state. */
  Optional<QueryContext> end(String queryId, boolean commit);

  /** Remove the context entirely. */
  boolean delete(String queryId);

  /** Return approximate cache size. */
  long size();

  /**
   * Replace an existing QueryContext with an updated version.
   *
   * <p>This is required for DescribeInputs(), GetCatalogBundle(), and other RPCs to populate: -
   * expansionMap - obligations - additional metadata
   *
   * <p>Unlike put(), this MUST overwrite the existing context.
   */
  void replace(QueryContext ctx);

  @Override
  void close();
}
