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
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;

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

  /**
   * Replace an existing QueryContext with an updated version.
   *
   * <p>This is required for DescribeInputs(), GetUserObjects(), and other RPCs to populate: -
   * expansionMap - obligations - additional metadata
   *
   * <p>Unlike put(), this MUST overwrite the existing context.
   */
  void replace(QueryContext ctx);

  /**
   * Atomically update a stored context, using the provided function.
   *
   * <p>If the function returns the same reference or throws, the store remains unchanged. The
   * resulting context version is bumped automatically.
   */
  Optional<QueryContext> update(String queryId, UnaryOperator<QueryContext> fn);

  /**
   * Immutable blob URIs (table and snapshot) referenced by the pins of every currently-live query
   * context. Blob GC treats these as roots so a pinned immutable blob is retained for the lifetime
   * of the query that pinned it, even after the current catalog pointers have advanced past it.
   *
   * <p>Node-local: this reflects contexts held by this process, which is sufficient while query
   * contexts live in an in-process store. Retaining blobs pinned by contexts on other nodes would
   * require the context store itself to be shared across nodes.
   *
   * <p>Also includes the blobs of pins still being resolved (see {@link
   * #registerResolvingPinBlobs}), so a pin is a GC root from the moment it is constructed, not only
   * once its context is committed.
   */
  Set<String> referencedPinBlobUris();

  /**
   * Register {@code blobUris} of pins being resolved for {@code queryId} as transient GC roots.
   * This closes the window between resolving a pin (capturing a table/snapshot blob that is current
   * now) and persisting it into a cached context: without it a concurrent table change could
   * supersede the blob and blob GC delete it before the pin becomes a durable root, failing a later
   * pinned read.
   *
   * <p>Keyed by the stable {@code queryId} — not a correlation id — so the RPC that commits the
   * context releases the roots by the same key that registered them. A query's correlation id
   * changes across its RPCs (BeginQuery vs. DescribeInputs vs. GetUserObjects), so a correlation-id
   * key would leave pins resolved on a later RPC registered under one id but released under
   * another, lingering until the fail-safe grace expired.
   *
   * <p>This is the single, transparent seam for that guarantee: the input resolver calls it as each
   * pin is constructed, so every resolve→persist path (BeginQuery, DescribeInputs, GetUserObjects,
   * and any future one) is covered with no per-caller bookkeeping. Registration accumulates URIs
   * per query id and refreshes the fail-safe grace.
   *
   * <p>Roots are released transparently, without any caller involvement: whenever a pin's blob is
   * committed into a stored context (via {@code put}/{@code update}), that context becomes the
   * durable GC root and the store drops the just-rooted URI from the resolving set. The lifecycle
   * is therefore bound to the pin commit, not to a timer. The grace ({@code
   * floecat.query.resolving-pin-grace-ms}) is only a fail-safe so a resolution abandoned before any
   * commit cannot leak roots; entries are also pruned opportunistically on registration, so the
   * structure is self-maintaining independent of the blob-GC schedule.
   */
  void registerResolvingPinBlobs(String queryId, Collection<String> blobUris);

  /**
   * Release transient resolving roots previously registered for {@code queryId}.
   *
   * <p>This is for resolution attempts that fail before their candidate pins are committed into the
   * query context (for example a temporal pin conflict). Successful commits are released
   * automatically by {@code put}/{@code update}; callers must not use this for a failed
   * {@code putIfAbsent} of a client-supplied query id because the losing registration shares the
   * same query id as the incumbent and could unroot the incumbent's still-resolving blobs.
   */
  void releaseResolvingPinBlobs(String queryId, Collection<String> blobUris);

  // ---------------------------------------------------------------------
  //  Scan session helpers
  // ---------------------------------------------------------------------

  ScanHandle createScanSession(String correlationId, ScanSession session);

  Optional<ScanSession> getScanSession(ScanHandle handle);

  void removeScanSession(ScanHandle handle);

  @Override
  void close();
}
