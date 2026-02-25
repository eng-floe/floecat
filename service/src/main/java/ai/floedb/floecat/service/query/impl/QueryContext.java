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

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.QueryStatus;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import java.time.Clock;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Immutable representation of a query’s server-side state.
 *
 * <p>A {@code QueryContext} tracks:
 *
 * <ul>
 *   <li>basic metadata (query ID, principal, create/expiry timestamps),
 *   <li>the pinned snapshots for all referenced tables,
 *   <li>the expansion map used during planning,
 *   <li>governance obligations (row/column filters),
 *   <li>the optional as-of-default timestamp provided at BeginQuery,
 *   <li>the current query lifecycle state,
 *   <li>a monotonic version for CAS updates.
 * </ul>
 *
 * <p>All fields are immutable. Updates occur strictly by building a new instance using {@link
 * #toBuilder()} and storing it in {@link ai.floedb.floecat.service.query.QueryContextStore}.
 *
 * <p>These objects never perform catalog lookups or I/O. They only carry state.
 */
public final class QueryContext {

  /** High-level lifecycle state. */
  public enum State {
    /** Query is active and may be extended or scanned. */
    ACTIVE,

    /** Query completed with commit and entered the grace period. */
    ENDED_COMMIT,

    /** Query completed with abort and entered the grace period. */
    ENDED_ABORT,

    /** Query expired naturally before being ended. */
    EXPIRED
  }

  private final String queryId;
  private final PrincipalContext principal;

  /**
   * Encoded metadata blobs (protobuf payloads):
   *
   * <p>expansionMap → view expansion results (may be null until DescribeInputs). snapshotSet →
   * pinned snapshots for all tables (never null once created). obligations → governance rules (may
   * be null until DescribeInputs). asOfDefault → optional Timestamp provided at BeginQuery
   * (protobuf-encoded, may be null).
   */
  private final byte[] expansionMap;

  private final byte[] snapshotSet;
  private final byte[] obligations;
  private final byte[] asOfDefault;

  private final long createdAtMs;
  private final long expiresAtMs;
  private final State state;
  private final long version;
  private final ResourceId queryDefaultCatalogId;

  private static final Clock clock = Clock.systemUTC();

  /** Planning completion state for external monitoring. */
  private final AtomicReference<QueryStatus> queryStatus;

  /** Cached parsed SnapshotSet for this immutable context instance (lazy). */
  private final transient AtomicReference<SnapshotSet> parsedSnapshotSet = new AtomicReference<>();

  // ----------------------------------------------------------------------
  //  Construction
  // ----------------------------------------------------------------------

  private QueryContext(Builder b) {
    this.queryId = requireNonEmpty(b.queryId, "queryId");
    this.principal = Objects.requireNonNull(b.principal, "principal");

    this.expansionMap = copyOrNull(b.expansionMap);
    this.snapshotSet = copyOrNull(b.snapshotSet);
    this.obligations = copyOrNull(b.obligations);
    this.asOfDefault = copyOrNull(b.asOfDefault);

    this.createdAtMs = positive(b.createdAtMs, "createdAtMs");
    this.expiresAtMs = positive(b.expiresAtMs, "expiresAtMs");

    if (expiresAtMs < createdAtMs) {
      throw new IllegalArgumentException("expiresAtMs must be >= createdAtMs");
    }

    this.state = Objects.requireNonNull(b.state, "state");
    this.version = Math.max(0, b.version);
    this.queryDefaultCatalogId =
        Objects.requireNonNull(b.queryDefaultCatalogId, "queryDefaultCatalogId");
    this.queryStatus = new AtomicReference<>(Objects.requireNonNull(b.queryStatus, "queryStatus"));
  }

  /**
   * Creates a new active QueryContext with TTL.
   *
   * <p>The context may already contain: - snapshotSet (never null) - expansionMap (usually null at
   * BeginQuery) - obligations (usually null at BeginQuery) - asOfDefault (optional)
   */
  public static QueryContext newActive(
      String queryId,
      PrincipalContext principal,
      byte[] expansionMap,
      byte[] snapshotSet,
      byte[] obligations,
      byte[] asOfDefault,
      long ttlMs,
      long version,
      ResourceId queryDefaultCatalogId) {

    long now = clock.millis();

    return builder()
        .queryId(queryId)
        .principal(principal)
        .expansionMap(expansionMap)
        .snapshotSet(snapshotSet)
        .obligations(obligations)
        .asOfDefault(asOfDefault)
        .createdAtMs(now)
        .expiresAtMs(now + Math.max(1, ttlMs))
        .state(State.ACTIVE)
        .version(version)
        .queryDefaultCatalogId(queryDefaultCatalogId)
        .build();
  }

  // ----------------------------------------------------------------------
  //  Builder
  // ----------------------------------------------------------------------

  public static Builder builder() {
    return new Builder();
  }

  public Builder toBuilder() {
    return builder()
        .queryId(queryId)
        .principal(principal)
        .expansionMap(expansionMap)
        .snapshotSet(snapshotSet)
        .obligations(obligations)
        .asOfDefault(asOfDefault)
        .createdAtMs(createdAtMs)
        .expiresAtMs(expiresAtMs)
        .state(state)
        .version(version)
        .queryDefaultCatalogId(queryDefaultCatalogId)
        .queryStatus(queryStatus.get());
  }

  public static final class Builder {
    private String queryId;
    private PrincipalContext principal;
    private byte[] expansionMap;
    private byte[] snapshotSet;
    private byte[] obligations;
    private byte[] asOfDefault;
    private long createdAtMs;
    private long expiresAtMs;
    private State state = State.ACTIVE;
    private long version;
    private ResourceId queryDefaultCatalogId;
    private QueryStatus queryStatus = QueryStatus.SUBMITTED;

    private Builder() {}

    public Builder queryId(String v) {
      this.queryId = v;
      return this;
    }

    public Builder principal(PrincipalContext v) {
      this.principal = v;
      return this;
    }

    public Builder expansionMap(byte[] v) {
      this.expansionMap = copyOrNull(v);
      return this;
    }

    public Builder snapshotSet(byte[] v) {
      this.snapshotSet = copyOrNull(v);
      return this;
    }

    public Builder obligations(byte[] v) {
      this.obligations = copyOrNull(v);
      return this;
    }

    /** Set encoded asOfDefault Timestamp (nullable). */
    public Builder asOfDefault(byte[] v) {
      this.asOfDefault = copyOrNull(v);
      return this;
    }

    public Builder createdAtMs(long v) {
      this.createdAtMs = v;
      return this;
    }

    public Builder expiresAtMs(long v) {
      this.expiresAtMs = v;
      return this;
    }

    public Builder state(State v) {
      this.state = v;
      return this;
    }

    public Builder version(long v) {
      this.version = v;
      return this;
    }

    public Builder queryStatus(QueryStatus v) {
      this.queryStatus = Objects.requireNonNull(v, "queryStatus");
      return this;
    }

    public Builder queryDefaultCatalogId(ResourceId v) {
      this.queryDefaultCatalogId = v;
      return this;
    }

    public QueryContext build() {
      return new QueryContext(this);
    }
  }

  // ----------------------------------------------------------------------
  //  Snapshot handling
  // ----------------------------------------------------------------------

  /** Returns the pinned snapshot for a given table, or throws if not pinned. */
  public SnapshotPin requireSnapshotPin(ResourceId tableId, String correlationId) {
    Objects.requireNonNull(tableId, "tableId");

    if (snapshotSet == null) {
      throw GrpcErrors.invalidArgument(
          correlationId, QUERY_SNAPSHOTS_MISSING, Map.of("query_id", queryId));
    }

    SnapshotSet set = parseSnapshotSet(correlationId);

    return set.getPinsList().stream()
        .filter(pin -> pin.hasTableId() && tableIdMatches(pin.getTableId(), tableId))
        .findFirst()
        .orElseThrow(
            () ->
                GrpcErrors.notFound(
                    correlationId,
                    QUERY_TABLE_NOT_PINNED,
                    Map.of("query_id", queryId, "table_id", tableId.getId())));
  }

  public Optional<SnapshotPin> findSnapshotPin(ResourceId tableId, String correlationId) {
    Objects.requireNonNull(tableId, "tableId");

    if (snapshotSet == null) {
      return Optional.empty();
    }

    SnapshotSet set = parseSnapshotSet(correlationId);

    return set.getPinsList().stream()
        .filter(pin -> pin.hasTableId() && tableIdMatches(pin.getTableId(), tableId))
        .findFirst();
  }

  private SnapshotSet parseSnapshotSet(String correlationId) {
    // Memoize parsing: QueryContext is immutable, so caching is safe for the life of the instance.
    SnapshotSet cached = parsedSnapshotSet.get();
    if (cached != null) {
      return cached;
    }
    try {
      SnapshotSet parsed = SnapshotSet.parseFrom(snapshotSet);
      // Benign race: multiple threads may parse; only one wins the cache.
      parsedSnapshotSet.compareAndSet(null, parsed);
      return parsed;
    } catch (InvalidProtocolBufferException e) {
      throw GrpcErrors.internal(
          correlationId, QUERY_SNAPSHOT_PARSE_FAILED, Map.of("query_id", queryId));
    }
  }

  // ----------------------------------------------------------------------
  //  As-of Default Parsing
  // ----------------------------------------------------------------------

  /**
   * Returns the parsed protobuf Timestamp for the BeginQuery-level "as-of default" or an empty
   * Optional if none was provided.
   *
   * <p>This is used by DescribeInputs() if no snapshot override exists and a timestamp was given as
   * default.
   */
  public java.util.Optional<Timestamp> parseAsOfDefault(String correlationId) {
    if (asOfDefault == null || asOfDefault.length == 0) {
      return java.util.Optional.empty();
    }

    try {
      return java.util.Optional.of(Timestamp.parseFrom(asOfDefault));
    } catch (InvalidProtocolBufferException e) {
      throw GrpcErrors.internal(
          correlationId, QUERY_AS_OF_DEFAULT_PARSE_FAILED, Map.of("query_id", queryId));
    }
  }

  // ----------------------------------------------------------------------
  //  Expiration and lifecycle
  // ----------------------------------------------------------------------

  public QueryContext extendLease(long newExpiresAtMs, long newVersion) {
    long next = Math.max(this.expiresAtMs, newExpiresAtMs);
    if (next == this.expiresAtMs) {
      return this; // no change
    }
    return this.toBuilder().expiresAtMs(next).version(newVersion).build();
  }

  public QueryContext end(boolean commit, long graceExpiresAtMs, long newVersion) {
    State nextState = commit ? State.ENDED_COMMIT : State.ENDED_ABORT;
    long nextExp = Math.max(this.expiresAtMs, graceExpiresAtMs);

    return this.toBuilder().state(nextState).expiresAtMs(nextExp).version(newVersion).build();
  }

  public QueryContext asExpired(long newVersion) {
    if (this.state != State.ACTIVE) {
      return this;
    }
    return this.toBuilder().state(State.EXPIRED).version(newVersion).build();
  }

  public boolean isActive() {
    return state == State.ACTIVE;
  }

  public long remainingTtlMs(long nowMs) {
    return Math.max(0, expiresAtMs - nowMs);
  }

  // ----------------------------------------------------------------------
  //  Planning status
  // ----------------------------------------------------------------------

  public QueryStatus getQueryStatus() {
    return queryStatus.get();
  }

  public void markPlanningCompleted() {
    queryStatus.set(QueryStatus.COMPLETED);
  }

  public void markPlanningFailed() {
    queryStatus.set(QueryStatus.FAILED);
  }

  // ----------------------------------------------------------------------
  //  Field Accessors
  // ----------------------------------------------------------------------

  public String getQueryId() {
    return queryId;
  }

  public PrincipalContext getPrincipal() {
    return principal;
  }

  public byte[] getExpansionMap() {
    return copyOrNull(expansionMap);
  }

  public byte[] getSnapshotSet() {
    return copyOrNull(snapshotSet);
  }

  public byte[] getObligations() {
    return copyOrNull(obligations);
  }

  /** Returns encoded as-of default Timestamp or null. */
  public byte[] getAsOfDefault() {
    return copyOrNull(asOfDefault);
  }

  public long getCreatedAtMs() {
    return createdAtMs;
  }

  public long getExpiresAtMs() {
    return expiresAtMs;
  }

  public State getState() {
    return state;
  }

  public long getVersion() {
    return version;
  }

  public ResourceId getQueryDefaultCatalogId() {
    return queryDefaultCatalogId;
  }

  // ----------------------------------------------------------------------
  //  Internal helpers
  // ----------------------------------------------------------------------

  private static byte[] copyOrNull(byte[] in) {
    return in == null ? null : Arrays.copyOf(in, in.length);
  }

  private static boolean isBlank(String s) {
    return s == null || s.isBlank();
  }

  private boolean tableIdMatches(ResourceId a, ResourceId b) {
    if (a == null || b == null) return false;

    if (!isBlank(a.getAccountId())
        && !isBlank(b.getAccountId())
        && !Objects.equals(a.getAccountId(), b.getAccountId())) {
      return false;
    }
    return Objects.equals(a.getId(), b.getId());
  }

  private static String requireNonEmpty(String v, String field) {
    if (v == null || v.isBlank()) {
      throw new IllegalArgumentException(field + " must be provided");
    }
    return v;
  }

  private static long positive(long v, String field) {
    if (v <= 0) {
      throw new IllegalArgumentException(field + " must be > 0");
    }
    return v;
  }
}
