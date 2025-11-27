package ai.floedb.metacat.service.query.impl;

import ai.floedb.metacat.common.rpc.PrincipalContext;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.query.rpc.QueryStatus;
import ai.floedb.metacat.query.rpc.SnapshotPin;
import ai.floedb.metacat.query.rpc.SnapshotSet;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Immutable representation of a query’s server-side state.
 *
 * <p>A {@code QueryContext} tracks:
 *
 * <ul>
 *   <li>basic metadata (query ID, principal, created/expiry timestamps),
 *   <li>the pinned snapshots for all referenced tables,
 *   <li>the expansion map used during planning,
 *   <li>the current query lifecycle state,
 *   <li>a monotonic version, used for consistent updates.
 * </ul>
 *
 * <p>This object does not perform any query execution. It acts as durable metadata stored in {@link
 * ai.floedb.metacat.service.query.QueryContextStore}.
 *
 * <p>Instances are created via the {@link Builder} or the helper {@link #newActive(String,
 * PrincipalContext, byte[], byte[], long, long)} method.
 */
public final class QueryContext {

  /** Represents the high-level lifecycle of a query. */
  public enum State {
    /** Query is active and may be extended or used for planning. */
    ACTIVE,

    /** Query completed successfully and is now in its commit grace period. */
    ENDED_COMMIT,

    /** Query ended with an abort and is now in its grace period. */
    ENDED_ABORT,

    /** Query expired before being explicitly ended. */
    EXPIRED
  }

  private final String queryId;
  private final PrincipalContext principal;
  private final byte[] expansionMap;
  private final byte[] snapshotSet;
  private final long createdAtMs;
  private final long expiresAtMs;
  private final State state;
  private final long version;

  private static final Clock clock = Clock.systemUTC();

  /** Tracks the planning status associated with this query. */
  private final AtomicReference<QueryStatus> queryStatus =
      new AtomicReference<>(QueryStatus.SUBMITTED);

  private QueryContext(Builder builder) {
    this.queryId = requireNonEmpty(builder.queryId, "queryId");
    this.principal = Objects.requireNonNull(builder.principal, "principal");
    this.expansionMap = copyOrNull(builder.expansionMap);
    this.snapshotSet = copyOrNull(builder.snapshotSet);
    this.createdAtMs = positive(builder.createdAtMs, "createdAtMs");
    this.expiresAtMs = positive(builder.expiresAtMs, "expiresAtMs");

    if (expiresAtMs < createdAtMs) {
      throw new IllegalArgumentException("expiresAtMs must be >= createdAtMs");
    }

    this.state = Objects.requireNonNull(builder.state, "state");
    this.version = Math.max(0, builder.version);
  }

  /** Constructs a new active {@code QueryContext} with the given TTL. */
  public static QueryContext newActive(
      String queryId,
      PrincipalContext principal,
      byte[] expansionMap,
      byte[] snapshotSet,
      long ttlMs,
      long version) {

    long now = clock.millis();
    return builder()
        .queryId(queryId)
        .principal(principal)
        .expansionMap(expansionMap)
        .snapshotSet(snapshotSet)
        .createdAtMs(now)
        .expiresAtMs(now + Math.max(1, ttlMs))
        .state(State.ACTIVE)
        .version(version)
        .build();
  }

  /**
   * Returns a new builder pre-filled with this instance's fields. Useful for producing modified
   * copies.
   */
  public Builder toBuilder() {
    return builder()
        .queryId(queryId)
        .principal(principal)
        .expansionMap(expansionMap)
        .snapshotSet(snapshotSet)
        .createdAtMs(createdAtMs)
        .expiresAtMs(expiresAtMs)
        .state(state)
        .version(version);
  }

  /** Returns a fresh builder. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Extends the TTL of the query context.
   *
   * <p>If {@code newExpiresAtMs} is before the existing expiration time, the instance is returned
   * unchanged. Otherwise, a new instance is produced.
   */
  public QueryContext extendLease(long newExpiresAtMs, long newVersion) {
    long next = Math.max(this.expiresAtMs, newExpiresAtMs);

    if (next == this.expiresAtMs) {
      return this;
    }

    return this.toBuilder().expiresAtMs(next).version(newVersion).build();
  }

  /**
   * Returns the pinned snapshot associated with the given table ID.
   *
   * <p>Throws a gRPC error if:
   *
   * <ul>
   *   <li>the query has no snapshot set, or
   *   <li>no matching pin exists for this table.
   * </ul>
   */
  public SnapshotPin requireSnapshotPin(ResourceId tableId, String correlationId) {
    Objects.requireNonNull(tableId, "tableId");

    if (snapshotSet == null) {
      throw GrpcErrors.invalidArgument(
          correlationId, "query.snapshots.missing", Map.of("query_id", queryId));
    }

    SnapshotSet snapshots = parseSnapshotSet(correlationId);
    return snapshots.getPinsList().stream()
        .filter(pin -> pin.hasTableId() && tableIdMatches(pin.getTableId(), tableId))
        .findFirst()
        .orElseThrow(
            () ->
                GrpcErrors.notFound(
                    correlationId,
                    "query.table.not_pinned",
                    Map.of("query_id", queryId, "table_id", tableId.getId())));
  }

  /** Safely parses the stored snapshot set. */
  private SnapshotSet parseSnapshotSet(String correlationId) {
    try {
      return SnapshotSet.parseFrom(snapshotSet);
    } catch (InvalidProtocolBufferException e) {
      throw GrpcErrors.internal(
          correlationId, "query.snapshot.parse_failed", Map.of("query_id", queryId));
    }
  }

  /**
   * Produces a new context representing the end of execution (either commit or abort) with an
   * extended grace TTL.
   */
  public QueryContext end(boolean commit, long graceExpiresAtMs, long newVersion) {
    var newState = commit ? State.ENDED_COMMIT : State.ENDED_ABORT;
    long nextExp = Math.max(this.expiresAtMs, graceExpiresAtMs);

    return this.toBuilder().state(newState).expiresAtMs(nextExp).version(newVersion).build();
  }

  /** Marks the query as expired if it is still active. */
  public QueryContext asExpired(long newVersion) {
    if (this.state != State.ACTIVE) {
      return this;
    }
    return this.toBuilder().state(State.EXPIRED).version(newVersion).build();
  }

  /** Returns true if the query is still active. */
  public boolean isActive() {
    return state == State.ACTIVE;
  }

  /** Returns remaining TTL in milliseconds relative to the provided timestamp. */
  public long remainingTtlMs(long nowMs) {
    return Math.max(0, expiresAtMs - nowMs);
  }

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

  /** Returns the planning status associated with this query. */
  public QueryStatus getQueryStatus() {
    return queryStatus.get();
  }

  /** Marks planning as completed successfully. */
  public void markPlanningCompleted() {
    queryStatus.set(QueryStatus.COMPLETED);
  }

  /** Marks planning as failed. */
  public void markPlanningFailed() {
    queryStatus.set(QueryStatus.FAILED);
  }

  /**
   * Determines whether two table IDs correspond to the same physical table.
   *
   * <p>Tenant IDs must match (if both are provided) and resource IDs must be equal.
   */
  private boolean tableIdMatches(ResourceId left, ResourceId right) {
    if (left == null || right == null) {
      return false;
    }

    String leftTenant = left.getTenantId();
    String rightTenant = right.getTenantId();

    if (!isBlank(leftTenant) && !isBlank(rightTenant) && !Objects.equals(leftTenant, rightTenant)) {
      return false;
    }
    return Objects.equals(left.getId(), right.getId());
  }

  private boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  private static byte[] copyOrNull(byte[] input) {
    return input == null ? null : Arrays.copyOf(input, input.length);
  }

  private static String requireNonEmpty(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(field + " must be provided");
    }
    return value;
  }

  private static long positive(long value, String field) {
    if (value <= 0) {
      throw new IllegalArgumentException(field + " must be > 0");
    }
    return value;
  }

  /** Builder used to create immutable {@link QueryContext} instances. */
  public static final class Builder {
    private String queryId;
    private PrincipalContext principal;
    private byte[] expansionMap;
    private byte[] snapshotSet;
    private long createdAtMs;
    private long expiresAtMs;
    private State state = State.ACTIVE;
    private long version;

    private Builder() {}

    public Builder queryId(String queryId) {
      this.queryId = queryId;
      return this;
    }

    public Builder principal(PrincipalContext principal) {
      this.principal = principal;
      return this;
    }

    public Builder expansionMap(byte[] expansionMap) {
      this.expansionMap = copyOrNull(expansionMap);
      return this;
    }

    public Builder snapshotSet(byte[] snapshotSet) {
      this.snapshotSet = copyOrNull(snapshotSet);
      return this;
    }

    public Builder createdAtMs(long createdAtMs) {
      this.createdAtMs = createdAtMs;
      return this;
    }

    public Builder expiresAtMs(long expiresAtMs) {
      this.expiresAtMs = expiresAtMs;
      return this;
    }

    public Builder state(State state) {
      this.state = state;
      return this;
    }

    public Builder version(long version) {
      this.version = version;
      return this;
    }

    public QueryContext build() {
      return new QueryContext(this);
    }
  }
}
