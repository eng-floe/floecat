package ai.floedb.metacat.service.planning.impl;

import java.time.Clock;
import java.util.Objects;
import java.util.Optional;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.protobuf.InvalidProtocolBufferException;

import ai.floedb.metacat.common.rpc.PrincipalContext;
import ai.floedb.metacat.planning.rpc.ExpansionMap;
import ai.floedb.metacat.planning.rpc.SnapshotSet;

public final class PlanContext {

  public enum State {
    ACTIVE,
    ENDED_COMMIT,
    ENDED_ABORT,
    EXPIRED
  }

  private final String planId;
  private final String tenantId;
  private final PrincipalContext principal;
  private final byte[] expansionMap;
  private final byte[] snapshotSet;
  private final long createdAtMs;
  private final long expiresAtMs;
  private final State state;
  private final long version;

  private static final Clock clock = Clock.systemUTC();

  private PlanContext(Builder b) {
    this.planId = requireNonEmpty(b.planId, "planId");
    this.tenantId = requireNonEmpty(b.tenantId, "tenantId");
    this.principal = Objects.requireNonNull(b.principal, "principal");
    if (!tenantId.equals(principal.getTenantId())) {
      throw new IllegalArgumentException("tenantId must match principal.tenant_id");
    }
    this.expansionMap = copyOrNull(b.expansionMap);
    this.snapshotSet = copyOrNull(b.snapshotSet);
    this.createdAtMs = positive(b.createdAtMs, "createdAtMs");
    this.expiresAtMs = positive(b.expiresAtMs, "expiresAtMs");
    if (expiresAtMs < createdAtMs) {
      throw new IllegalArgumentException("expiresAtMs must be >= createdAtMs");
    }
    this.state = Objects.requireNonNull(b.state, "state");
    this.version = b.version < 0 ? 0 : b.version;
  }

  public static PlanContext newActive(String planId,
                                      String tenantId,
                                      PrincipalContext principal,
                                      byte[] expansionMap,
                                      byte[] snapshotSet,
                                      long ttlMs,
                                      long version) {
    long now = clock.millis();
    return builder()
      .planId(planId)
      .tenantId(tenantId)
      .principal(principal)
      .expansionMap(expansionMap)
      .snapshotSet(snapshotSet)
      .createdAtMs(now)
      .expiresAtMs(now + Math.max(1, ttlMs))
      .state(State.ACTIVE)
      .version(version)
      .build();
  }

  public Builder toBuilder() {
    return builder()
      .planId(planId)
      .tenantId(tenantId)
      .principal(principal)
      .expansionMap(expansionMap)
      .snapshotSet(snapshotSet)
      .createdAtMs(createdAtMs)
      .expiresAtMs(expiresAtMs)
      .state(state)
      .version(version);
  }

  public static Builder builder() { return new Builder(); }

  public PlanContext extendLease(long newExpiresAtMs, long newVersion) {
    long next = Math.max(this.expiresAtMs, newExpiresAtMs);
    if (next == this.expiresAtMs) return this;
    return this.toBuilder().expiresAtMs(next).version(newVersion).build();
  }

  public PlanContext end(boolean commit, long graceExpiresAtMs, long newVersion) {
    var newState = commit ? State.ENDED_COMMIT : State.ENDED_ABORT;
    long nextExp = Math.max(this.expiresAtMs, graceExpiresAtMs);
    return this.toBuilder().state(newState).expiresAtMs(nextExp).version(newVersion).build();
  }

  public PlanContext asExpired(long newVersion) {
    if (this.state != State.ACTIVE) return this;
    return this.toBuilder().state(State.EXPIRED).version(newVersion).build();
  }

  public boolean isActive() { return state == State.ACTIVE; }

  public long remainingTtlMs(long nowMs) { return Math.max(0, expiresAtMs - nowMs); }

  public String getPlanId() { return planId; }
  public String getTenantId() { return tenantId; }
  public PrincipalContext getPrincipal() { return principal; }
  public byte[] getExpansionMap() { return copyOrNull(expansionMap); }
  public byte[] getSnapshotSet() { return copyOrNull(snapshotSet); }
  public long getCreatedAtMs() { return createdAtMs; }
  public long getExpiresAtMs() { return expiresAtMs; }
  public State getState() { return state; }
  public long getVersion() { return version; }

  public static final class Builder {
    private String planId;
    private String tenantId;
    private PrincipalContext principal;
    private byte[] expansionMap;
    private byte[] snapshotSet;
    private long createdAtMs;
    private long expiresAtMs;
    private State state = State.ACTIVE;
    private long version;

    private Builder() {}

    public Builder planId(String v) { this.planId = v; return this; }
    public Builder tenantId(String v) { this.tenantId = v; return this; }
    public Builder principal(PrincipalContext v) { this.principal = v; return this; }
    public Builder expansionMap(byte[] v) { this.expansionMap = v; return this; }
    public Builder snapshotSet(byte[] v) { this.snapshotSet = v; return this; }
    public Builder createdAtMs(long v) { this.createdAtMs = v; return this; }
    public Builder expiresAtMs(long v) { this.expiresAtMs = v; return this; }
    public Builder state(State v) { this.state = v; return this; }
    public Builder version(long v) { this.version = v; return this; }

    public PlanContext build() { return new PlanContext(this); }
  }

  private static String requireNonEmpty(String s, String name) {
    if (s == null || s.isBlank()) throw new IllegalArgumentException(name + " must be non-empty");
    return s;
  }

  private static long positive(long v, String name) {
    if (v <= 0) throw new IllegalArgumentException(name + " must be > 0");
    return v;
  }

  private static byte[] copyOrNull(byte[] in) {
    if (in == null) return null;
    return java.util.Arrays.copyOf(in, in.length);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PlanContext)) return false;
    PlanContext that = (PlanContext) o;
    return createdAtMs == that.createdAtMs &&
           expiresAtMs == that.expiresAtMs &&
           version == that.version &&
           planId.equals(that.planId) &&
           tenantId.equals(that.tenantId) &&
           principal.equals(that.principal) &&
           java.util.Arrays.equals(expansionMap, that.expansionMap) &&
           java.util.Arrays.equals(snapshotSet, that.snapshotSet) &&
           state == that.state;
  }

  @Override public int hashCode() {
    int result = Objects.hash(planId, tenantId, principal, createdAtMs, expiresAtMs, state, version);
    result = 31 * result + java.util.Arrays.hashCode(expansionMap);
    result = 31 * result + java.util.Arrays.hashCode(snapshotSet);
    return result;
  }

  @Override public String toString() {
    return "PlanContext{" +
      "planId='" + planId + '\'' +
      ", tenantId='" + tenantId + '\'' +
      ", createdAtMs=" + createdAtMs +
      ", expiresAtMs=" + expiresAtMs +
      ", state=" + state +
      ", version=" + version +
      '}';
  }
}
