package ai.floedb.metacat.service.planning.impl;

import ai.floedb.metacat.catalog.rpc.FileColumnStats;
import ai.floedb.metacat.catalog.rpc.FileContent;
import ai.floedb.metacat.catalog.rpc.GetTableRequest;
import ai.floedb.metacat.catalog.rpc.GetTableResponse;
import ai.floedb.metacat.catalog.rpc.ListFileColumnStatsRequest;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.metacat.common.rpc.PageRequest;
import ai.floedb.metacat.common.rpc.PrincipalContext;
import ai.floedb.metacat.connector.spi.MetacatConnector;
import ai.floedb.metacat.planning.rpc.PlanStatus;
import ai.floedb.metacat.planning.rpc.SnapshotPin;
import ai.floedb.metacat.planning.rpc.SnapshotSet;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public final class PlanContext {

  public enum State {
    ACTIVE,
    ENDED_COMMIT,
    ENDED_ABORT,
    EXPIRED
  }

  private final String planId;
  private final PrincipalContext principal;
  private final byte[] expansionMap;
  private final byte[] snapshotSet;
  private final long createdAtMs;
  private final long expiresAtMs;
  private final State state;
  private final long version;

  private static final Clock clock = Clock.systemUTC();

  private AtomicReference<PlanStatus> planStatus = new AtomicReference<>(PlanStatus.SUBMITTED);

  private PlanContext(Builder builder) {
    this.planId = requireNonEmpty(builder.planId, "planId");
    this.principal = Objects.requireNonNull(builder.principal, "principal");
    this.expansionMap = copyOrNull(builder.expansionMap);
    this.snapshotSet = copyOrNull(builder.snapshotSet);
    this.createdAtMs = positive(builder.createdAtMs, "createdAtMs");
    this.expiresAtMs = positive(builder.expiresAtMs, "expiresAtMs");

    if (expiresAtMs < createdAtMs) {
      throw new IllegalArgumentException("expiresAtMs must be >= createdAtMs");
    }

    this.state = Objects.requireNonNull(builder.state, "state");
    this.version = builder.version < 0 ? 0 : builder.version;
  }

  public static PlanContext newActive(
      String planId,
      PrincipalContext principal,
      byte[] expansionMap,
      byte[] snapshotSet,
      long ttlMs,
      long version) {
    long now = clock.millis();
    return builder()
        .planId(planId)
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
        .principal(principal)
        .expansionMap(expansionMap)
        .snapshotSet(snapshotSet)
        .createdAtMs(createdAtMs)
        .expiresAtMs(expiresAtMs)
        .state(state)
        .version(version);
  }

  public static Builder builder() {
    return new Builder();
  }

  public PlanContext extendLease(long newExpiresAtMs, long newVersion) {
    long next = Math.max(this.expiresAtMs, newExpiresAtMs);

    if (next == this.expiresAtMs) {
      return this;
    }

    return this.toBuilder().expiresAtMs(next).version(newVersion).build();
  }

  public MetacatConnector.PlanBundle runPlanningFromStore(
      TableServiceGrpc.TableServiceBlockingStub tables,
      TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub stats) {
    try {
      if (snapshotSet != null) {
        SnapshotSet snapshots;
        try {
          snapshots = SnapshotSet.parseFrom(snapshotSet);
        } catch (InvalidProtocolBufferException e) {
          throw GrpcErrors.internal(
              principal.getCorrelationId(),
              "plan.snapshot.parse_failed",
              Map.of("plan_id", planId));
        }

        for (SnapshotPin s : snapshots.getPinsList()) {
          GetTableResponse tableResponse =
              tables.getTable(GetTableRequest.newBuilder().setTableId(s.getTableId()).build());
          Table table = tableResponse.getTable();
          var bundle = buildFromStats(table, s.getSnapshotId(), stats);
          planStatus.set(PlanStatus.COMPLETED);
          return bundle;
        }
      }
    } finally {
      if (!planStatus.get().equals(PlanStatus.COMPLETED)) {
        planStatus.set(PlanStatus.FAILED);
      }
    }
    return null;
  }

  private MetacatConnector.PlanBundle buildFromStats(
      Table table,
      long snapshotId,
      TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub stats) {
    var data = new ArrayList<ai.floedb.metacat.planning.rpc.PlanFile>();
    var deletes = new ArrayList<ai.floedb.metacat.planning.rpc.PlanFile>();
    String format = table.getUpstream().getFormat().name();

    String pageToken = "";
    do {
      var req =
          ListFileColumnStatsRequest.newBuilder()
              .setTableId(table.getResourceId())
              .setSnapshot(
                  ai.floedb.metacat.common.rpc.SnapshotRef.newBuilder().setSnapshotId(snapshotId))
              .setPage(PageRequest.newBuilder().setPageSize(1000).setPageToken(pageToken))
              .build();
      var resp = stats.listFileColumnStats(req);
      for (FileColumnStats fcs : resp.getFileColumnsList()) {
        var pf =
            ai.floedb.metacat.planning.rpc.PlanFile.newBuilder()
                .setFilePath(fcs.getFilePath())
                .setFileFormat(format)
                .setFileSizeInBytes(fcs.getSizeBytes())
                .setRecordCount(fcs.getRowCount())
                .setFileContent(mapContent(fcs.getFileContent()))
                .addAllColumns(fcs.getColumnsList())
                .build();
        if (fcs.getFileContent() == FileContent.FC_DATA) {
          data.add(pf);
        } else {
          deletes.add(pf);
        }
      }
      pageToken = resp.hasPage() ? resp.getPage().getNextPageToken() : "";
    } while (!pageToken.isBlank());

    return new MetacatConnector.PlanBundle(data, deletes);
  }

  private ai.floedb.metacat.catalog.rpc.FileContent mapContent(FileContent fc) {
    return switch (fc) {
      case FC_EQUALITY_DELETES -> ai.floedb.metacat.catalog.rpc.FileContent.FC_EQUALITY_DELETES;
      case FC_POSITION_DELETES -> ai.floedb.metacat.catalog.rpc.FileContent.FC_POSITION_DELETES;
      default -> ai.floedb.metacat.catalog.rpc.FileContent.FC_DATA;
    };
  }

  public PlanContext end(boolean commit, long graceExpiresAtMs, long newVersion) {
    var newState = commit ? State.ENDED_COMMIT : State.ENDED_ABORT;
    long nextExp = Math.max(this.expiresAtMs, graceExpiresAtMs);

    return this.toBuilder().state(newState).expiresAtMs(nextExp).version(newVersion).build();
  }

  public PlanContext asExpired(long newVersion) {
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

  public String getPlanId() {
    return planId;
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

  public PlanStatus getPlanStatus() {
    return planStatus.get();
  }

  public static final class Builder {
    private String planId;
    private PrincipalContext principal;
    private byte[] expansionMap;
    private byte[] snapshotSet;
    private long createdAtMs;
    private long expiresAtMs;
    private State state = State.ACTIVE;
    private long version;

    private Builder() {}

    public Builder planId(String v) {
      this.planId = v;
      return this;
    }

    public Builder principal(PrincipalContext v) {
      this.principal = v;
      return this;
    }

    public Builder expansionMap(byte[] v) {
      this.expansionMap = v;
      return this;
    }

    public Builder snapshotSet(byte[] v) {
      this.snapshotSet = v;
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

    public PlanContext build() {
      return new PlanContext(this);
    }
  }

  private static String requireNonEmpty(String s, String name) {
    if (s == null || s.isBlank()) {
      throw new IllegalArgumentException(name + " must be non-empty");
    }

    return s;
  }

  private static long positive(long v, String name) {
    if (v <= 0) {
      throw new IllegalArgumentException(name + " must be > 0");
    }

    return v;
  }

  private static byte[] copyOrNull(byte[] in) {
    if (in == null) {
      return null;
    }

    return Arrays.copyOf(in, in.length);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof PlanContext)) {
      return false;
    }

    PlanContext that = (PlanContext) o;
    return createdAtMs == that.createdAtMs
        && expiresAtMs == that.expiresAtMs
        && version == that.version
        && planId.equals(that.planId)
        && principal.equals(that.principal)
        && Arrays.equals(expansionMap, that.expansionMap)
        && Arrays.equals(snapshotSet, that.snapshotSet)
        && state == that.state;
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(planId, principal, createdAtMs, expiresAtMs, state, version);

    result = 31 * result + Arrays.hashCode(expansionMap);
    result = 31 * result + Arrays.hashCode(snapshotSet);
    return result;
  }

  @Override
  public String toString() {
    return "PlanContext{"
        + "planId='"
        + planId
        + '\''
        + ", createdAtMs="
        + createdAtMs
        + ", expiresAtMs="
        + expiresAtMs
        + ", state="
        + state
        + ", version="
        + version
        + '}';
  }
}
