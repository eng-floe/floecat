package ai.floedb.metacat.service.query.impl;

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
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.connector.spi.MetacatConnector;
import ai.floedb.metacat.execution.rpc.ScanFile;
import ai.floedb.metacat.execution.rpc.ScanFileContent;
import ai.floedb.metacat.query.rpc.QueryStatus;
import ai.floedb.metacat.query.rpc.SnapshotPin;
import ai.floedb.metacat.query.rpc.SnapshotSet;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public final class QueryContext {

  public enum State {
    ACTIVE,
    ENDED_COMMIT,
    ENDED_ABORT,
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

  public static Builder builder() {
    return new Builder();
  }

  public QueryContext extendLease(long newExpiresAtMs, long newVersion) {
    long next = Math.max(this.expiresAtMs, newExpiresAtMs);

    if (next == this.expiresAtMs) {
      return this;
    }

    return this.toBuilder().expiresAtMs(next).version(newVersion).build();
  }

  public MetacatConnector.ScanBundle fetchScanBundle(
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
              "query.snapshot.parse_failed",
              Map.of("query_id", queryId));
        }

        for (SnapshotPin s : snapshots.getPinsList()) {
          GetTableResponse tableResponse =
              tables.getTable(GetTableRequest.newBuilder().setTableId(s.getTableId()).build());
          Table table = tableResponse.getTable();
          var bundle = buildFromStats(table, s.getSnapshotId(), stats);
          queryStatus.set(QueryStatus.COMPLETED);
          return bundle;
        }
      }
    } finally {
      if (!queryStatus.get().equals(QueryStatus.COMPLETED)) {
        queryStatus.set(QueryStatus.FAILED);
      }
    }
    return null;
  }

  private MetacatConnector.ScanBundle buildFromStats(
      Table table,
      long snapshotId,
      TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub stats) {
    var data = new ArrayList<ScanFile>();
    var deletes = new ArrayList<ScanFile>();
    String format = table.getUpstream().getFormat().name();

    String pageToken = "";
    do {
      var req =
          ListFileColumnStatsRequest.newBuilder()
              .setTableId(table.getResourceId())
              .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId))
              .setPage(PageRequest.newBuilder().setPageSize(1000).setPageToken(pageToken))
              .build();
      var resp = stats.listFileColumnStats(req);
      for (FileColumnStats fcs : resp.getFileColumnsList()) {
        var scanFile =
            ScanFile.newBuilder()
                .setFilePath(fcs.getFilePath())
                .setFileFormat(format)
                .setFileSizeInBytes(fcs.getSizeBytes())
                .setRecordCount(fcs.getRowCount())
                .setPartitionDataJson(fcs.getPartitionDataJson())
                .setPartitionSpecId(fcs.getPartitionSpecId())
                .addAllEqualityFieldIds(fcs.getEqualityFieldIdsList())
                .setFileContent(mapContent(fcs.getFileContent()))
                .addAllColumns(fcs.getColumnsList())
                .build();
        if (fcs.getFileContent() == FileContent.FC_DATA) {
          data.add(scanFile);
        } else {
          deletes.add(scanFile);
        }
      }
      pageToken = resp.hasPage() ? resp.getPage().getNextPageToken() : "";
    } while (!pageToken.isBlank());

    return new MetacatConnector.ScanBundle(data, deletes);
  }

  private ScanFileContent mapContent(FileContent fc) {
    return switch (fc) {
      case FC_EQUALITY_DELETES -> ScanFileContent.SCAN_FILE_CONTENT_EQUALITY_DELETES;
      case FC_POSITION_DELETES -> ScanFileContent.SCAN_FILE_CONTENT_POSITION_DELETES;
      default -> ScanFileContent.SCAN_FILE_CONTENT_DATA;
    };
  }

  public QueryContext end(boolean commit, long graceExpiresAtMs, long newVersion) {
    var newState = commit ? State.ENDED_COMMIT : State.ENDED_ABORT;
    long nextExp = Math.max(this.expiresAtMs, graceExpiresAtMs);

    return this.toBuilder().state(newState).expiresAtMs(nextExp).version(newVersion).build();
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

  public QueryStatus getQueryStatus() {
    return queryStatus.get();
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
