package ai.floedb.metacat.client.trino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.execution.rpc.ScanFile;
import ai.floedb.metacat.query.rpc.BeginQueryRequest;
import ai.floedb.metacat.query.rpc.BeginQueryResponse;
import ai.floedb.metacat.query.rpc.Operator;
import ai.floedb.metacat.query.rpc.QueryDescriptor;
import ai.floedb.metacat.query.rpc.QueryInput;
import ai.floedb.metacat.query.rpc.QueryServiceGrpc;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TimeZoneKey;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class MetacatSplitManagerTest {

  private static Server server;
  private static ManagedChannel channel;
  private static PlanningStub planning;

  @BeforeAll
  static void startServer() throws Exception {
    planning = new PlanningStub();
    String serverName = InProcessServerBuilder.generateName();
    server = InProcessServerBuilder.forName(serverName).addService(planning).build().start();
    channel = InProcessChannelBuilder.forName(serverName).build();
  }

  @AfterAll
  static void shutdown() {
    if (channel != null) {
      channel.shutdownNow();
    }
    if (server != null) {
      server.shutdownNow();
    }
  }

  @AfterEach
  void resetStub() {
    planning.dataFileFormat = "PARQUET";
    planning.partitionDataJson = null;
  }

  @Test
  void includesSnapshotIdInBeginQuery() {
    MetacatSplitManager splitManager =
        new MetacatSplitManager(QueryServiceGrpc.newBlockingStub(channel), new MetacatConfig());

    MetacatTableHandle handle =
        new MetacatTableHandle(
            new SchemaTableName("s", "t"),
            "tbl",
            "tenant",
            ResourceKind.RK_TABLE.name(),
            "s3://bucket/table",
            "{}",
            null,
            "TF_ICEBERG",
            "catalog",
            TupleDomain.all(),
            Set.of("col1"),
            123L,
            null);

    ConnectorSession session =
        new SimpleSession(
            Map.of(
                MetacatSessionProperties.SNAPSHOT_ID, 123L,
                MetacatSessionProperties.AS_OF_EPOCH_MILLIS, -1L));

    try {
      splitManager.getSplits(null, session, handle, DynamicFilter.EMPTY, Constraint.alwaysTrue());
    } catch (Throwable t) {
      assumeProcessAccessible(t);
      throw t;
    }

    QueryInput input = planning.lastRequest.getInputs(0);
    assertTrue(input.hasSnapshot());
    assertEquals(123L, input.getSnapshot().getSnapshotId());
  }

  @Test
  void includesAsOfInBeginQuery() {
    MetacatSplitManager splitManager =
        new MetacatSplitManager(QueryServiceGrpc.newBlockingStub(channel), new MetacatConfig());

    long asOf = 1_700_000_000_000L;
    MetacatTableHandle handle =
        new MetacatTableHandle(
            new SchemaTableName("s", "t"),
            "tbl",
            "tenant",
            ResourceKind.RK_TABLE.name(),
            "s3://bucket/table",
            "{}",
            null,
            "TF_ICEBERG",
            "catalog",
            TupleDomain.all(),
            Set.of("col1"),
            null,
            asOf);

    ConnectorSession session =
        new SimpleSession(
            Map.of(
                MetacatSessionProperties.SNAPSHOT_ID,
                -1L,
                MetacatSessionProperties.AS_OF_EPOCH_MILLIS,
                asOf));

    try {
      splitManager.getSplits(null, session, handle, DynamicFilter.EMPTY, Constraint.alwaysTrue());
    } catch (Throwable t) {
      assumeProcessAccessible(t);
      throw t;
    }

    QueryInput input = planning.lastRequest.getInputs(0);
    assertTrue(input.hasSnapshot());
    SnapshotRef snap = input.getSnapshot();
    assertTrue(snap.hasAsOf());
    Timestamp ts = snap.getAsOf();
    long millis = ts.getSeconds() * 1000L + ts.getNanos() / 1_000_000;
    assertEquals(asOf, millis);
  }

  @Test
  void buildsPredicatesAndRequiredColumnsFromDomains() throws Exception {
    MetacatSplitManager splitManager =
        new MetacatSplitManager(QueryServiceGrpc.newBlockingStub(channel), new MetacatConfig());

    IcebergColumnHandle idCol =
        new IcebergColumnHandle(
            ColumnIdentity.primitiveColumnIdentity(1, "id"),
            BigintType.BIGINT,
            List.of(),
            BigintType.BIGINT,
            false,
            Optional.empty());
    IcebergColumnHandle bucketCol =
        new IcebergColumnHandle(
            ColumnIdentity.primitiveColumnIdentity(2, "bucket"),
            BigintType.BIGINT,
            List.of(),
            BigintType.BIGINT,
            false,
            Optional.empty());

    TupleDomain<IcebergColumnHandle> enforced =
        TupleDomain.withColumnDomains(Map.of(idCol, Domain.singleValue(BigintType.BIGINT, 7L)));
    MetacatTableHandle handle =
        new MetacatTableHandle(
            new SchemaTableName("s", "t"),
            "tbl",
            "tenant",
            ResourceKind.RK_TABLE.name(),
            "s3://bucket/table",
            "{}",
            null,
            "TF_ICEBERG",
            "catalog",
            enforced,
            Set.of("projected"),
            123L,
            null);

    Map<ColumnHandle, Domain> constraintDomains =
        Map.of(
            (ColumnHandle) bucketCol,
            Domain.create(ValueSet.ofRanges(Range.greaterThan(BigintType.BIGINT, 4L)), false));
    Constraint constraint = new Constraint(TupleDomain.withColumnDomains(constraintDomains));
    ConnectorSession session =
        new SimpleSession(
            Map.of(
                MetacatSessionProperties.SNAPSHOT_ID, 123L,
                MetacatSessionProperties.AS_OF_EPOCH_MILLIS, -1L));

    ConnectorSplitSource splitSource;
    try {
      splitSource = splitManager.getSplits(null, session, handle, DynamicFilter.EMPTY, constraint);
    } catch (Throwable t) {
      assumeProcessAccessible(t);
      throw t;
    }

    assertTrue(
        planning
            .lastRequest
            .getRequiredColumnsList()
            .containsAll(List.of("id", "bucket", "projected")));
    assertEquals(4, planning.lastRequest.getPredicatesCount());
    assertTrue(
        planning.lastRequest.getPredicatesList().stream()
            .anyMatch(
                p ->
                    p.getColumn().equals("id")
                        && p.getOp() == ai.floedb.metacat.query.rpc.Operator.OP_EQ));
    assertTrue(
        planning.lastRequest.getPredicatesList().stream()
            .anyMatch(
                p ->
                    p.getColumn().equals("bucket")
                        && (p.getOp() == Operator.OP_GT || p.getOp() == Operator.OP_IS_NOT_NULL)));

    List<ConnectorSplit> splits = splitSource.getNextBatch(10).get().getSplits();
    assertEquals(1, splits.size());
  }

  @Test
  void defaultsFormatAndPartitionDataWhenMissing() throws Exception {
    planning.dataFileFormat = "ORC";
    planning.partitionDataJson = "";

    MetacatSplitManager splitManager =
        new MetacatSplitManager(QueryServiceGrpc.newBlockingStub(channel), new MetacatConfig());

    MetacatTableHandle handle =
        new MetacatTableHandle(
            new SchemaTableName("s", "t"),
            "tbl",
            "tenant",
            ResourceKind.RK_TABLE.name(),
            "s3://bucket/table",
            "{}",
            null,
            "TF_ICEBERG",
            "catalog",
            TupleDomain.all(),
            Set.of(),
            null,
            null);

    ConnectorSession session =
        new SimpleSession(
            Map.of(
                MetacatSessionProperties.SNAPSHOT_ID, -1L,
                MetacatSessionProperties.AS_OF_EPOCH_MILLIS, -1L));

    ConnectorSplitSource splitSource;
    try {
      splitSource =
          splitManager.getSplits(
              null, session, handle, DynamicFilter.EMPTY, Constraint.alwaysTrue());
    } catch (Throwable t) {
      assumeProcessAccessible(t);
      throw t;
    }
    ConnectorSplit split = splitSource.getNextBatch(10).get().getSplits().getFirst();
    io.trino.plugin.iceberg.IcebergSplit iceberg = (io.trino.plugin.iceberg.IcebergSplit) split;

    assertEquals("{\"partitionValues\":[]}", iceberg.getPartitionDataJson());
    assertEquals(io.trino.plugin.iceberg.IcebergFileFormat.ORC, iceberg.getFileFormat());
  }

  private static class SimpleSession implements ConnectorSession {
    private final Map<String, Object> properties;

    SimpleSession(Map<String, Object> properties) {
      this.properties = properties;
    }

    @Override
    public String getQueryId() {
      return "query";
    }

    @Override
    public Optional<String> getSource() {
      return Optional.empty();
    }

    @Override
    public ConnectorIdentity getIdentity() {
      return ConnectorIdentity.ofUser("user");
    }

    @Override
    public TimeZoneKey getTimeZoneKey() {
      return TimeZoneKey.UTC_KEY;
    }

    @Override
    public Locale getLocale() {
      return Locale.US;
    }

    @Override
    public Optional<String> getTraceToken() {
      return Optional.empty();
    }

    @Override
    public Instant getStart() {
      return Instant.EPOCH;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getProperty(String name, Class<T> type) {
      return (T) properties.get(name);
    }
  }

  private static class PlanningStub extends QueryServiceGrpc.QueryServiceImplBase {
    volatile BeginQueryRequest lastRequest;
    volatile String dataFileFormat = "PARQUET";
    volatile String partitionDataJson;

    @Override
    public void beginQuery(
        BeginQueryRequest request, StreamObserver<BeginQueryResponse> responseObserver) {
      this.lastRequest = request;

      var fileBuilder =
          ScanFile.newBuilder()
              .setFilePath("/tmp/file")
              .setFileFormat(dataFileFormat)
              .setFileSizeInBytes(10)
              .setRecordCount(1);
      if (partitionDataJson != null) {
        fileBuilder.setPartitionDataJson(partitionDataJson);
      }
      ScanFile file = fileBuilder.build();
      QueryDescriptor descriptor = QueryDescriptor.newBuilder().addDataFiles(file).build();
      responseObserver.onNext(BeginQueryResponse.newBuilder().setQuery(descriptor).build());
      responseObserver.onCompleted();
    }
  }

  private static void assumeProcessAccessible(Throwable t) {
    Throwable c = t;
    while (c != null) {
      String msg = c.getMessage();
      if (msg != null && msg.contains("io.smallrye.common.os.Process")) {
        Assumptions.assumeTrue(false, "ProcessHandle info not permitted in this environment");
      }
      c = c.getCause();
    }
  }
}
