package ai.floedb.metacat.trino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.execution.rpc.ScanFile;
import ai.floedb.metacat.query.rpc.BeginQueryRequest;
import ai.floedb.metacat.query.rpc.BeginQueryResponse;
import ai.floedb.metacat.query.rpc.QueryDescriptor;
import ai.floedb.metacat.query.rpc.QueryInput;
import ai.floedb.metacat.query.rpc.QueryServiceGrpc;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TimeZoneKey;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
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

  @Test
  void includesSnapshotIdInBeginQuery() {
    MetacatSplitManager splitManager =
        new MetacatSplitManager(QueryServiceGrpc.newBlockingStub(channel));

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

    splitManager.getSplits(null, session, handle, DynamicFilter.EMPTY, Constraint.alwaysTrue());

    QueryInput input = planning.lastRequest.getInputs(0);
    assertTrue(input.hasSnapshot());
    assertEquals(123L, input.getSnapshot().getSnapshotId());
  }

  @Test
  void includesAsOfInBeginQuery() {
    MetacatSplitManager splitManager =
        new MetacatSplitManager(QueryServiceGrpc.newBlockingStub(channel));

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

    splitManager.getSplits(null, session, handle, DynamicFilter.EMPTY, Constraint.alwaysTrue());

    QueryInput input = planning.lastRequest.getInputs(0);
    assertTrue(input.hasSnapshot());
    SnapshotRef snap = input.getSnapshot();
    assertTrue(snap.hasAsOf());
    Timestamp ts = snap.getAsOf();
    long millis = ts.getSeconds() * 1000L + ts.getNanos() / 1_000_000;
    assertEquals(asOf, millis);
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

    @Override
    public void beginQuery(
        BeginQueryRequest request, StreamObserver<BeginQueryResponse> responseObserver) {
      this.lastRequest = request;

      ScanFile file =
          ScanFile.newBuilder()
              .setFilePath("/tmp/file")
              .setFileFormat("PARQUET")
              .setFileSizeInBytes(10)
              .setRecordCount(1)
              .build();
      QueryDescriptor descriptor = QueryDescriptor.newBuilder().addDataFiles(file).build();
      responseObserver.onNext(BeginQueryResponse.newBuilder().setQuery(descriptor).build());
      responseObserver.onCompleted();
    }
  }
}
