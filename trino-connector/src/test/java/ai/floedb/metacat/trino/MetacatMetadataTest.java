package ai.floedb.metacat.trino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.metacat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.metacat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.metacat.catalog.rpc.GetTableRequest;
import ai.floedb.metacat.catalog.rpc.GetTableResponse;
import ai.floedb.metacat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.metacat.catalog.rpc.ListNamespacesResponse;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.metacat.catalog.rpc.ResolveFQTablesRequest;
import ai.floedb.metacat.catalog.rpc.ResolveFQTablesResponse;
import ai.floedb.metacat.catalog.rpc.ResolveTableRequest;
import ai.floedb.metacat.catalog.rpc.ResolveTableResponse;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.catalog.rpc.UpstreamRef;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class MetacatMetadataTest {

  private static final ResourceId TABLE_ID =
      ResourceId.newBuilder()
          .setId("table-id")
          .setTenantId("tenant-1")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  private static final ResourceId CATALOG_ID =
      ResourceId.newBuilder().setId("catalog-id").setTenantId("tenant-1").build();

  private static final String CURRENT_SCHEMA_JSON =
      SchemaParser.toJson(
          new Schema(Types.NestedField.required(1, "id", Types.LongType.get()))); // bigint id

  private static final String SNAPSHOT_SCHEMA_JSON =
      SchemaParser.toJson(
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "value", Types.IntegerType.get()))); // adds column

  private static ManagedChannel channel;
  private static Server server;
  private static MetacatMetadata metadata;

  @BeforeAll
  static void startServer() throws Exception {
    var directory = new DirectoryStub();
    var namespace = new NamespaceStub();
    var table = new TableStub();
    var snapshot = new SnapshotStub();

    String serverName = InProcessServerBuilder.generateName();
    server =
        InProcessServerBuilder.forName(serverName)
            .addService(directory)
            .addService(namespace)
            .addService(table)
            .addService(snapshot)
            .build()
            .start();

    channel = InProcessChannelBuilder.forName(serverName).build();

    metadata =
        new MetacatMetadata(
            NamespaceServiceGrpc.newBlockingStub(channel),
            TableServiceGrpc.newBlockingStub(channel),
            DirectoryServiceGrpc.newBlockingStub(channel),
            SnapshotServiceGrpc.newBlockingStub(channel),
            new CatalogName("test"),
            CatalogHandle.fromId("test:normal:v1"),
            new SimpleTypeManager());
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
  void usesSnapshotSchemaWhenProvided() {
    ConnectorSession session =
        new TestingSession(
            Map.of(
                MetacatSessionProperties.SNAPSHOT_ID, 42L,
                MetacatSessionProperties.AS_OF_EPOCH_MILLIS, -1L));

    MetacatTableHandle handle =
        (MetacatTableHandle)
            metadata.getTableHandle(
                session, new SchemaTableName("demo", "tbl"), Optional.empty(), Optional.empty());

    assertEquals(SNAPSHOT_SCHEMA_JSON, handle.getSchemaJson());
    assertEquals(42L, handle.getSnapshotId());
    assertNull(handle.getAsOfEpochMillis());
  }

  @Test
  void usesCurrentSchemaWhenNoSnapshot() {
    ConnectorSession session =
        new TestingSession(
            Map.of(
                MetacatSessionProperties.SNAPSHOT_ID, -1L,
                MetacatSessionProperties.AS_OF_EPOCH_MILLIS, -1L));

    MetacatTableHandle handle =
        (MetacatTableHandle)
            metadata.getTableHandle(
                session, new SchemaTableName("demo", "tbl"), Optional.empty(), Optional.empty());

    assertEquals(CURRENT_SCHEMA_JSON, handle.getSchemaJson());
    assertNull(handle.getSnapshotId());
  }

  @Test
  void rejectsSnapshotAndAsOfTogether() {
    ConnectorSession session =
        new TestingSession(
            Map.of(
                MetacatSessionProperties.SNAPSHOT_ID, 99L,
                MetacatSessionProperties.AS_OF_EPOCH_MILLIS, 1000L));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            metadata.getTableHandle(
                session, new SchemaTableName("demo", "tbl"), Optional.empty(), Optional.empty()));
  }

  private static class TestingSession implements ConnectorSession {
    private final Map<String, Object> properties;

    TestingSession(Map<String, Object> properties) {
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

  private static class SimpleTypeManager implements TypeManager {
    private final TypeOperators typeOperators = new TypeOperators();

    @Override
    public Type getType(TypeSignature signature) {
      String base = signature.getBase();
      if (StandardTypes.BIGINT.equals(base)) {
        return BigintType.BIGINT;
      }
      throw new IllegalArgumentException("Unknown type: " + signature);
    }

    @Override
    public Type fromSqlType(String sqlType) {
      if (StandardTypes.BIGINT.equalsIgnoreCase(sqlType)) {
        return BigintType.BIGINT;
      }
      throw new IllegalArgumentException("Unknown sql type: " + sqlType);
    }

    @Override
    public Type getType(TypeId typeId) {
      return BigintType.BIGINT;
    }

    @Override
    public TypeOperators getTypeOperators() {
      return typeOperators;
    }
  }

  private static class DirectoryStub extends DirectoryServiceGrpc.DirectoryServiceImplBase {
    @Override
    public void resolveCatalog(
        ResolveCatalogRequest request, StreamObserver<ResolveCatalogResponse> responseObserver) {
      responseObserver.onNext(
          ResolveCatalogResponse.newBuilder().setResourceId(CATALOG_ID).build());
      responseObserver.onCompleted();
    }

    @Override
    public void resolveTable(
        ResolveTableRequest request, StreamObserver<ResolveTableResponse> responseObserver) {
      responseObserver.onNext(ResolveTableResponse.newBuilder().setResourceId(TABLE_ID).build());
      responseObserver.onCompleted();
    }

    @Override
    public void resolveFQTables(
        ResolveFQTablesRequest request, StreamObserver<ResolveFQTablesResponse> responseObserver) {
      NameRef ref = request.getPrefix();
      var entry =
          ResolveFQTablesResponse.Entry.newBuilder().setName(ref).setResourceId(TABLE_ID).build();
      responseObserver.onNext(ResolveFQTablesResponse.newBuilder().addTables(entry).build());
      responseObserver.onCompleted();
    }
  }

  private static class NamespaceStub extends NamespaceServiceGrpc.NamespaceServiceImplBase {
    @Override
    public void listNamespaces(
        ListNamespacesRequest request, StreamObserver<ListNamespacesResponse> responseObserver) {
      var ns =
          Namespace.newBuilder()
              .setDisplayName("default")
              .setResourceId(ResourceId.newBuilder().setId("ns").build())
              .build();
      responseObserver.onNext(ListNamespacesResponse.newBuilder().addNamespaces(ns).build());
      responseObserver.onCompleted();
    }
  }

  private static class TableStub extends TableServiceGrpc.TableServiceImplBase {
    @Override
    public void getTable(
        GetTableRequest request, StreamObserver<GetTableResponse> responseObserver) {
      var upstream =
          UpstreamRef.newBuilder()
              .setUri("s3://bucket/table")
              .setFormat(TableFormat.TF_ICEBERG)
              .build();
      var table =
          Table.newBuilder()
              .setResourceId(TABLE_ID)
              .setCatalogId(CATALOG_ID)
              .setSchemaJson(CURRENT_SCHEMA_JSON)
              .setUpstream(upstream)
              .build();
      responseObserver.onNext(GetTableResponse.newBuilder().setTable(table).build());
      responseObserver.onCompleted();
    }
  }

  private static class SnapshotStub extends SnapshotServiceGrpc.SnapshotServiceImplBase {
    @Override
    public void getSnapshot(
        GetSnapshotRequest request, StreamObserver<GetSnapshotResponse> responseObserver) {
      SnapshotRef ref = request.getSnapshot();
      var snapshot =
          Snapshot.newBuilder()
              .setTableId(TABLE_ID)
              .setSnapshotId(ref.hasSnapshotId() ? ref.getSnapshotId() : 0L)
              .setSchemaJson(SNAPSHOT_SCHEMA_JSON)
              .build();
      responseObserver.onNext(GetSnapshotResponse.newBuilder().setSnapshot(snapshot).build());
      responseObserver.onCompleted();
    }
  }
}
