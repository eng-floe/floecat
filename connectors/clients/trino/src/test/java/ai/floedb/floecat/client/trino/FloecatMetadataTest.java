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

package ai.floedb.floecat.client.trino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.floecat.catalog.rpc.ListNamespacesResponse;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.floecat.catalog.rpc.ResolveFQTablesRequest;
import ai.floedb.floecat.catalog.rpc.ResolveFQTablesResponse;
import ai.floedb.floecat.catalog.rpc.ResolveTableRequest;
import ai.floedb.floecat.catalog.rpc.ResolveTableResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
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
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class FloecatMetadataTest {

  private static final ResourceId TABLE_ID =
      ResourceId.newBuilder()
          .setId("table-id")
          .setAccountId("account-1")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  private static final ResourceId CATALOG_ID =
      ResourceId.newBuilder()
          .setId("catalog-id")
          .setAccountId("account-1")
          .setKind(ResourceKind.RK_CATALOG)
          .build();

  private static final String CURRENT_SCHEMA_JSON =
      SchemaParser.toJson(
          new Schema(Types.NestedField.required(1, "id", Types.LongType.get()))); // bigint id

  private static final String SNAPSHOT_SCHEMA_JSON =
      SchemaParser.toJson(
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "value", Types.IntegerType.get()))); // adds column
  private static volatile boolean includeUpstream = true;
  private static volatile String tableSchemaJson = CURRENT_SCHEMA_JSON;
  private static volatile String upstreamUri = "s3://bucket/table";
  private static volatile String snapshotSchemaJson = SNAPSHOT_SCHEMA_JSON;

  private static ManagedChannel channel;
  private static Server server;
  private static FloecatMetadata metadata;

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
        new FloecatMetadata(
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

  @AfterEach
  void resetStubs() {
    includeUpstream = true;
    tableSchemaJson = CURRENT_SCHEMA_JSON;
    upstreamUri = "s3://bucket/table";
    snapshotSchemaJson = SNAPSHOT_SCHEMA_JSON;
  }

  @Test
  void usesSnapshotSchemaWhenProvided() {
    ConnectorSession session =
        new TestingSession(
            Map.of(
                FloecatSessionProperties.SNAPSHOT_ID, 42L,
                FloecatSessionProperties.AS_OF_EPOCH_MILLIS, -1L));

    FloecatTableHandle handle =
        (FloecatTableHandle)
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
                FloecatSessionProperties.SNAPSHOT_ID, -1L,
                FloecatSessionProperties.AS_OF_EPOCH_MILLIS, -1L));

    FloecatTableHandle handle =
        (FloecatTableHandle)
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
                FloecatSessionProperties.SNAPSHOT_ID, 99L,
                FloecatSessionProperties.AS_OF_EPOCH_MILLIS, 1000L));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            metadata.getTableHandle(
                session, new SchemaTableName("demo", "tbl"), Optional.empty(), Optional.empty()));
  }

  @Test
  void fallsBackToCurrentSchemaWhenSnapshotSchemaIsBlank() {
    snapshotSchemaJson = "";
    ConnectorSession session =
        new TestingSession(
            Map.of(
                FloecatSessionProperties.SNAPSHOT_ID, 42L,
                FloecatSessionProperties.AS_OF_EPOCH_MILLIS, -1L));

    FloecatTableHandle handle =
        (FloecatTableHandle)
            metadata.getTableHandle(
                session, new SchemaTableName("demo", "tbl"), Optional.empty(), Optional.empty());

    assertEquals(CURRENT_SCHEMA_JSON, handle.getSchemaJson());
    assertEquals(42L, handle.getSnapshotId());
  }

  @Test
  void applyFilterIntersectsDomains() {
    IcebergColumnHandle col =
        new IcebergColumnHandle(
            ColumnIdentity.primitiveColumnIdentity(1, "id"),
            BigintType.BIGINT,
            java.util.List.of(),
            BigintType.BIGINT,
            false,
            Optional.empty());
    FloecatTableHandle handle =
        new FloecatTableHandle(
            new SchemaTableName("demo", "tbl"),
            TABLE_ID.getId(),
            TABLE_ID.getAccountId(),
            TABLE_ID.getKind().name(),
            "s3://bucket/table",
            CURRENT_SCHEMA_JSON,
            null,
            TableFormat.TF_ICEBERG.name(),
            "catalog",
            TupleDomain.all(),
            Set.of(),
            null,
            null);

    Map<ColumnHandle, Domain> domainMap =
        Map.of((ColumnHandle) col, Domain.singleValue(BigintType.BIGINT, 5L));
    Constraint constraint = new Constraint(TupleDomain.withColumnDomains(domainMap));

    var result =
        metadata.applyFilter(
            new TestingSession(
                Map.of(
                    FloecatSessionProperties.SNAPSHOT_ID, -1L,
                    FloecatSessionProperties.AS_OF_EPOCH_MILLIS, -1L)),
            handle,
            constraint);

    FloecatTableHandle newHandle = (FloecatTableHandle) result.orElseThrow().getHandle();
    assertEquals(
        TupleDomain.withColumnDomains(Map.of(col, Domain.singleValue(BigintType.BIGINT, 5L))),
        newHandle.getEnforcedConstraint());
  }

  @Test
  void applyFilterReturnsEmptyWhenUnchanged() {
    IcebergColumnHandle col =
        new IcebergColumnHandle(
            ColumnIdentity.primitiveColumnIdentity(1, "id"),
            BigintType.BIGINT,
            java.util.List.of(),
            BigintType.BIGINT,
            false,
            Optional.empty());
    TupleDomain<IcebergColumnHandle> domain =
        TupleDomain.withColumnDomains(Map.of(col, Domain.singleValue(BigintType.BIGINT, 5L)));
    FloecatTableHandle handle =
        new FloecatTableHandle(
            new SchemaTableName("demo", "tbl"),
            TABLE_ID.getId(),
            TABLE_ID.getAccountId(),
            TABLE_ID.getKind().name(),
            "s3://bucket/table",
            CURRENT_SCHEMA_JSON,
            null,
            TableFormat.TF_ICEBERG.name(),
            "catalog",
            domain,
            Set.of(),
            null,
            null);

    Constraint constraint =
        new Constraint((TupleDomain<ColumnHandle>) (TupleDomain<?>) domain);
    var result =
        metadata.applyFilter(
            new TestingSession(
                Map.of(
                    FloecatSessionProperties.SNAPSHOT_ID, -1L,
                    FloecatSessionProperties.AS_OF_EPOCH_MILLIS, -1L)),
            handle,
            constraint);

    assertTrue(result.isEmpty());
  }

  @Test
  void applyProjectionPreservesAssignments() {
    IcebergColumnHandle col =
        new IcebergColumnHandle(
            ColumnIdentity.primitiveColumnIdentity(1, "bucket"),
            BigintType.BIGINT,
            java.util.List.of(),
            BigintType.BIGINT,
            false,
            Optional.empty());

    FloecatTableHandle handle =
        new FloecatTableHandle(
            new SchemaTableName("demo", "tbl"),
            TABLE_ID.getId(),
            TABLE_ID.getAccountId(),
            TABLE_ID.getKind().name(),
            "s3://bucket/table",
            CURRENT_SCHEMA_JSON,
            null,
            TableFormat.TF_ICEBERG.name(),
            "catalog",
            TupleDomain.all(),
            Set.of(),
            null,
            null);

    ProjectionApplicationResult<ConnectorTableHandle> result =
        metadata
            .applyProjection(
                new TestingSession(
                    Map.of(
                        FloecatSessionProperties.SNAPSHOT_ID, -1L,
                        FloecatSessionProperties.AS_OF_EPOCH_MILLIS, -1L)),
                handle,
                java.util.List.of(),
                Map.of("bucket", col))
            .orElseThrow();

    FloecatTableHandle newHandle = (FloecatTableHandle) result.getHandle();
    assertTrue(newHandle.getProjectedColumns().contains("bucket"));
    assertEquals(1, result.getAssignments().size());
    Assignment assignment = result.getAssignments().getFirst();
    assertEquals("bucket", assignment.getVariable());
    assertEquals(col, assignment.getColumn());
  }

  @Test
  void returnsNullWhenUpstreamMissing() {
    includeUpstream = false;
    ConnectorSession session =
        new TestingSession(
            Map.of(
                FloecatSessionProperties.SNAPSHOT_ID, -1L,
                FloecatSessionProperties.AS_OF_EPOCH_MILLIS, -1L));

    assertNull(
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
              .setResourceId(
                  ResourceId.newBuilder()
                      .setId("ns")
                      .setKind(ResourceKind.RK_NAMESPACE)
                      .build())
              .build();
      responseObserver.onNext(ListNamespacesResponse.newBuilder().addNamespaces(ns).build());
      responseObserver.onCompleted();
    }
  }

  private static class TableStub extends TableServiceGrpc.TableServiceImplBase {
    @Override
    public void getTable(
        GetTableRequest request, StreamObserver<GetTableResponse> responseObserver) {
      var tableBuilder =
          Table.newBuilder()
              .setResourceId(TABLE_ID)
              .setCatalogId(CATALOG_ID)
              .setSchemaJson(tableSchemaJson);
      if (includeUpstream) {
        var upstream =
            UpstreamRef.newBuilder().setUri(upstreamUri).setFormat(TableFormat.TF_ICEBERG).build();
        tableBuilder.setUpstream(upstream);
      }
      responseObserver.onNext(GetTableResponse.newBuilder().setTable(tableBuilder.build()).build());
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
              .setSchemaJson(snapshotSchemaJson)
              .build();
      responseObserver.onNext(GetSnapshotResponse.newBuilder().setSnapshot(snapshot).build());
      responseObserver.onCompleted();
    }
  }
}
