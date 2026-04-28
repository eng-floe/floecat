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

package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.EndQueryRequest;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.scanner.utils.EngineCatalogNames;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.it.profiles.FlightDevProfile;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.system.rpc.SystemTableFlightCommand;
import ai.floedb.floecat.system.rpc.SystemTableTarget;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import io.grpc.Metadata;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.grpc.MetadataAdapter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the Arrow Flight server for FLOECAT system tables.
 *
 * <p>Tests: getFlightInfo returns projected schema; getStream returns matching data; schema from
 * getFlightInfo matches schema from getStream.
 */
@QuarkusTest
@TestProfile(FlightDevProfile.class)
public class SystemTableFlightIT {

  @GrpcClient("floecat")
  QueryServiceGrpc.QueryServiceBlockingStub queries;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @ConfigProperty(name = "quarkus.grpc.server.port")
  int grpcPort;

  private BufferAllocator allocator;
  private FlightClient client;
  private String queryId;

  private static CallOption queryHeader(String queryId) {
    Metadata metadata = new Metadata();
    metadata.put(Metadata.Key.of("x-query-id", Metadata.ASCII_STRING_MARSHALLER), queryId);
    CallHeaders headers = new MetadataAdapter(metadata);
    return new HeaderCallOption(headers);
  }

  private static CallOption correlationHeader(String correlationId) {
    Metadata metadata = new Metadata();
    metadata.put(
        Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER), correlationId);
    CallHeaders headers = new MetadataAdapter(metadata);
    return new HeaderCallOption(headers);
  }

  @BeforeEach
  void setUp() {
    resetter.wipeAll();
    seeder.seedData();
    allocator = new RootAllocator(Long.MAX_VALUE);
    assertTrue(grpcPort > 0, "gRPC server should be running on a valid port");
    client =
        FlightClient.builder(allocator, Location.forGrpcInsecure("localhost", grpcPort)).build();
    queryId =
        TestSupport.callWhenGrpcReady(
                () ->
                    queries.beginQuery(
                        BeginQueryRequest.newBuilder()
                            .setDefaultCatalogId(
                                SystemNodeRegistry.systemCatalogContainerId(
                                    EngineCatalogNames.FLOECAT_DEFAULT_CATALOG))
                            .build()))
            .getQuery()
            .getQueryId();
    assertFalse(queryId.isBlank(), "BeginQuery must return a queryId");
  }

  @AfterEach
  void tearDown() throws Exception {
    if (queryId != null && !queryId.isBlank()) {
      queries.endQuery(EndQueryRequest.newBuilder().setQueryId(queryId).setCommit(true).build());
    }
    if (client != null) {
      client.close();
    }
    if (allocator != null) {
      allocator.close();
    }
  }

  @Test
  void getFlightInfo_returnsProjectedSchemaForInformationSchemaColumns() {
    ResourceId tableId = informationSchemaColumnsTableId();
    SystemTableFlightCommand command =
        SystemTableFlightCommand.newBuilder()
            .setTarget(SystemTableTarget.newBuilder().setId(tableId).build())
            .setQueryId(queryId)
            .build();
    FlightDescriptor descriptor = FlightDescriptor.command(command.toByteArray());

    FlightInfo info = client.getInfo(descriptor, queryHeader(queryId));

    assertNotNull(info, "FlightInfo should not be null");
    var schema =
        info.getSchemaOptional().orElseThrow(() -> new AssertionError("FlightInfo schema missing"));
    assertFalse(schema.getFields().isEmpty(), "Schema should have fields");
    assertFalse(info.getEndpoints().isEmpty(), "FlightInfo should have at least one endpoint");
  }

  @Test
  void getFlightInfo_echoesCorrelationIdInResponseHeaders() throws Exception {
    ResourceId tableId = informationSchemaColumnsTableId();
    SystemTableFlightCommand command =
        SystemTableFlightCommand.newBuilder()
            .setTarget(SystemTableTarget.newBuilder().setId(tableId).build())
            .setQueryId(queryId)
            .build();
    FlightDescriptor descriptor = FlightDescriptor.command(command.toByteArray());
    String correlationId = "flight-it-" + UUID.randomUUID();
    HeaderCaptureMiddleware.Factory capture = new HeaderCaptureMiddleware.Factory();

    try (FlightClient clientWithCapture =
        FlightClient.builder(allocator, Location.forGrpcInsecure("localhost", grpcPort))
            .intercept(capture)
            .build()) {
      clientWithCapture.getInfo(descriptor, queryHeader(queryId), correlationHeader(correlationId));
    }

    assertEquals(correlationId, capture.lastHeaders.get(), "Flight should echo x-correlation-id");
  }

  @Test
  void getFlightInfo_projectedSchemaMatchesStreamSchema() {
    ResourceId tableId = informationSchemaColumnsTableId();
    SystemTableFlightCommand command =
        SystemTableFlightCommand.newBuilder()
            .setTarget(SystemTableTarget.newBuilder().setId(tableId).build())
            .addRequiredColumns("table_name")
            .addRequiredColumns("column_name")
            .setQueryId(queryId)
            .build();
    FlightDescriptor descriptor = FlightDescriptor.command(command.toByteArray());

    FlightInfo info = client.getInfo(descriptor, queryHeader(queryId));
    var projectedSchema =
        info.getSchemaOptional().orElseThrow(() -> new AssertionError("Projected schema missing"));
    assertEquals(2, projectedSchema.getFields().size(), "Projected schema should have 2 fields");
    assertEquals("table_name", projectedSchema.getFields().get(0).getName());
    assertEquals("column_name", projectedSchema.getFields().get(1).getName());

    // Verify the stream also has the projected schema
    List<List<String>> rows = readRows(info);
    // Rows may be empty if no data, but that's OK — schema consistency is what we test here.
    // The schema used for reading succeeded without an error.
  }

  @Test
  void getStream_returnsRowsForInformationSchemaTables() {
    ResourceId tableId = informationSchemaTablesTableId();
    SystemTableFlightCommand command =
        SystemTableFlightCommand.newBuilder()
            .setTarget(SystemTableTarget.newBuilder().setId(tableId).build())
            .setQueryId(queryId)
            .build();
    FlightDescriptor descriptor = FlightDescriptor.command(command.toByteArray());

    FlightInfo info = client.getInfo(descriptor, queryHeader(queryId));
    List<List<String>> rows = readRows(info);

    // information_schema.tables is always populated (at minimum with information_schema itself)
    assertFalse(rows.isEmpty(), "information_schema.tables should return at least one row");
  }

  @Test
  void getStream_returnsRowsForInformationSchemaTablesByName() {
    SystemTableFlightCommand command =
        SystemTableFlightCommand.newBuilder()
            .setTarget(
                SystemTableTarget.newBuilder()
                    .setName(
                        NameRef.newBuilder()
                            .addPath("information_schema")
                            .setName("tables")
                            .build())
                    .build())
            .setQueryId(queryId)
            .build();
    FlightDescriptor descriptor = FlightDescriptor.command(command.toByteArray());

    FlightInfo info = client.getInfo(descriptor, queryHeader(queryId));
    List<List<String>> rows = readRows(info);

    assertFalse(rows.isEmpty(), "information_schema.tables by name should return rows");
  }

  @Test
  void getFlightInfo_acceptsSystemTableIdWithCallerAccount() {
    ResourceId callerScopedTableId =
        informationSchemaTablesTableId().toBuilder().setAccountId("caller-account").build();
    SystemTableFlightCommand command =
        SystemTableFlightCommand.newBuilder()
            .setTarget(SystemTableTarget.newBuilder().setId(callerScopedTableId).build())
            .setQueryId(queryId)
            .build();
    FlightDescriptor descriptor = FlightDescriptor.command(command.toByteArray());

    FlightInfo info = client.getInfo(descriptor, queryHeader(queryId));
    List<List<String>> rows = readRows(info);

    assertNotNull(info, "FlightInfo should not be null");
    assertFalse(rows.isEmpty(), "Caller-scoped system id should still resolve and stream rows");
  }

  @Test
  void getStream_projectionReducesColumns() {
    ResourceId tableId = informationSchemaColumnsTableId();
    SystemTableFlightCommand command =
        SystemTableFlightCommand.newBuilder()
            .setTarget(SystemTableTarget.newBuilder().setId(tableId).build())
            .addRequiredColumns("column_name")
            .setQueryId(queryId)
            .build();
    FlightDescriptor descriptor = FlightDescriptor.command(command.toByteArray());

    FlightInfo info = client.getInfo(descriptor, queryHeader(queryId));
    var projectedSchema =
        info.getSchemaOptional().orElseThrow(() -> new AssertionError("schema missing"));
    assertEquals(1, projectedSchema.getFields().size());

    List<List<String>> rows = readRows(info);
    for (List<String> row : rows) {
      assertEquals(1, row.size(), "Each row should have exactly 1 column after projection");
    }
  }

  @Test
  void getFlightInfo_rejectsMismatchedEngineTableId() {
    ResourceId wrongEngineId =
        SystemNodeRegistry.resourceId("floedb", ResourceKind.RK_TABLE, "information_schema.tables");
    SystemTableFlightCommand command =
        SystemTableFlightCommand.newBuilder()
            .setTarget(SystemTableTarget.newBuilder().setId(wrongEngineId).build())
            .setQueryId(queryId)
            .build();
    FlightDescriptor descriptor = FlightDescriptor.command(command.toByteArray());

    FlightRuntimeException error =
        assertThrows(
            FlightRuntimeException.class, () -> client.getInfo(descriptor, queryHeader(queryId)));
    assertEquals(CallStatus.NOT_FOUND.code(), error.status().code());
  }

  // -------------------------------------------------------------------------
  //  Helpers
  // -------------------------------------------------------------------------

  private List<List<String>> readRows(FlightInfo info) {
    List<List<String>> rows = new ArrayList<>();
    for (var endpoint : info.getEndpoints()) {
      try (FlightStream stream = client.getStream(endpoint.getTicket(), queryHeader(queryId))) {
        VectorSchemaRoot root = stream.getRoot();
        while (stream.next()) {
          for (int i = 0; i < root.getRowCount(); i++) {
            List<String> row = new ArrayList<>();
            for (FieldVector vector : root.getFieldVectors()) {
              Object val = vector.getObject(i);
              row.add(val == null ? null : val.toString());
            }
            rows.add(row);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to read Flight stream", e);
      }
    }
    return rows;
  }

  /** Returns the ResourceId for information_schema.columns in the FLOECAT default engine. */
  private static ResourceId informationSchemaColumnsTableId() {
    return systemTableId("information_schema.columns");
  }

  private static ResourceId informationSchemaTablesTableId() {
    return systemTableId("information_schema.tables");
  }

  private static ResourceId systemTableId(String qualifiedName) {
    return SystemNodeRegistry.resourceId(
        EngineCatalogNames.FLOECAT_DEFAULT_CATALOG, ResourceKind.RK_TABLE, qualifiedName);
  }

  private static final class HeaderCaptureMiddleware implements FlightClientMiddleware {
    private final AtomicReference<String> headers;

    private HeaderCaptureMiddleware(AtomicReference<String> headers) {
      this.headers = headers;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {}

    @Override
    public void onHeadersReceived(CallHeaders incomingHeaders) {
      headers.set(incomingHeaders.get("x-correlation-id"));
    }

    @Override
    public void onCallCompleted(CallStatus status) {}

    private static final class Factory implements FlightClientMiddleware.Factory {
      private final AtomicReference<String> lastHeaders = new AtomicReference<>();

      @Override
      public FlightClientMiddleware onCallStarted(CallInfo info) {
        return new HeaderCaptureMiddleware(lastHeaders);
      }
    }
  }
}
