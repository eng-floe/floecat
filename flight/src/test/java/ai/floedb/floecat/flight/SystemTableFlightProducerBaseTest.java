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

package ai.floedb.floecat.flight;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.arrow.ArrowScanPlan;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.system.rpc.SystemTableFlightCommand;
import ai.floedb.floecat.system.rpc.SystemTableFlightTicket;
import ai.floedb.floecat.system.rpc.SystemTableTarget;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SystemTableFlightProducerBaseTest {

  private static final String TABLE_NAME = "sys.test";
  private static final SchemaColumn COLUMN =
      SchemaColumn.newBuilder()
          .setName("dummy")
          .setLogicalType("int")
          .setFieldId(1)
          .setOrdinal(1)
          .setLeaf(true)
          .build();
  private static final Schema SCHEMA =
      new Schema(
          List.of(new Field("dummy", FieldType.nullable(new ArrowType.Int(32, true)), null)));

  private BufferAllocator allocator;
  private FlightExecutor executor;
  private TestProducer producer;
  private FlightProducer.CallContext callContext;

  @BeforeEach
  void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    executor = new FlightExecutor();
    producer = new TestProducer(allocator, executor);
    callContext =
        new FlightProducer.CallContext() {
          @Override
          public String peerIdentity() {
            return "test";
          }

          @Override
          public boolean isCancelled() {
            return false;
          }

          @Override
          public <T extends FlightServerMiddleware> T getMiddleware(
              FlightServerMiddleware.Key<T> key) {
            return null;
          }

          @Override
          public Map<FlightServerMiddleware.Key<?>, FlightServerMiddleware> getMiddleware() {
            return Map.of();
          }
        };
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    executor.executor().shutdown();
    if (!executor.executor().awaitTermination(5, TimeUnit.SECONDS)) {
      executor.executor().shutdownNow();
      executor.executor().awaitTermination(5, TimeUnit.SECONDS);
    }
    allocator.close();
  }

  @Test
  void getFlightInfoWithoutQueryIdFails() {
    producer.setContext(makeContext(""));
    FlightDescriptor descriptor = descriptorWithQueryId(null);
    FlightRuntimeException err =
        assertThrows(
            FlightRuntimeException.class, () -> producer.getFlightInfo(callContext, descriptor));
    assertEquals(CallStatus.INVALID_ARGUMENT.code(), err.status().code());
  }

  @Test
  void getFlightInfoWithMismatchedQueryIdFails() {
    producer.setContext(makeContext("header"));
    FlightDescriptor descriptor = descriptorWithQueryId("command");
    FlightRuntimeException err =
        assertThrows(
            FlightRuntimeException.class, () -> producer.getFlightInfo(callContext, descriptor));
    assertEquals(CallStatus.INVALID_ARGUMENT.code(), err.status().code());
  }

  @Test
  void getFlightInfoWithHeaderOnlySucceeds() {
    producer.setContext(makeContext("header"));
    FlightDescriptor descriptor = descriptorWithQueryId(null);
    assertNotNull(producer.getFlightInfo(callContext, descriptor));
  }

  @Test
  void getStreamTicketVersionMismatch() throws InterruptedException {
    producer.setContext(makeContext("header"));
    SystemTableFlightCommand command =
        SystemTableFlightCommand.newBuilder()
            .setTarget(
                SystemTableTarget.newBuilder()
                    .setName(NameRefUtil.fromCanonical(TABLE_NAME))
                    .build())
            .setQueryId("header")
            .build();
    SystemTableFlightTicket wrong =
        SystemTableFlightTicket.newBuilder().setVersion(99).setCmd(command.toByteString()).build();
    Ticket ticket = new Ticket(wrong.toByteArray());

    CapturingStreamListener listener = new CapturingStreamListener();
    producer.getStream(callContext, ticket, listener);
    assertTrue(listener.awaitError());
    assertTrue(listener.rawError() instanceof FlightRuntimeException);
    assertEquals(CallStatus.INVALID_ARGUMENT.code(), listener.error().status().code());
  }

  @Test
  void getStreamNameOnlyTicketSucceedsWithoutIdResolution() throws InterruptedException {
    producer.setContext(makeContext("header"));
    FlightDescriptor descriptor = descriptorWithQueryId("header");
    FlightInfo info = producer.getFlightInfo(callContext, descriptor);
    Ticket ticket = info.getEndpoints().get(0).getTicket();

    CapturingStreamListener listener = new CapturingStreamListener();
    producer.getStream(callContext, ticket, listener);

    assertTrue(listener.awaitCompleted());
    assertNull(listener.rawError());
  }

  @Test
  void getFlightInfoWithIdTargetFailsWhenIdCannotBeResolvedToName() {
    producer.setContext(makeContext("header"));
    ResourceId unknownId =
        ResourceId.newBuilder()
            .setAccountId("_system")
            .setId("ffffffff-ffff-4fff-8fff-ffffffffffff")
            .build();
    SystemTableFlightCommand command =
        SystemTableFlightCommand.newBuilder()
            .setTarget(SystemTableTarget.newBuilder().setId(unknownId).build())
            .setQueryId("header")
            .build();
    FlightDescriptor descriptor = FlightDescriptor.command(command.toByteArray());

    FlightRuntimeException err =
        assertThrows(
            FlightRuntimeException.class, () -> producer.getFlightInfo(callContext, descriptor));
    assertEquals(CallStatus.NOT_FOUND.code(), err.status().code());
  }

  @Test
  void supportsDescriptorWithIdTargetWhenIdResolvesToSupportedName() {
    producer.setContext(makeContext("header"));
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("_system")
            .setId("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
            .build();
    producer.registerTableId(tableId, TABLE_NAME);
    SystemTableFlightCommand command =
        SystemTableFlightCommand.newBuilder()
            .setTarget(SystemTableTarget.newBuilder().setId(tableId).build())
            .setQueryId("header")
            .build();
    FlightDescriptor descriptor = FlightDescriptor.command(command.toByteArray());

    assertTrue(producer.supportsDescriptor(descriptor));
    FlightInfo info = producer.getFlightInfo(callContext, descriptor);
    assertNotNull(info);
    assertTrue(producer.supportsTicket(info.getEndpoints().get(0).getTicket()));
  }

  private ResolvedCallContext makeContext(String queryId) {
    return new ResolvedCallContext(
        PrincipalContext.getDefaultInstance(), queryId, "corr", EngineContext.empty(), null, null);
  }

  private FlightDescriptor descriptorWithQueryId(String queryId) {
    SystemTableFlightCommand.Builder builder =
        SystemTableFlightCommand.newBuilder()
            .setTarget(
                SystemTableTarget.newBuilder()
                    .setName(NameRefUtil.fromCanonical(TABLE_NAME))
                    .build());
    if (queryId != null && !queryId.isBlank()) {
      builder.setQueryId(queryId);
    }
    return FlightDescriptor.command(builder.build().toByteArray());
  }

  private static final class TestProducer extends SystemTableFlightProducerBase {

    private ResolvedCallContext context = ResolvedCallContext.unauthenticated();
    private final Map<String, String> resolvedNamesById = new HashMap<>();

    TestProducer(BufferAllocator allocator, FlightExecutor executor) {
      super(allocator, executor);
    }

    void registerTableId(ResourceId id, String tableName) {
      resolvedNamesById.put(id.getId(), tableName);
    }

    void setContext(ResolvedCallContext context) {
      this.context = context;
    }

    @Override
    protected ResolvedCallContext resolveCallContext(CallContext context) {
      return this.context;
    }

    @Override
    protected Collection<String> tableNames(ResolvedCallContext context) {
      return List.of(TABLE_NAME);
    }

    @Override
    protected List<SchemaColumn> schemaColumns(
        String tableName,
        Optional<ResourceId> tableId,
        SystemTableFlightCommand command,
        ResolvedCallContext context) {
      return List.of(COLUMN);
    }

    @Override
    protected ArrowScanPlan buildPlan(
        String tableName,
        Optional<ResourceId> tableId,
        SystemTableFlightCommand command,
        ResolvedCallContext context,
        BufferAllocator allocator,
        BooleanSupplier cancelled) {
      return ArrowScanPlan.of(SCHEMA, Stream.empty());
    }

    @Override
    protected Location selfLocation() {
      return Location.forGrpcInsecure("localhost", 0);
    }

    @Override
    protected Optional<ResourceId> resolveSystemTableId(NameRef name, ResolvedCallContext context) {
      return Optional.empty();
    }

    @Override
    protected Optional<String> resolveSystemTableName(ResourceId id, ResolvedCallContext context) {
      return Optional.ofNullable(resolvedNamesById.get(id.getId()));
    }
  }

  private static final class CapturingStreamListener
      implements FlightProducer.ServerStreamListener {

    private final CountDownLatch errorLatch = new CountDownLatch(1);
    private final CountDownLatch completedLatch = new CountDownLatch(1);
    private volatile Throwable rawError;
    private volatile FlightRuntimeException error;

    @Override
    public void error(Throwable t) {
      rawError = t;
      if (t instanceof FlightRuntimeException fre) {
        error = fre;
      }
      errorLatch.countDown();
    }

    @Override
    public void completed() {
      completedLatch.countDown();
    }

    @Override
    public void putMetadata(org.apache.arrow.memory.ArrowBuf metadata) {}

    @Override
    public void start(org.apache.arrow.vector.VectorSchemaRoot root) {}

    @Override
    public void start(
        org.apache.arrow.vector.VectorSchemaRoot root,
        DictionaryProvider dictionaries,
        IpcOption option) {}

    @Override
    public void putNext() {}

    @Override
    public void putNext(org.apache.arrow.memory.ArrowBuf metadata) {}

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public void setOnReadyHandler(Runnable handler) {}

    @Override
    public void setOnCancelHandler(Runnable handler) {}

    @Override
    public void setUseZeroCopy(boolean enabled) {}

    @Override
    public boolean isCancelled() {
      return false;
    }

    boolean awaitError() throws InterruptedException {
      return errorLatch.await(5, TimeUnit.SECONDS);
    }

    boolean awaitCompleted() throws InterruptedException {
      return completedLatch.await(5, TimeUnit.SECONDS);
    }

    FlightRuntimeException error() {
      return error;
    }

    Throwable rawError() {
      return rawError;
    }
  }
}
