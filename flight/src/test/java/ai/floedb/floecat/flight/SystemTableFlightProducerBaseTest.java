package ai.floedb.floecat.flight;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.arrow.ArrowScanPlan;
import ai.floedb.floecat.arrow.SimpleColumnarBatch;
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.flight.testhelpers.FlightTestHelpers;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.system.rpc.SystemTableFlightCommand;
import ai.floedb.floecat.system.rpc.SystemTableFlightTicket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SystemTableFlightProducerBaseTest {

  private static final Schema SCHEMA = FlightTestHelpers.simpleSchema();
  private static final SchemaColumn SCHEMA_COLUMN =
      FlightTestHelpers.schemaColumn("col", new ArrowType.Int(32, false), 1);
  private static final SystemTableFlightCommand COMMAND = FlightTestHelpers.command("sys.test");

  private FlightExecutor flightExecutor;
  private FlightAllocatorHolder allocatorHolder;
  private TrackingAllocator trackingAllocator;
  private TestProducer producer;

  @BeforeEach
  void setUp() {
    flightExecutor = new FlightExecutor();
    allocatorHolder = new FlightAllocatorHolder();
    trackingAllocator = new TrackingAllocator(Long.MAX_VALUE);
    allocatorHolder.setAllocator(trackingAllocator);

    producer = new TestProducer();
    producer.allocatorHolder = allocatorHolder;
    producer.contextProvider = ctx -> ResolvedCallContext.unauthenticated();
    producer.flightExecutor = flightExecutor;
    producer.setPlanFactory(this::defaultPlanFactory);
  }

  @AfterEach
  void tearDown() {
    flightExecutor.executor().shutdownNow();
    trackingAllocator.close();
  }

  @Test
  void childAllocatorClosedAfterSuccess() throws Exception {
    TestServerStreamListener listener = new TestServerStreamListener();
    producer.getStream(null, ticket(COMMAND), listener);
    assertTrue(listener.awaitCompletion(), "stream should finish");
    assertChildAllocatorClosed("child allocator closed");
  }

  @Test
  void childAllocatorClosedAfterCancellation() throws Exception {
    TestServerStreamListener listener = new TestServerStreamListener();
    listener.setReady(false);
    producer.getStream(null, ticket(COMMAND), listener);
    listener.cancel();
    assertTrue(listener.awaitError(), "stream should fail after cancel");
    assertChildAllocatorClosed("child allocator closed after cancel");
  }

  @Test
  void childAllocatorClosedWhenBuildPlanThrows() throws Exception {
    producer.setPlanFactory(
        (cmd, ctx, allocator) -> {
          throw new IllegalStateException("boom");
        });
    TestServerStreamListener listener = new TestServerStreamListener();
    producer.getStream(null, ticket(COMMAND), listener);
    assertTrue(listener.awaitError(), "error should propagate");
    assertChildAllocatorClosed("child allocator closed on error");
  }

  @Test
  void descriptorNotCommandResultsInInvalidArgument() {
    FlightDescriptor descriptor = FlightDescriptor.path("sys", "test");
    FlightRuntimeException ex =
        assertThrows(FlightRuntimeException.class, () -> producer.getFlightInfo(null, descriptor));
    assertEquals(FlightStatusCode.INVALID_ARGUMENT, ex.status().code());
  }

  @Test
  void ticketWrongVersionResultsInInvalidArgument() throws Exception {
    Ticket ticket =
        new Ticket(
            SystemTableFlightTicket.newBuilder()
                .setVersion(99)
                .setCmd(COMMAND.toByteString())
                .build()
                .toByteArray());
    TestServerStreamListener listener = new TestServerStreamListener();
    producer.getStream(null, ticket, listener);
    assertTrue(listener.awaitError());
    assertEquals(FlightStatusCode.INVALID_ARGUMENT, listener.failureStatus().status().code());
  }

  @Test
  void malformedTicketResultsInInvalidArgument() throws Exception {
    TestServerStreamListener listener = new TestServerStreamListener();
    producer.getStream(null, new Ticket(new byte[] {1, 2, 3}), listener);
    assertTrue(listener.awaitError());
    assertEquals(FlightStatusCode.INVALID_ARGUMENT, listener.failureStatus().status().code());
  }

  private ArrowScanPlan defaultPlanFactory(
      SystemTableFlightCommand command, ResolvedCallContext ctx, BufferAllocator allocator) {
    VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator);
    root.allocateNew();
    root.setRowCount(0);
    return ArrowScanPlan.of(SCHEMA, Stream.of(new SimpleColumnarBatch(root)));
  }

  private static Ticket ticket(SystemTableFlightCommand command) {
    return FlightTestHelpers.ticket(command);
  }

  private static final class TestProducer extends SystemTableFlightProducerBase {

    private static final Location LOCATION = Location.forGrpcInsecure("localhost", 1);
    private PlanFactory planFactory;

    void setPlanFactory(PlanFactory planFactory) {
      this.planFactory = planFactory;
    }

    @Override
    protected List<SchemaColumn> resolveSchemaColumns(
        SystemTableFlightCommand command, ResolvedCallContext ctx) {
      return List.of(SCHEMA_COLUMN);
    }

    @Override
    protected ArrowScanPlan buildPlan(
        SystemTableFlightCommand command, ResolvedCallContext ctx, BufferAllocator allocator) {
      return planFactory.apply(command, ctx, allocator);
    }

    @Override
    protected Location selfLocation() {
      return LOCATION;
    }

    @Override
    protected boolean supportsCommand(SystemTableFlightCommand command) {
      return true;
    }
  }

  @FunctionalInterface
  private interface PlanFactory {
    ArrowScanPlan apply(
        SystemTableFlightCommand command, ResolvedCallContext ctx, BufferAllocator allocator);
  }

  private static final class TestServerStreamListener implements ServerStreamListener {

    private final CountDownLatch completion = new CountDownLatch(1);
    private final CountDownLatch error = new CountDownLatch(1);
    private volatile FlightRuntimeException failure;
    private volatile Runnable cancelHandler;
    private final AtomicReference<Boolean> ready = new AtomicReference<>(true);
    private volatile boolean cancelled;

    @Override
    public void putNext() {}

    @Override
    public void putNext(ArrowBuf batch) {
      putNext();
    }

    @Override
    public void error(Throwable error) {
      if (error instanceof FlightRuntimeException fre) {
        this.failure = fre;
      } else {
        this.failure = CallStatus.INTERNAL.withDescription(error.getMessage()).toRuntimeException();
      }
      this.error.countDown();
    }

    @Override
    public void completed() {
      completion.countDown();
    }

    @Override
    public boolean isCancelled() {
      return cancelled;
    }

    @Override
    public void setOnCancelHandler(Runnable handler) {
      this.cancelHandler = handler;
    }

    void cancel() {
      cancelled = true;
      ready.set(false);
      if (cancelHandler != null) {
        cancelHandler.run();
      }
    }

    boolean awaitCompletion() throws InterruptedException {
      return completion.await(5, TimeUnit.SECONDS);
    }

    boolean awaitError() throws InterruptedException {
      return error.await(5, TimeUnit.SECONDS);
    }

    void setReady(boolean value) {
      ready.set(value);
    }

    @Override
    public boolean isReady() {
      return ready.get();
    }

    @Override
    public void start(VectorSchemaRoot root, DictionaryProvider provider, IpcOption option) {}

    @Override
    public void putMetadata(ArrowBuf metadata) {}

    FlightRuntimeException failureStatus() {
      return failure;
    }
  }

  private static final class TrackingAllocator extends RootAllocator {

    private final List<BufferAllocator> children = new ArrayList<>();

    TrackingAllocator(long limit) {
      super(limit);
    }

    @Override
    public BufferAllocator newChildAllocator(String name, long reservation, long limit) {
      BufferAllocator child = super.newChildAllocator(name, reservation, limit);
      children.add(child);
      return child;
    }

    BufferAllocator lastChild() {
      return children.isEmpty() ? null : children.get(children.size() - 1);
    }
  }

  private void assertChildAllocatorClosed(String message) {
    BufferAllocator child = trackingAllocator.lastChild();
    assertNotNull(child, message + " (no child allocator)");
    assertThrows(IllegalStateException.class, child::assertOpen, message + " (child still open)");
  }
}
