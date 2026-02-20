package ai.floedb.floecat.flight;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.system.rpc.SystemTableFlightCommand;
import ai.floedb.floecat.system.rpc.SystemTableFlightTicket;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

class RouterFlightProducerTest {

  @Test
  void routesDescriptorCallsToMatchingProducer() {
    FakeProducer descriptorProducer = new FakeProducer("sys.a");
    FakeProducer ticketProducer = new FakeProducer("sys.b");
    RouterFlightProducer router =
        new RouterFlightProducer(List.of(descriptorProducer, ticketProducer));

    FlightDescriptor descriptor = descriptorFor("sys.a");
    FlightInfo info = router.getFlightInfo(null, descriptor);

    assertThat(info).isSameAs(descriptorProducer.info);
    assertThat(descriptorProducer.infoCalled).isTrue();
    assertThat(descriptorProducer.streamCalled).isFalse();
  }

  @Test
  void routesTicketCallsToMatchingProducer() {
    FakeProducer descriptorProducer = new FakeProducer("sys.a");
    FakeProducer ticketProducer = new FakeProducer("sys.b");
    RouterFlightProducer router =
        new RouterFlightProducer(List.of(descriptorProducer, ticketProducer));

    Ticket ticket = ticketFor("sys.b");
    router.getStream(null, ticket, new NoOpListener());

    assertThat(ticketProducer.streamCalled).isTrue();
    assertThat(descriptorProducer.streamCalled).isFalse();
  }

  private static FlightDescriptor descriptorFor(String tableId) {
    SystemTableFlightCommand command =
        SystemTableFlightCommand.newBuilder()
            .setTableId(ResourceId.newBuilder().setId(tableId).build())
            .build();
    return FlightDescriptor.command(command.toByteArray());
  }

  private static Ticket ticketFor(String tableId) {
    SystemTableFlightTicket ticket =
        SystemTableFlightTicket.newBuilder()
            .setVersion(1)
            .setCmd(
                SystemTableFlightCommand.newBuilder()
                    .setTableId(ResourceId.newBuilder().setId(tableId).build())
                    .build()
                    .toByteString())
            .build();
    return new Ticket(ticket.toByteArray());
  }

  private static final class FakeProducer extends NoOpFlightProducer
      implements RoutedFlightProducer {

    final String tableId;
    final FlightInfo info;
    final AtomicBoolean infoCalled = new AtomicBoolean();
    final AtomicBoolean streamCalled = new AtomicBoolean();

    FakeProducer(String tableId) {
      this.tableId = tableId;
      Schema schema =
          new Schema(
              List.of(
                  new Field("col", FieldType.nullable(new ArrowType.Int(32, false)), List.of())));
      SystemTableFlightCommand command =
          SystemTableFlightCommand.newBuilder()
              .setTableId(ResourceId.newBuilder().setId(tableId).build())
              .build();
      this.info =
          FlightInfo.builder(
                  schema,
                  FlightDescriptor.command(command.toByteArray()),
                  List.of(
                      new FlightEndpoint(
                          new Ticket(
                              SystemTableFlightTicket.newBuilder()
                                  .setVersion(1)
                                  .setCmd(command.toByteString())
                                  .build()
                                  .toByteArray()),
                          Location.forGrpcInsecure("localhost", 0))))
              .build();
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
      infoCalled.set(true);
      return info;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      streamCalled.set(true);
      listener.completed();
    }

    @Override
    public boolean supportsDescriptor(FlightDescriptor descriptor) {
      SystemTableFlightCommand command = decodeCommand(descriptor);
      return command != null
          && command.hasTableId()
          && tableId.equals(command.getTableId().getId());
    }

    @Override
    public boolean supportsTicket(Ticket ticket) {
      SystemTableFlightCommand command = decodeTicket(ticket);
      return command != null
          && command.hasTableId()
          && tableId.equals(command.getTableId().getId());
    }

    private static SystemTableFlightCommand decodeCommand(FlightDescriptor descriptor) {
      if (descriptor == null || !descriptor.isCommand()) {
        return null;
      }
      try {
        return SystemTableFlightCommand.parseFrom(descriptor.getCommand());
      } catch (InvalidProtocolBufferException e) {
        return null;
      }
    }

    private static SystemTableFlightCommand decodeTicket(Ticket ticket) {
      if (ticket == null) {
        return null;
      }
      try {
        SystemTableFlightTicket wrapper = SystemTableFlightTicket.parseFrom(ticket.getBytes());
        if (wrapper.getVersion() != 1) {
          return null;
        }
        return SystemTableFlightCommand.parseFrom(wrapper.getCmd());
      } catch (InvalidProtocolBufferException e) {
        return null;
      }
    }
  }

  private static final class NoOpListener implements ServerStreamListener {
    @Override
    public void putNext() {}

    @Override
    public void error(Throwable error) {}

    @Override
    public void completed() {}

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public void setOnCancelHandler(Runnable handler) {}

    @Override
    public void putNext(ArrowBuf batch) {}

    @Override
    public void putMetadata(ArrowBuf metadata) {}

    @Override
    public void start(VectorSchemaRoot root, DictionaryProvider provider, IpcOption option) {}

    @Override
    public boolean isReady() {
      return true;
    }
  }
}
