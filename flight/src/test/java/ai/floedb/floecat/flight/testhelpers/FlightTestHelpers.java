package ai.floedb.floecat.flight.testhelpers;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.system.rpc.SystemTableFlightCommand;
import ai.floedb.floecat.system.rpc.SystemTableFlightTicket;
import java.util.List;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/** Helpers shared by Flight-related unit tests. */
public final class FlightTestHelpers {

  private static final Schema SIMPLE_SCHEMA =
      new Schema(List.of(Field.nullable("col", new ArrowType.Int(32, false))));

  private FlightTestHelpers() {}

  public static SystemTableFlightCommand command(String tableId) {
    return SystemTableFlightCommand.newBuilder()
        .setTableId(ResourceId.newBuilder().setId(tableId).build())
        .build();
  }

  public static FlightDescriptor descriptor(String tableId) {
    return FlightDescriptor.command(command(tableId).toByteArray());
  }

  public static FlightDescriptor descriptor(SystemTableFlightCommand command) {
    return FlightDescriptor.command(command.toByteArray());
  }

  public static Ticket ticket(String tableId) {
    return ticket(command(tableId));
  }

  public static Ticket ticket(SystemTableFlightCommand command) {
    SystemTableFlightTicket wrapper =
        SystemTableFlightTicket.newBuilder().setVersion(1).setCmd(command.toByteString()).build();
    return new Ticket(wrapper.toByteArray());
  }

  public static Schema simpleSchema() {
    return SIMPLE_SCHEMA;
  }

  public static SchemaColumn schemaColumn(String name, ArrowType type, int ordinal) {
    return SchemaColumn.newBuilder()
        .setName(name)
        .setLogicalType(type.toString())
        .setOrdinal(ordinal)
        .build();
  }

  public static ServerStreamListener newNoOpListener() {
    return new NoOpListener();
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
    public void putNext(org.apache.arrow.memory.ArrowBuf batch) {}

    @Override
    public void putMetadata(org.apache.arrow.memory.ArrowBuf metadata) {}

    @Override
    public void start(VectorSchemaRoot root, DictionaryProvider provider, IpcOption option) {}

    @Override
    public boolean isReady() {
      return true;
    }
  }
}
