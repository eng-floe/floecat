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

import ai.floedb.floecat.arrow.ArrowBatchSerializer;
import ai.floedb.floecat.arrow.ArrowScanPlan;
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.system.rpc.SystemTableFlightCommand;
import ai.floedb.floecat.system.rpc.SystemTableFlightTicket;
import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

/**
 * Base for Flight producers that speak the {@link SystemTableFlightCommand} protocol.
 *
 * <p>Subclasses provide the table resource ID, the schema columns, and the plan for a command. This
 * class serializes commands/tickets and drives the shared {@link ArrowBatchSerializer} loop.
 */
public abstract class SystemTableFlightProducerBase extends NoOpFlightProducer
    implements RoutedFlightProducer {

  private static final Logger LOG = Logger.getLogger(SystemTableFlightProducerBase.class);
  private static final int TICKET_VERSION = 1;

  @Inject FlightAllocatorHolder allocatorHolder;
  @Inject FlightCallContextProvider contextProvider;
  @Inject FlightExecutor flightExecutor;

  /** Configurable cap (bytes) for the child allocator used during streaming. */
  @ConfigProperty(name = "ai.floedb.floecat.arrow.max-bytes", defaultValue = "1073741824")
  long arrowMaxBytes;

  // executor lifecycle handled by FlightExecutor

  // -----------------------------------------------------------------------
  // abstract contract
  // -----------------------------------------------------------------------

  /** Resolve the schema columns for the requested command. */
  protected abstract List<SchemaColumn> resolveSchemaColumns(
      SystemTableFlightCommand command, ResolvedCallContext ctx);

  /** Build the Arrow scan plan using the provided allocator. */
  protected abstract ArrowScanPlan buildPlan(
      SystemTableFlightCommand command, ResolvedCallContext ctx, BufferAllocator allocator);

  /** The location that should be advertised to clients for this producer. */
  protected abstract Location selfLocation();

  /** Hook for subclasses that need to enforce auth/permission checks. */
  protected void authorize(ResolvedCallContext ctx) {
    // no-op by default
  }

  /** Returns {@code true} when this producer can serve the provided command. */
  protected abstract boolean supportsCommand(SystemTableFlightCommand command);

  // -----------------------------------------------------------------------
  // routing
  // -----------------------------------------------------------------------

  @Override
  public boolean supportsDescriptor(FlightDescriptor descriptor) {
    return decodeCommandQuietly(descriptor).map(this::supportsCommand).orElse(false);
  }

  @Override
  public boolean supportsTicket(Ticket ticket) {
    return decodeTicketQuietly(ticket).map(this::supportsCommand).orElse(false);
  }

  // -----------------------------------------------------------------------
  // command ticket lifecycle
  // -----------------------------------------------------------------------

  @Override
  public final FlightInfo getFlightInfo(CallContext ctx, FlightDescriptor descriptor) {
    try {
      ResolvedCallContext callCtx = requireAuth(ctx);
      SystemTableFlightCommand command = decodeCommand(descriptor);
      Schema projected = projectSchema(command, callCtx);
      Ticket ticket = encodeTicket(command);
      FlightEndpoint endpoint = new FlightEndpoint(ticket, selfLocation());
      return FlightInfo.builder(projected, descriptor, List.of(endpoint)).build();
    } catch (Throwable t) {
      throw toFlightStatus(t).toRuntimeException();
    }
  }

  @Override
  public final void getStream(CallContext ctx, Ticket ticket, ServerStreamListener listener) {
    try {
      ResolvedCallContext callCtx = requireAuth(ctx);
      SystemTableFlightCommand command = decodeTicket(ticket);
      BufferAllocator allocator = createStreamAllocator();
      Map<String, Object> mdcSnapshot = copyContext(MDC.getMap());
      try {
        flightExecutor
            .executor()
            .submit(
                () -> {
                  Map<String, Object> previous = copyContext(MDC.getMap());
                  try {
                    applyContext(mdcSnapshot);
                    stream(command, listener, callCtx, allocator);
                  } catch (Throwable t) {
                    if (t instanceof FlightRuntimeException fre) {
                      listener.error(fre);
                    } else {
                      listener.error(toFlightStatus(t).toRuntimeException());
                    }
                  } finally {
                    applyContext(previous);
                  }
                });
      } catch (Throwable submitError) {
        closeAllocator(allocator);
        throw submitError;
      }
    } catch (Throwable t) {
      listener.error(toFlightStatus(t).toRuntimeException());
    }
  }

  protected void stream(
      SystemTableFlightCommand command,
      ServerStreamListener listener,
      ResolvedCallContext ctx,
      BufferAllocator allocator) {
    try {
      ArrowScanPlan plan = buildPlan(command, ctx, allocator);
      streamPlan(plan, listener, allocator, ctx);
    } catch (Throwable t) {
      closeAllocator(allocator);
      throw t;
    }
  }

  protected void streamPlan(
      ArrowScanPlan plan,
      ServerStreamListener listener,
      BufferAllocator allocator,
      ResolvedCallContext ctx) {

    FlightArrowBatchSink sink = new FlightArrowBatchSink(listener, allocator);
    AtomicBoolean cleanupCalled = new AtomicBoolean(false);
    AtomicBoolean shouldSignalCompletion = new AtomicBoolean(false);
    FlightRuntimeException[] caughtError = {null};
    FlightRuntimeException[] cancellationError = {null};
    Runnable cleanup =
        () -> {
          if (cleanupCalled.compareAndSet(false, true)) {
            sink.close();
          }
        };
    try {
      ArrowBatchSerializer.serialize(plan, sink, listener::isCancelled, cleanup);
      shouldSignalCompletion.set(true);
    } catch (Throwable t) {
      if (!listener.isCancelled()) {
        caughtError[0] =
            t instanceof FlightRuntimeException
                ? (FlightRuntimeException) t
                : toFlightStatus(t).toRuntimeException();
      }
    } finally {
      if (listener.isCancelled()) {
        cancellationError[0] =
            CallStatus.CANCELLED.withDescription("Flight stream cancelled").toRuntimeException();
      }
      // Ensure sink is properly closed before closing allocator
      if (cleanupCalled.compareAndSet(false, true)) {
        sink.close();
      }
      // Always close allocator after all other cleanup
      closeAllocator(allocator);
      // Signal errors AFTER allocator is closed
      if (cancellationError[0] != null) {
        listener.error(cancellationError[0]);
      } else if (caughtError[0] != null) {
        listener.error(caughtError[0]);
      }
      // Signal completion AFTER allocator is closed
      if (shouldSignalCompletion.get()) {
        listener.completed();
      }
    }
  }

  // -----------------------------------------------------------------------
  // helpers
  // -----------------------------------------------------------------------

  protected Schema projectSchema(SystemTableFlightCommand command, ResolvedCallContext ctx) {
    List<SchemaColumn> columns = resolveSchemaColumns(command, ctx);
    return ArrowBatchSerializer.schemaForColumns(columns, command.getRequiredColumnsList());
  }

  protected final ResolvedCallContext requireAuth(CallContext context) {
    ResolvedCallContext resolved = resolvedCallContext(context);
    authorize(resolved);
    return resolved;
  }

  protected static List<SchemaColumn> schemaColumnsFromSchema(Schema schema) {
    if (schema == null) {
      return List.of();
    }
    List<SchemaColumn> cols = new ArrayList<>(schema.getFields().size());
    int ordinal = 1;
    for (Field field : schema.getFields()) {
      cols.add(
          SchemaColumn.newBuilder()
              .setName(field.getName())
              .setLogicalType(field.getFieldType().getType().toString())
              .setOrdinal(ordinal++)
              .build());
    }
    return cols;
  }

  private ResolvedCallContext resolvedCallContext(CallContext context) {
    ResolvedCallContext resolved = contextProvider.resolve(context);
    return resolved != null ? resolved : ResolvedCallContext.unauthenticated();
  }

  private BufferAllocator createStreamAllocator() {
    long cap = arrowMaxBytes > 0 ? arrowMaxBytes : Long.MAX_VALUE;
    return allocatorHolder.allocator().newChildAllocator("flight-stream", 0, cap);
  }

  private void closeAllocator(BufferAllocator allocator) {
    try {
      allocator.close();
    } catch (Exception ignored) {
      // May throw if memory is leaked, but allocator is still marked for closure
    }
  }

  private static SystemTableFlightCommand decodeCommand(FlightDescriptor descriptor) {
    if (descriptor == null || !descriptor.isCommand()) {
      throw CallStatus.INVALID_ARGUMENT
          .withDescription("FlightDescriptor must be a command")
          .toRuntimeException();
    }
    try {
      return SystemTableFlightCommand.parseFrom(descriptor.getCommand());
    } catch (InvalidProtocolBufferException e) {
      throw CallStatus.INVALID_ARGUMENT
          .withDescription("Cannot decode SystemTableFlightCommand: " + e.getMessage())
          .toRuntimeException();
    }
  }

  private static Ticket encodeTicket(SystemTableFlightCommand command) {
    SystemTableFlightTicket ticket =
        SystemTableFlightTicket.newBuilder()
            .setVersion(TICKET_VERSION)
            .setCmd(command.toByteString())
            .build();
    return new Ticket(ticket.toByteArray());
  }

  private static SystemTableFlightCommand decodeTicket(Ticket ticket) {
    try {
      SystemTableFlightTicket wrapper = SystemTableFlightTicket.parseFrom(ticket.getBytes());
      if (wrapper.getVersion() != TICKET_VERSION) {
        throw CallStatus.INVALID_ARGUMENT
            .withDescription("Unsupported ticket version: " + wrapper.getVersion())
            .toRuntimeException();
      }
      return SystemTableFlightCommand.parseFrom(wrapper.getCmd());
    } catch (InvalidProtocolBufferException e) {
      throw CallStatus.INVALID_ARGUMENT
          .withDescription("Cannot decode Flight ticket: " + e.getMessage())
          .toRuntimeException();
    }
  }

  protected Optional<SystemTableFlightCommand> decodeTicketQuietly(Ticket ticket) {
    try {
      return Optional.of(decodeTicket(ticket));
    } catch (Throwable t) {
      return Optional.empty();
    }
  }

  protected Optional<SystemTableFlightCommand> decodeCommandQuietly(FlightDescriptor descriptor) {
    try {
      return Optional.of(decodeCommand(descriptor));
    } catch (Throwable t) {
      return Optional.empty();
    }
  }

  protected static CallStatus toFlightStatus(Throwable t) {
    if (t instanceof io.grpc.StatusRuntimeException) {
      io.grpc.StatusRuntimeException sre = (io.grpc.StatusRuntimeException) t;
      switch (sre.getStatus().getCode()) {
        case NOT_FOUND:
          return CallStatus.NOT_FOUND.withDescription(sre.getMessage());
        case INVALID_ARGUMENT:
          return CallStatus.INVALID_ARGUMENT.withDescription(sre.getMessage());
        case UNAUTHENTICATED:
          return CallStatus.UNAUTHENTICATED.withDescription(sre.getMessage());
        case PERMISSION_DENIED:
          return CallStatus.UNAUTHORIZED.withDescription(sre.getMessage());
        case UNAVAILABLE:
          return CallStatus.UNAVAILABLE.withDescription(sre.getMessage());
        default:
          return CallStatus.INTERNAL.withDescription(sre.getMessage()).withCause(sre);
      }
    }
    if (t instanceof FlightRuntimeException) {
      return ((FlightRuntimeException) t).status();
    }
    return CallStatus.INTERNAL.withDescription(t.getMessage()).withCause(t);
  }

  private static Map<String, Object> copyContext(Map<String, Object> source) {
    if (source == null || source.isEmpty()) {
      return Map.of();
    }
    return new HashMap<>(source);
  }

  private static void applyContext(Map<String, Object> context) {
    MDC.clear();
    if (context == null) {
      return;
    }
    context.forEach((k, v) -> MDC.put(k, v == null ? "" : v.toString()));
  }
}
