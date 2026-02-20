package ai.floedb.floecat.flight;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.List;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;

/**
 * Delegates Flight calls to the first {@link RoutedFlightProducer} that claims the incoming data.
 *
 * <p>The router enforces that exactly one producer matches a descriptor or ticket. When more than
 * one producer claims the same descriptor/ticket, the router fails with {@link
 * CallStatus#FAILED_PRECONDITION FAILED_PRECONDITION}. Producers must ensure their {@link
 * RoutedFlightProducer} beans are mutually exclusive for each supported command (e.g., via unique
 * {@code table_id} values or scoped/qualified beans). Explicit ordering (e.g., via
 * {@code @Priority}) is not supported; the first producer declared is not relied upon.
 */
@ApplicationScoped
public final class RouterFlightProducer extends NoOpFlightProducer implements RoutedFlightProducer {

  @Inject Instance<RoutedFlightProducer> producers;

  private List<RoutedFlightProducer> delegates;

  @Inject
  public RouterFlightProducer() {
    // No-arg constructor for CDI proxying
  }

  public RouterFlightProducer(List<RoutedFlightProducer> delegates) {
    this.delegates = List.copyOf(delegates);
  }

  @PostConstruct
  void init() {
    if (delegates == null) {
      // We’re in CDI: build the list from the injected Instance, filtering out self
      this.delegates =
          producers.stream()
              .filter(p -> p != this)
              .filter(p -> !RouterFlightProducer.class.isAssignableFrom(p.getClass()))
              .toList();
    }
  }

  @Override
  public FlightInfo getFlightInfo(CallContext ctx, FlightDescriptor descriptor) {
    RoutedFlightProducer delegate =
        selectSingle(
            p -> p.supportsDescriptor(descriptor),
            "No Flight producer registered for descriptor",
            "Multiple Flight producers match descriptor");
    return delegate.getFlightInfo(ctx, descriptor);
  }

  @Override
  public void getStream(CallContext ctx, Ticket ticket, ServerStreamListener listener) {
    RoutedFlightProducer delegate =
        selectSingle(
            p -> p.supportsTicket(ticket),
            "No Flight producer registered for ticket",
            "Multiple Flight producers match ticket");
    delegate.getStream(ctx, ticket, listener);
  }

  @Override
  public boolean supportsDescriptor(FlightDescriptor descriptor) {
    return delegates.stream().anyMatch(p -> p.supportsDescriptor(descriptor));
  }

  @Override
  public boolean supportsTicket(Ticket ticket) {
    return delegates.stream().anyMatch(p -> p.supportsTicket(ticket));
  }

  private RoutedFlightProducer selectSingle(
      java.util.function.Predicate<RoutedFlightProducer> filter,
      String notFoundMessage,
      String multipleMessage) {
    List<RoutedFlightProducer> matches = delegates.stream().filter(filter).toList();
    if (matches.isEmpty()) {
      throw CallStatus.NOT_FOUND.withDescription(notFoundMessage).toRuntimeException();
    }
    if (matches.size() > 1) {
      String names =
          matches.stream().map(p -> p.getClass().getName()).distinct().toList().toString();
      throw CallStatus.INVALID_ARGUMENT
          .withDescription(String.format("%s: %s", multipleMessage, names))
          .toRuntimeException();
    }
    return matches.get(0);
  }
}
