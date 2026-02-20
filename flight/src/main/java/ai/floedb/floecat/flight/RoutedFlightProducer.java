package ai.floedb.floecat.flight;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.Ticket;

/** Flight producer that can claim ownership of descriptors/tickets. */
public interface RoutedFlightProducer extends FlightProducer {
  boolean supportsDescriptor(FlightDescriptor descriptor);

  boolean supportsTicket(Ticket ticket);
}
