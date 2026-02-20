package ai.floedb.floecat.service.flight;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.flight.context.CallContextResolver;
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.context.flight.InboundContextFlightMiddleware;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.junit.jupiter.api.Test;

final class ServiceFlightCallContextProviderTest {

  private final ServiceFlightCallContextProvider provider = new ServiceFlightCallContextProvider();

  @Test
  void resolvesContextFromMiddleware() {
    ResolvedCallContext expected =
        new ResolvedCallContext(
            PrincipalContext.newBuilder().setAccountId("acct").setSubject("user").build(),
            "query-1",
            "corr-1",
            EngineContext.empty(),
            "session-header",
            "authorization-header");
    CallContextResolver resolver = (headers, allowUnauthenticated) -> expected;
    InboundContextFlightMiddleware middleware =
        new InboundContextFlightMiddleware.Factory(resolver)
            .onCallStarted(null, new TestCallHeaders(), null);
    FlightProducer.CallContext context =
        new TestCallContext(Map.of(InboundContextFlightMiddleware.KEY, middleware));

    assertSame(expected, provider.resolve(context));
  }

  @Test
  void returnsUnauthenticatedWhenMiddlewareMissing() {
    FlightProducer.CallContext context = new TestCallContext(Map.of());
    assertEquals(ResolvedCallContext.unauthenticated(), provider.resolve(context));
  }

  private static final class TestCallContext implements FlightProducer.CallContext {
    private final Map<FlightServerMiddleware.Key<?>, FlightServerMiddleware> middleware;

    private TestCallContext(Map<FlightServerMiddleware.Key<?>, FlightServerMiddleware> middleware) {
      this.middleware = Map.copyOf(middleware);
    }

    @Override
    public String peerIdentity() {
      return "";
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends FlightServerMiddleware> T getMiddleware(FlightServerMiddleware.Key<T> key) {
      return (T) middleware.get(key);
    }

    @Override
    public Map<FlightServerMiddleware.Key<?>, FlightServerMiddleware> getMiddleware() {
      return middleware;
    }
  }

  private static final class TestCallHeaders implements CallHeaders {
    private final Map<String, String> values = new HashMap<>();
    private final Map<String, byte[]> bytes = new HashMap<>();

    @Override
    public String get(String key) {
      return values.get(key);
    }

    @Override
    public byte[] getByte(String key) {
      return bytes.get(key);
    }

    @Override
    public Iterable<String> getAll(String key) {
      String value = values.get(key);
      return value == null ? List.of() : List.of(value);
    }

    @Override
    public Iterable<byte[]> getAllByte(String key) {
      byte[] value = bytes.get(key);
      return value == null ? List.of() : List.of(value);
    }

    @Override
    public void insert(String key, String value) {
      values.put(key, value);
    }

    @Override
    public void insert(String key, byte[] value) {
      bytes.put(key, value);
    }

    @Override
    public Set<String> keys() {
      return Collections.unmodifiableSet(values.keySet());
    }

    @Override
    public boolean containsKey(String key) {
      return values.containsKey(key);
    }
  }
}
