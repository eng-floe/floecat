package ai.floedb.metacat.trino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TimeZoneKey;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class MetacatSessionPropertiesTest {

  @Test
  void returnsOptionalsForSnapshotAndAsOf() {
    ConnectorSession session =
        new TestSession(
            Map.of(
                MetacatSessionProperties.SNAPSHOT_ID, 77L,
                MetacatSessionProperties.AS_OF_EPOCH_MILLIS, -1L));

    assertEquals(77L, MetacatSessionProperties.getSnapshotId(session).orElseThrow());
    assertTrue(MetacatSessionProperties.getAsOfEpochMillis(session).isEmpty());
  }

  @Test
  void emptyWhenUnset() {
    ConnectorSession session =
        new TestSession(
            Map.of(
                MetacatSessionProperties.SNAPSHOT_ID, -1L,
                MetacatSessionProperties.AS_OF_EPOCH_MILLIS, -1L));

    assertTrue(MetacatSessionProperties.getSnapshotId(session).isEmpty());
    assertTrue(MetacatSessionProperties.getAsOfEpochMillis(session).isEmpty());
  }

  private static class TestSession implements ConnectorSession {
    private final Map<String, Object> properties;

    TestSession(Map<String, Object> properties) {
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
}
