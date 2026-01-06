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

package ai.floedb.floecat.client.trino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.session.PropertyMetadata;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class FloecatSessionPropertiesTest {

  @Test
  void returnsOptionalsForSnapshotAndAsOf() {
    ConnectorSession session =
        new TestSession(
            Map.of(
                FloecatSessionProperties.SNAPSHOT_ID, 77L,
                FloecatSessionProperties.AS_OF_EPOCH_MILLIS, -1L));

    assertEquals(77L, FloecatSessionProperties.getSnapshotId(session).orElseThrow());
    assertTrue(FloecatSessionProperties.getAsOfEpochMillis(session).isEmpty());
  }

  @Test
  void emptyWhenUnset() {
    ConnectorSession session =
        new TestSession(
            Map.of(
                FloecatSessionProperties.SNAPSHOT_ID, -1L,
                FloecatSessionProperties.AS_OF_EPOCH_MILLIS, -1L));

    assertTrue(FloecatSessionProperties.getSnapshotId(session).isEmpty());
    assertTrue(FloecatSessionProperties.getAsOfEpochMillis(session).isEmpty());
  }

  @Test
  void exposesExpectedDefaults() {
    FloecatSessionProperties properties = new FloecatSessionProperties();
    Map<String, PropertyMetadata<?>> byName =
        properties.getSessionProperties().stream()
            .collect(Collectors.toMap(PropertyMetadata::getName, p -> p));

    assertEquals(-1L, byName.get(FloecatSessionProperties.SNAPSHOT_ID).getDefaultValue());
    assertEquals(-1L, byName.get(FloecatSessionProperties.AS_OF_EPOCH_MILLIS).getDefaultValue());
    assertEquals(
        Boolean.TRUE,
        byName.get(FloecatSessionProperties.USE_FILE_SIZE_FROM_METADATA).getDefaultValue());
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
