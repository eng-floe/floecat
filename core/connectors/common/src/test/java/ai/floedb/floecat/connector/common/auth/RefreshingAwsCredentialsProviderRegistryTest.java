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

package ai.floedb.floecat.connector.common.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;

class RefreshingAwsCredentialsProviderRegistryTest {
  @Test
  void cachesTerminalRefreshFailure() {
    String providerId = "terminal-refresh-test";
    AtomicInteger refreshes = new AtomicInteger();
    TerminalCredentialRefreshException terminal =
        new TerminalCredentialRefreshException("lease lost", new IllegalStateException("stale"));
    try {
      RefreshingAwsCredentialsProviderRegistry.register(
          providerId,
          credentials("old", Instant.now().minusSeconds(1)),
          () -> {
            refreshes.incrementAndGet();
            throw terminal;
          },
          Duration.ZERO);

      assertSame(
          terminal,
          assertThrows(
              TerminalCredentialRefreshException.class,
              () -> RefreshingAwsCredentialsProviderRegistry.resolve(providerId)));
      assertSame(
          terminal,
          assertThrows(
              TerminalCredentialRefreshException.class,
              () -> RefreshingAwsCredentialsProviderRegistry.resolve(providerId)));
      assertEquals(1, refreshes.get());
    } finally {
      RefreshingAwsCredentialsProviderRegistry.unregister(providerId);
    }
  }

  @Test
  void logsSuccessfulCredentialRefreshAtInfo() {
    String providerId = "refresh-log-test";
    Instant previousExpiry = Instant.now().minusSeconds(1);
    Instant refreshedExpiry = Instant.now().plusSeconds(3600);
    AtomicReference<LogRecord> refreshLog = new AtomicReference<>();
    Logger logger = Logger.getLogger(RefreshingAwsCredentialsProviderRegistry.class.getName());
    Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            if (record != null
                && record.getMessage() != null
                && record.getMessage().startsWith("Refreshed AWS storage credentials")) {
              refreshLog.set(record);
            }
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    logger.addHandler(handler);
    try {
      RefreshingAwsCredentialsProviderRegistry.register(
          providerId,
          credentials("old", previousExpiry),
          () -> credentials("new", refreshedExpiry),
          Duration.ZERO);

      RefreshingAwsCredentialsProviderRegistry.resolve(providerId);

      LogRecord record = refreshLog.get();
      assertNotNull(record);
      assertEquals(Level.INFO, record.getLevel());
      assertEquals(previousExpiry, record.getParameters()[0]);
      assertEquals(refreshedExpiry, record.getParameters()[1]);
    } finally {
      RefreshingAwsCredentialsProviderRegistry.unregister(providerId);
      logger.removeHandler(handler);
    }
  }

  private static ResolvedStorageCredentials credentials(String suffix, Instant expiresAt) {
    return new ResolvedStorageCredentials(
        "access-" + suffix, "secret-" + suffix, "token-" + suffix, expiresAt);
  }
}
