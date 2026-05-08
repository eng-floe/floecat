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

package ai.floedb.floecat.reconciler.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class OidcReconcileWorkerAuthProviderTest {

  @Test
  void cachesTokenUntilRefreshWindow() {
    MutableClock clock = new MutableClock(Instant.parse("2026-05-07T12:00:00Z"));
    AtomicInteger calls = new AtomicInteger();
    OidcReconcileWorkerAuthProvider provider =
        new OidcReconcileWorkerAuthProvider(
            Optional.of("http://issuer"),
            Optional.of("worker-client"),
            Optional.of("worker-secret"),
            30,
            Duration.ofSeconds(10),
            clock,
            (endpoint, requestBody, connectTimeout) ->
                new OidcReconcileWorkerAuthProvider.TokenResponse(
                    "token-" + calls.incrementAndGet(), 120));

    assertThat(provider.authorizationHeader()).contains("Bearer token-1");
    clock.advanceSeconds(60);
    assertThat(provider.authorizationHeader()).contains("Bearer token-1");
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void refreshesTokenBeforeExpiryUsingConfiguredSkew() {
    MutableClock clock = new MutableClock(Instant.parse("2026-05-07T12:00:00Z"));
    AtomicInteger calls = new AtomicInteger();
    OidcReconcileWorkerAuthProvider provider =
        new OidcReconcileWorkerAuthProvider(
            Optional.of("http://issuer"),
            Optional.of("worker-client"),
            Optional.of("worker-secret"),
            30,
            Duration.ofSeconds(10),
            clock,
            (endpoint, requestBody, connectTimeout) ->
                new OidcReconcileWorkerAuthProvider.TokenResponse(
                    "token-" + calls.incrementAndGet(), 120));

    assertThat(provider.authorizationHeader()).contains("Bearer token-1");
    clock.advanceSeconds(91);
    assertThat(provider.authorizationHeader()).contains("Bearer token-2");
    assertThat(calls.get()).isEqualTo(2);
  }

  @Test
  void parsesAccessTokenAndExpiryFromTokenResponse() {
    assertThat(
            OidcReconcileWorkerAuthProvider.parseTokenResponse(
                "{\"access_token\":\"abc\",\"expires_in\":120}"))
        .isEqualTo(new OidcReconcileWorkerAuthProvider.TokenResponse("abc", 120));
  }

  private static final class MutableClock extends Clock {
    private Instant current;

    private MutableClock(Instant current) {
      this.current = current;
    }

    @Override
    public ZoneOffset getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(java.time.ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return current;
    }

    private void advanceSeconds(long seconds) {
      current = current.plusSeconds(seconds);
    }
  }
}
