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

package ai.floedb.floecat.service.it.support;

import io.quarkus.test.junit.callback.QuarkusTestBeforeEachCallback;
import io.quarkus.test.junit.callback.QuarkusTestMethodContext;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;

public class GrpcServiceReadyCallback implements QuarkusTestBeforeEachCallback {
  private static final Duration READY_TIMEOUT = Duration.ofSeconds(5);
  private static final long READY_POLL_MS = 100L;
  private static final int CONNECT_TIMEOUT_MS = 100;

  @Override
  public void beforeEach(QuarkusTestMethodContext context) {
    if (!isIntegrationTest(context.getTestInstance().getClass().getName())) {
      return;
    }
    awaitGrpcPort();
  }

  private static boolean isIntegrationTest(String className) {
    return className.startsWith("ai.floedb.floecat.service.it.")
        || className.startsWith("ai.floedb.floecat.service.constraints.it.")
        || className.startsWith("ai.floedb.floecat.service.catalog.it.")
        || className.startsWith("ai.floedb.floecat.service.telemetry.");
  }

  private static void awaitGrpcPort() {
    String configuredHost = System.getProperty("quarkus.grpc.clients.floecat.host", "localhost");
    String host =
        "localhost".equalsIgnoreCase(configuredHost)
            ? InetAddress.getLoopbackAddress().getHostAddress()
            : configuredHost;
    int port = Integer.getInteger("quarkus.grpc.clients.floecat.port", 9101);
    long deadlineNanos = System.nanoTime() + READY_TIMEOUT.toNanos();
    while (System.nanoTime() < deadlineNanos) {
      try (Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
        return;
      } catch (IOException ignored) {
        try {
          Thread.sleep(READY_POLL_MS);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Interrupted while waiting for gRPC service readiness");
        }
      }
    }
    throw new IllegalStateException(
        "Timed out waiting for gRPC service readiness on " + host + ":" + port);
  }
}
