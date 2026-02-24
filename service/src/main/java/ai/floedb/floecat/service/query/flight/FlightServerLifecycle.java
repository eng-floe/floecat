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

package ai.floedb.floecat.service.query.flight;

import ai.floedb.floecat.flight.FlightAllocatorHolder;
import ai.floedb.floecat.service.context.impl.InboundCallContextHelper;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import io.quarkus.oidc.TenantIdentityProvider;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * Quarkus lifecycle bean that starts and stops the Arrow Flight server.
 *
 * <p>The server listens on {@code floecat.flight.port} (default {@code 47470}) and binds to {@code
 * 0.0.0.0}.
 *
 * <p>Auth is enforced via {@link InboundContextFlightMiddleware}, which shares the same {@link
 * InboundCallContextHelper} logic as the gRPC {@code InboundContextInterceptor}. Auth configuration
 * ({@code floecat.auth.mode}, session/authorization headers, OIDC) is read from the same properties
 * as the gRPC path.
 */
@ApplicationScoped
public class FlightServerLifecycle {

  private static final Logger LOG = Logger.getLogger(FlightServerLifecycle.class);

  @Inject SystemTableFlightProducer producer;
  @Inject AccountRepository accountRepository;
  @Inject TenantIdentityProvider identityProvider;

  @ConfigProperty(name = "floecat.flight.port", defaultValue = "47470")
  int flightPort;

  @ConfigProperty(name = "floecat.flight.tls", defaultValue = "false")
  boolean flightTls;

  @ConfigProperty(name = "floecat.flight.memory.max-bytes", defaultValue = "0")
  long flightMemoryMaxBytes;

  // Auth config — same properties as BlockingInboundContextInterceptor
  @ConfigProperty(name = "floecat.interceptor.validate.account", defaultValue = "true")
  boolean validateAccount;

  @ConfigProperty(name = "floecat.interceptor.session.header")
  Optional<String> sessionHeader;

  @ConfigProperty(name = "floecat.interceptor.authorization.header")
  Optional<String> authorizationHeader;

  @ConfigProperty(name = "floecat.auth.mode", defaultValue = "oidc")
  String authMode;

  @ConfigProperty(name = "floecat.interceptor.session.account-claim", defaultValue = "account_id")
  String accountClaimName;

  @ConfigProperty(name = "floecat.interceptor.session.role-claim", defaultValue = "roles")
  String roleClaimName;

  private FlightServer server;
  private BufferAllocator allocator;

  @Inject FlightAllocatorHolder allocatorHolder;

  void onStart(@Observes StartupEvent event) {
    InboundCallContextHelper contextHelper =
        new InboundCallContextHelper(
            accountRepository,
            identityProvider,
            validateAccount,
            sessionHeader,
            authorizationHeader,
            authMode,
            accountClaimName,
            roleClaimName);

    if (flightTls) {
      throw new IllegalStateException(
          "Arrow Flight TLS is not yet supported; set floecat.flight.tls=false");
    }
    long parentCap = flightMemoryMaxBytes > 0 ? flightMemoryMaxBytes : Long.MAX_VALUE;
    allocator = new RootAllocator(parentCap);
    allocatorHolder.setAllocator(allocator);
    Location location = Location.forGrpcInsecure("0.0.0.0", flightPort);
    try {
      server =
          FlightServer.builder(allocator, location, producer)
              .middleware(
                  InboundContextFlightMiddleware.KEY,
                  new InboundContextFlightMiddleware.Factory(contextHelper))
              .build();
      server.start();
      LOG.infof("Arrow Flight server started on port %d (authMode=%s)", flightPort, authMode);
    } catch (IOException e) {
      allocator.close();
      throw new UncheckedIOException(
          "Failed to start Arrow Flight server on port " + flightPort, e);
    }
  }

  void onStop(@Observes ShutdownEvent event) {
    if (server != null) {
      try {
        server.close();
        LOG.info("Arrow Flight server stopped");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Interrupted while stopping Arrow Flight server", e);
      }
    }
    if (allocator != null) {
      try {
        allocator.close();
      } catch (Exception e) {
        LOG.warn("Error closing Flight server allocator", e);
      } finally {
        allocatorHolder.clear();
      }
    }
  }

  /** Returns the port the Flight server is bound to (useful for tests). */
  public int port() {
    return server != null ? server.getPort() : -1;
  }
}
