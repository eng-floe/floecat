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
import ai.floedb.floecat.flight.RouterFlightProducer;
import ai.floedb.floecat.service.context.flight.CallContextResolverAdapter;
import ai.floedb.floecat.service.context.flight.InboundContextFlightMiddleware;
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

@ApplicationScoped
public class FlightServerLifecycle {

  private static final Logger LOG = Logger.getLogger(FlightServerLifecycle.class);

  @Inject RouterFlightProducer producer;
  @Inject AccountRepository accountRepository;
  @Inject TenantIdentityProvider identityProvider;
  @Inject FlightAllocatorHolder allocatorHolder;

  @ConfigProperty(name = "floecat.flight.port", defaultValue = "47470")
  int flightPort;

  @ConfigProperty(name = "floecat.flight.tls", defaultValue = "false")
  boolean flightTls;

  @ConfigProperty(name = "floecat.flight.memory.max-bytes", defaultValue = "0")
  long flightMemoryMaxBytes;

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
                  new InboundContextFlightMiddleware.Factory(
                      new CallContextResolverAdapter(contextHelper)))
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

  public int port() {
    return server != null ? server.getPort() : -1;
  }
}
