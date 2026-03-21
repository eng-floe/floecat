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

import ai.floedb.floecat.flight.FlightExecutor;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.quarkus.grpc.GrpcService;
import jakarta.inject.Inject;
import java.lang.reflect.Constructor;
import java.util.concurrent.ExecutorService;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.memory.BufferAllocator;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/** Registers Arrow Flight on Floecat's shared Quarkus gRPC server. */
@GrpcService
public final class FlightBindableService implements BindableService {

  private static final Logger LOG = Logger.getLogger(FlightBindableService.class);

  private final BindableService delegate;

  @Inject
  public FlightBindableService(
      FlightServerAllocator allocatorProvider,
      FlightExecutor flightExecutor,
      SystemTableFlightProducer producer,
      @ConfigProperty(name = "quarkus.grpc.server.port") int grpcPort,
      @ConfigProperty(name = "floecat.flight.host", defaultValue = "localhost")
          String flightAdvertisedHost,
      @ConfigProperty(name = "floecat.flight.port") int flightAdvertisedPort,
      @ConfigProperty(name = "floecat.auth.mode", defaultValue = "oidc") String authMode) {
    this.delegate =
        createDelegate(
            allocatorProvider.allocator(),
            producer,
            ServerAuthHandler.NO_OP,
            flightExecutor.executor());
    LOG.infof(
        "Arrow Flight gRPC service registered on shared port %d (advertised=%s:%d, authMode=%s)",
        grpcPort, flightAdvertisedHost, flightAdvertisedPort, authMode);
  }

  @Override
  public ServerServiceDefinition bindService() {
    return delegate.bindService();
  }

  private static BindableService createDelegate(
      BufferAllocator allocator,
      FlightProducer producer,
      ServerAuthHandler authHandler,
      ExecutorService executor) {
    try {
      Class<?> serviceClass = Class.forName("org.apache.arrow.flight.FlightBindingService");
      Constructor<?> ctor =
          serviceClass.getDeclaredConstructor(
              BufferAllocator.class,
              FlightProducer.class,
              ServerAuthHandler.class,
              ExecutorService.class);
      ctor.setAccessible(true);
      return (BindableService) ctor.newInstance(allocator, producer, authHandler, executor);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed to initialize Arrow Flight gRPC binding", e);
    }
  }
}
