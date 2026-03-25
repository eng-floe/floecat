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

package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.service.it.profiles.AuthModeOidcProfile;
import ai.floedb.floecat.system.rpc.SystemTableFlightCommand;
import ai.floedb.floecat.system.rpc.SystemTableTarget;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(AuthModeOidcProfile.class)
class SystemTableFlightAuthModeOidcIT {

  @ConfigProperty(name = "quarkus.grpc.server.port")
  int grpcPort;

  private BufferAllocator allocator;
  private FlightClient client;

  @BeforeEach
  void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    client =
        FlightClient.builder(allocator, Location.forGrpcInsecure("localhost", grpcPort)).build();
  }

  @AfterEach
  void tearDown() throws Exception {
    if (client != null) {
      client.close();
    }
    if (allocator != null) {
      allocator.close();
    }
  }

  @Test
  void getFlightInfoRejectsMissingAuthorizationHeader() {
    SystemTableFlightCommand command =
        SystemTableFlightCommand.newBuilder()
            .setTarget(
                SystemTableTarget.newBuilder()
                    .setName(
                        NameRef.newBuilder()
                            .addPath("information_schema")
                            .setName("tables")
                            .build())
                    .build())
            .build();
    FlightDescriptor descriptor = FlightDescriptor.command(command.toByteArray());

    FlightRuntimeException ex =
        assertThrows(FlightRuntimeException.class, () -> client.getInfo(descriptor));

    assertEquals(CallStatus.UNAUTHENTICATED.code(), ex.status().code());
  }
}
