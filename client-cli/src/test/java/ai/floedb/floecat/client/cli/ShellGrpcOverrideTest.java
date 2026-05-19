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

package ai.floedb.floecat.client.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class ShellGrpcOverrideTest {

  @Test
  void resolveGrpcEndpointOverrideUsesExplicitOptionsFirst() {
    Shell.GrpcEndpointOverride override =
        Shell.resolveGrpcEndpointOverride(
            "explicit-host",
            9200,
            "env-host",
            "9300",
            "quarkus-run.jar --host launch-host --port 9400");

    assertNotNull(override);
    assertEquals("explicit-host", override.host());
    assertEquals(9200, override.port());
  }

  @Test
  void resolveGrpcEndpointOverrideFallsBackToLaunchArgs() {
    Shell.GrpcEndpointOverride override =
        Shell.resolveGrpcEndpointOverride(
            null, null, null, null, "quarkus-run.jar --host host.docker.internal --port 9100");

    assertNotNull(override);
    assertEquals("host.docker.internal", override.host());
    assertEquals(9100, override.port());
  }

  @Test
  void resolveGrpcEndpointOverrideSupportsEqualsSyntax() {
    Shell.GrpcEndpointOverride override =
        Shell.resolveGrpcEndpointOverride(
            null, null, null, null, "quarkus-run.jar --host=service --port=9200");

    assertNotNull(override);
    assertEquals("service", override.host());
    assertEquals(9200, override.port());
  }

  @Test
  void resolveGrpcEndpointOverrideFallsBackToEnv() {
    Shell.GrpcEndpointOverride override =
        Shell.resolveGrpcEndpointOverride(null, null, "env-host", "9300", "quarkus-run.jar");

    assertNotNull(override);
    assertEquals("env-host", override.host());
    assertEquals(9300, override.port());
  }

  @Test
  void resolveGrpcEndpointOverrideReturnsNullWhenUnset() {
    assertNull(Shell.resolveGrpcEndpointOverride(null, null, null, null, "quarkus-run.jar"));
  }
}
