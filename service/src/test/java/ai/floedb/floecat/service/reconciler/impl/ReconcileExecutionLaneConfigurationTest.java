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

package ai.floedb.floecat.service.reconciler.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class ReconcileExecutionLaneConfigurationTest {
  @Test
  void acceptsConcreteAndUnlabelledLanes() {
    ReconcileExecutionLaneConfiguration configuration = new ReconcileExecutionLaneConfiguration();

    configuration.configuredLane = Optional.of("ci-run-a");
    assertDoesNotThrow(() -> configuration.validate(null));
    configuration.configuredLane = Optional.empty();
    assertDoesNotThrow(() -> configuration.validate(null));
  }

  @Test
  void rejectsWildcardLaneAtStartup() {
    ReconcileExecutionLaneConfiguration configuration = new ReconcileExecutionLaneConfiguration();
    configuration.configuredLane = Optional.of(" * ");

    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> configuration.validate(null));

    assertEquals("execution lane '*' is reserved for internal lease scans", error.getMessage());
  }
}
