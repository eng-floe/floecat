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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.StageCommitException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StageCommitProcessorTest {
  private final StageCommitProcessor processor = new StageCommitProcessor();

  @BeforeEach
  void setUp() {}

  @Test
  void assertCreateRequirementFailsWhenTableExists() {
    StageCommitException ex =
        assertThrows(
            StageCommitException.class,
            () ->
                processor.validateStageRequirements(
                    List.of(Map.of("type", "assert-create")),
                    "cat",
                    List.of("db"),
                    "orders",
                    true));

    assertEquals("assert-create failed", ex.getMessage());
  }
}
