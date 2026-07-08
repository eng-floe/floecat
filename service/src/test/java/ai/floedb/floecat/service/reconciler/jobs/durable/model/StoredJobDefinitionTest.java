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

package ai.floedb.floecat.service.reconciler.jobs.durable.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotSelection;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class StoredJobDefinitionTest {
  @Test
  void jsonRoundTripPreservesTypedCapturePolicy() throws Exception {
    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            "table-1",
            null,
            List.of(),
            ReconcileCapturePolicy.of(
                List.of(new ReconcileCapturePolicy.Column("c1", true, false)),
                Set.of(ReconcileCapturePolicy.Output.TABLE_STATS),
                ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY,
                16),
            ReconcileSnapshotSelection.current());

    ObjectMapper mapper = new ObjectMapper();
    StoredJobDefinition decoded =
        mapper.readValue(
            mapper.writeValueAsBytes(StoredJobDefinition.of(scope, null, null)),
            StoredJobDefinition.class);

    ReconcileScope restored = decoded.toScope();
    assertEquals(scope.capturePolicy().columns(), restored.capturePolicy().columns());
    assertEquals(scope.capturePolicy().outputs(), restored.capturePolicy().outputs());
    assertEquals(
        scope.capturePolicy().defaultColumnScope(), restored.capturePolicy().defaultColumnScope());
    assertEquals(
        scope.capturePolicy().maxDefaultColumns(), restored.capturePolicy().maxDefaultColumns());
  }
}
