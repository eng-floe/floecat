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

package ai.floedb.floecat.service.query.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.SketchPayload;
import ai.floedb.floecat.catalog.rpc.SketchRole;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.query.rpc.StatRole;
import ai.floedb.floecat.query.rpc.StatsResultStatus;
import com.google.protobuf.ByteString;
import java.util.List;
import org.junit.jupiter.api.Test;

class PlannerStatsResultMaterializerTest {

  @Test
  void scalarRequestDropsEstimateLessNdvEnvelope() {
    // Generation enrichment can persist an Ndv envelope with no exact/approx estimate, existing
    // purely to carry a superseded generation's sketch forward. Once a scalar-only request
    // filters that sketch out, the envelope carries nothing servable and must not be emitted:
    // hasNdv() with neither mode nor payloads would turn "NDV absent" into "NDV zero" for any
    // consumer gating on presence alone.
    TargetStatsRecord record = recordWithNdv(Ndv.newBuilder().addSketches(ndvSketch()).build());

    TargetStatsRecord materialized = materializeScalarOnly(record);

    assertTrue(materialized.hasScalar());
    assertFalse(materialized.getScalar().hasNdv());
  }

  @Test
  void scalarRequestKeepsNdvEstimateAndClearsSketches() {
    TargetStatsRecord record =
        recordWithNdv(Ndv.newBuilder().setExact(42).addSketches(ndvSketch()).build());

    TargetStatsRecord materialized = materializeScalarOnly(record);

    assertTrue(materialized.getScalar().hasNdv());
    assertEquals(42, materialized.getScalar().getNdv().getExact());
    assertEquals(0, materialized.getScalar().getNdv().getSketchesCount());
  }

  private static TargetStatsRecord materializeScalarOnly(TargetStatsRecord record) {
    PlannerStatsTargetNeed need =
        new PlannerStatsTargetNeed(
            StatsTarget.getDefaultInstance(),
            List.of(new PlannerStatsStatRequest(StatRole.STAT_ROLE_SCALAR, "", 0, 0, false, "")),
            "target",
            0);
    PlannerStatsResultMaterializer.Materialized materialized =
        PlannerStatsResultMaterializer.materialize(
            need, PlannerTargetStatsLookupResult.hit(record));
    assertEquals(StatsResultStatus.STATS_RESULT_HIT_COMPLETE, materialized.status());
    return materialized.record();
  }

  private static TargetStatsRecord recordWithNdv(Ndv ndv) {
    return TargetStatsRecord.newBuilder().setScalar(ScalarStats.newBuilder().setNdv(ndv)).build();
  }

  private static SketchPayload ndvSketch() {
    return SketchPayload.newBuilder()
        .setRole(SketchRole.SKETCH_ROLE_NDV)
        .setSketchType("theta")
        .setData(ByteString.copyFromUtf8("sketch-bytes"))
        .build();
  }
}
