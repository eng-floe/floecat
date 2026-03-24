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

package ai.floedb.floecat.service.repo.util;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.StatsCaptureMode;
import ai.floedb.floecat.catalog.rpc.StatsCompleteness;
import ai.floedb.floecat.catalog.rpc.StatsCoverage;
import ai.floedb.floecat.catalog.rpc.StatsMetadata;
import ai.floedb.floecat.catalog.rpc.StatsProducer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import org.junit.jupiter.api.Test;

class ColumnStatsNormalizerTest {

  @Test
  void normalizeOrdersMetadataPropertiesDeterministically() {
    ColumnStats left =
        baseColumnStatsBuilder()
            .setMetadata(
                StatsMetadata.newBuilder()
                    .setProducer(StatsProducer.SPROD_ENGINE_COMPUTED)
                    .setCaptureMode(StatsCaptureMode.SCM_SYNC)
                    .setCompleteness(StatsCompleteness.SC_PARTIAL)
                    .setConfidenceLevel(0.42d)
                    .setCoverage(
                        StatsCoverage.newBuilder()
                            .setRowGroupsSampled(3)
                            .putProperties("z", "9")
                            .putProperties("a", "1")
                            .build())
                    .putProperties("z", "last")
                    .putProperties("a", "first")
                    .build())
            .build();

    ColumnStats right =
        baseColumnStatsBuilder()
            .setMetadata(
                StatsMetadata.newBuilder()
                    .setProducer(StatsProducer.SPROD_ENGINE_COMPUTED)
                    .setCaptureMode(StatsCaptureMode.SCM_SYNC)
                    .setCompleteness(StatsCompleteness.SC_PARTIAL)
                    .setConfidenceLevel(0.42d)
                    .setCoverage(
                        StatsCoverage.newBuilder()
                            .setRowGroupsSampled(3)
                            .putProperties("a", "1")
                            .putProperties("z", "9")
                            .build())
                    .putProperties("a", "first")
                    .putProperties("z", "last")
                    .build())
            .build();

    assertEquals(ColumnStatsNormalizer.normalize(left), ColumnStatsNormalizer.normalize(right));
  }

  @Test
  void normalizePreservesMetadataFields() {
    ColumnStats in =
        baseColumnStatsBuilder()
            .setMetadata(
                StatsMetadata.newBuilder()
                    .setProducer(StatsProducer.SPROD_ENGINE_COMPUTED)
                    .setCaptureMode(StatsCaptureMode.SCM_ASYNC)
                    .setCompleteness(StatsCompleteness.SC_COMPLETE)
                    .setCoverage(StatsCoverage.newBuilder().setFilesScanned(2))
                    .build())
            .build();

    ColumnStats out = ColumnStatsNormalizer.normalize(in);
    assertTrue(out.hasMetadata());
    assertEquals(StatsProducer.SPROD_ENGINE_COMPUTED, out.getMetadata().getProducer());
    assertEquals(2L, out.getMetadata().getCoverage().getFilesScanned());
  }

  private static ColumnStats.Builder baseColumnStatsBuilder() {
    return ColumnStats.newBuilder()
        .setTableId(
            ResourceId.newBuilder()
                .setAccountId("t-1")
                .setId("table-1")
                .setKind(ResourceKind.RK_TABLE))
        .setSnapshotId(7L)
        .setColumnId(42L)
        .setColumnName("c")
        .setLogicalType("int");
  }
}
