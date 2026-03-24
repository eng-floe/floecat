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

import ai.floedb.floecat.catalog.rpc.StatsCaptureMode;
import ai.floedb.floecat.catalog.rpc.StatsCompleteness;
import ai.floedb.floecat.catalog.rpc.StatsCoverage;
import ai.floedb.floecat.catalog.rpc.StatsMetadata;
import ai.floedb.floecat.catalog.rpc.StatsProducer;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.catalog.rpc.UpstreamStamp;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import com.google.protobuf.util.Timestamps;
import org.junit.jupiter.api.Test;

class TableStatsNormalizerTest {

  @Test
  void normalizeOrdersMetadataPropertiesDeterministically() {
    TableStats left =
        baseTableStatsBuilder()
            .setMetadata(
                StatsMetadata.newBuilder()
                    .setProducer(StatsProducer.SPROD_SOURCE_NATIVE)
                    .setCaptureMode(StatsCaptureMode.SCM_ASYNC)
                    .setCompleteness(StatsCompleteness.SC_PARTIAL)
                    .setConfidenceLevel(0.8d)
                    .setCoverage(
                        StatsCoverage.newBuilder()
                            .setRowsScanned(1000)
                            .putProperties("z", "9")
                            .putProperties("a", "1")
                            .build())
                    .putProperties("k2", "v2")
                    .putProperties("k1", "v1")
                    .build())
            .build();

    TableStats right =
        baseTableStatsBuilder()
            .setMetadata(
                StatsMetadata.newBuilder()
                    .setProducer(StatsProducer.SPROD_SOURCE_NATIVE)
                    .setCaptureMode(StatsCaptureMode.SCM_ASYNC)
                    .setCompleteness(StatsCompleteness.SC_PARTIAL)
                    .setConfidenceLevel(0.8d)
                    .setCoverage(
                        StatsCoverage.newBuilder()
                            .setRowsScanned(1000)
                            .putProperties("a", "1")
                            .putProperties("z", "9")
                            .build())
                    .putProperties("k1", "v1")
                    .putProperties("k2", "v2")
                    .build())
            .build();

    assertEquals(TableStatsNormalizer.normalize(left), TableStatsNormalizer.normalize(right));
  }

  @Test
  void normalizePreservesMetadataButClearsVolatileUpstreamFields() {
    TableStats in =
        baseTableStatsBuilder()
            .setMetadata(
                StatsMetadata.newBuilder()
                    .setProducer(StatsProducer.SPROD_UPSTREAM_METADATA_DERIVED)
                    .setCaptureMode(StatsCaptureMode.SCM_SYNC)
                    .setCompleteness(StatsCompleteness.SC_COMPLETE)
                    .setCoverage(StatsCoverage.newBuilder().setBytesScanned(2048))
                    .build())
            .setUpstream(
                UpstreamStamp.newBuilder()
                    .setCommitRef("abc123")
                    .setFetchedAt(Timestamps.fromSeconds(1700000000L))
                    .build())
            .build();

    TableStats out = TableStatsNormalizer.normalize(in);
    assertTrue(out.hasMetadata());
    assertEquals(StatsCompleteness.SC_COMPLETE, out.getMetadata().getCompleteness());
    assertFalse(out.getUpstream().hasFetchedAt());
    assertEquals("", out.getUpstream().getCommitRef());
  }

  private static TableStats.Builder baseTableStatsBuilder() {
    return TableStats.newBuilder()
        .setTableId(
            ResourceId.newBuilder()
                .setAccountId("t-1")
                .setId("table-1")
                .setKind(ResourceKind.RK_TABLE))
        .setSnapshotId(1L)
        .setRowCount(10)
        .setDataFileCount(2)
        .setTotalSizeBytes(128);
  }
}
