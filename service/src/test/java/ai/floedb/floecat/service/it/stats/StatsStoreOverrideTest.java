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

package ai.floedb.floecat.service.it.stats;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.it.profiles.StatsStoreOverrideProfile;
import ai.floedb.floecat.service.statistics.engine.impl.PersistedStatsCaptureEngine;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsKind;
import ai.floedb.floecat.stats.spi.StatsStore;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(StatsStoreOverrideProfile.class)
class StatsStoreOverrideTest {

  @Inject StatsStore statsStore;

  @Inject PersistedStatsCaptureEngine persistedEngine;

  @BeforeEach
  void resetCounters() {
    TestOverrideStatsStore.resetCounters();
  }

  @Test
  void selectedAlternativeStoreIsUsedByCaptureEngine() {
    assertThat(statsStore).isInstanceOf(TestOverrideStatsStore.class);

    ResourceId tableId =
        ResourceId.newBuilder().setAccountId("a").setId("t").setKind(ResourceKind.RK_TABLE).build();
    long snapshotId = 77L;

    TargetStatsRecord columnRecord =
        TargetStatsRecords.columnRecord(
            tableId,
            snapshotId,
            1L,
            ScalarStats.newBuilder()
                .setDisplayName("c1")
                .setLogicalType("BIGINT")
                .setValueCount(5L)
                .build(),
            null);
    statsStore.putTargetStats(columnRecord);

    var request =
        new StatsCaptureRequest(
            tableId,
            snapshotId,
            StatsTargetIdentity.columnTarget(1L),
            Set.of(StatsKind.NULL_COUNT),
            StatsExecutionMode.SYNC,
            "iceberg",
            "test-corr",
            false);

    var result = persistedEngine.capture(request);

    assertThat(result).isPresent();
    assertThat(result.orElseThrow().record()).isEqualTo(columnRecord);
    assertThat(TestOverrideStatsStore.putCount()).isEqualTo(1);
    assertThat(TestOverrideStatsStore.getCount()).isGreaterThan(0);
  }
}
