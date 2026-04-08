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

package ai.floedb.floecat.stats.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.TableStats;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class StatsCaptureValueTest {

  @Test
  void requiresExactlyOnePayload() {
    assertThatThrownBy(
            () ->
                new StatsCaptureValue(
                    Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Exactly one payload");

    assertThatThrownBy(
            () ->
                new StatsCaptureValue(
                    Optional.of(TableStats.getDefaultInstance()),
                    Optional.empty(),
                    Optional.of(
                        new StatsColumnValue(
                            1L,
                            "c",
                            "bigint",
                            Optional.empty(),
                            new StatsValueSummary(
                                0L,
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Map.of()))),
                    Optional.empty()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Exactly one payload");
  }

  @Test
  void mapsColumnStatsToSharedValueStats() {
    ColumnStats proto =
        ColumnStats.newBuilder()
            .setColumnId(7L)
            .setColumnName("col")
            .setLogicalType("varchar")
            .setValueCount(123L)
            .setNullCount(5L)
            .setMin("a")
            .setMax("z")
            .putProperties("k", "v")
            .build();

    StatsCaptureResult result =
        StatsCaptureResult.forColumn(
            "engine",
            ai.floedb.floecat.catalog.rpc.StatsTarget.getDefaultInstance(),
            proto,
            Map.of());

    assertThat(result.value().column()).isPresent();
    StatsColumnValue mapped = result.value().column().orElseThrow();
    assertThat(mapped.columnId()).isEqualTo(7L);
    assertThat(mapped.valueStats().valueCount()).isEqualTo(123L);
    assertThat(mapped.valueStats().nullCount()).contains(5L);
    assertThat(mapped.valueStats().min()).contains("a");
    assertThat(mapped.valueStats().max()).contains("z");
    assertThat(mapped.valueStats().properties()).containsEntry("k", "v");
  }

  @Test
  void mapsFileStatsToFilePayload() {
    FileColumnStats file =
        FileColumnStats.newBuilder().setFilePath("/data/file.parquet").setRowCount(123L).build();

    StatsCaptureResult result =
        StatsCaptureResult.forFile(
            "engine",
            ai.floedb.floecat.catalog.rpc.StatsTarget.getDefaultInstance(),
            file,
            Map.of());

    assertThat(result.value().file()).contains(file);
  }
}
