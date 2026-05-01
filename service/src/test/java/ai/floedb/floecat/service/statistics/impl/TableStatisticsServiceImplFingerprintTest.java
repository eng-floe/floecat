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

package ai.floedb.floecat.service.statistics.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import org.junit.jupiter.api.Test;

class TableStatisticsServiceImplFingerprintTest {

  @Test
  void fileFingerprintIsStableAcrossColumnOrderings() throws Exception {
    ScalarStats colA =
        ScalarStats.newBuilder()
            .setDisplayName("same")
            .setLogicalType("BIGINT")
            .setValueCount(1L)
            .setMin("1")
            .setMax("1")
            .build();
    ScalarStats colB =
        ScalarStats.newBuilder()
            .setDisplayName("same")
            .setLogicalType("BIGINT")
            .setValueCount(1L)
            .setMin("2")
            .setMax("2")
            .build();

    FileTargetStats first =
        FileTargetStats.newBuilder()
            .setFilePath("/data/f.parquet")
            .setFileFormat("parquet")
            .setRowCount(2L)
            .setSizeBytes(100L)
            .addColumns(FileColumnStats.newBuilder().setColumnId(1L).setScalar(colA).build())
            .addColumns(FileColumnStats.newBuilder().setColumnId(2L).setScalar(colB).build())
            .build();
    FileTargetStats second =
        first.toBuilder()
            .clearColumns()
            .addColumns(FileColumnStats.newBuilder().setColumnId(2L).setScalar(colB).build())
            .addColumns(FileColumnStats.newBuilder().setColumnId(1L).setScalar(colA).build())
            .build();

    assertThat(StatsCanonicalizer.canonicalFingerprint(first))
        .isEqualTo(StatsCanonicalizer.canonicalFingerprint(second));
  }

  @Test
  void scalarFingerprintDistinguishesDisplayName() {
    ScalarStats first =
        ScalarStats.newBuilder()
            .setDisplayName("col_a")
            .setLogicalType("BIGINT")
            .setMin("1")
            .build();
    ScalarStats second = first.toBuilder().setDisplayName("alias_name").build();

    assertThat(StatsCanonicalizer.canonicalFingerprint(first))
        .isNotEqualTo(StatsCanonicalizer.canonicalFingerprint(second));
  }
}
