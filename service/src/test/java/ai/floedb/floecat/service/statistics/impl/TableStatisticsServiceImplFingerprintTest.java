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

import ai.floedb.floecat.catalog.rpc.AppliedDeletes;
import ai.floedb.floecat.catalog.rpc.ColumnStatsTarget;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import java.util.List;
import org.junit.jupiter.api.Test;

class TableStatisticsServiceImplFingerprintTest {

  @Test
  void fileFingerprintIsStableAcrossColumnOrderings() throws Exception {
    ScalarStats colA =
        ScalarStats.newBuilder()
            .setDisplayName("same")
            .setLogicalType("BIGINT")
            .setRowCount(1L)
            .setMin("1")
            .setMax("1")
            .build();
    ScalarStats colB =
        ScalarStats.newBuilder()
            .setDisplayName("same")
            .setLogicalType("BIGINT")
            .setRowCount(1L)
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
  void scalarFingerprintIgnoresDisplayName() {
    // display_name is cosmetic (stats.proto) and must not affect identity, matching the
    // content-hash image. Two scalars differing only in display_name fingerprint identically.
    ScalarStats first =
        ScalarStats.newBuilder()
            .setDisplayName("col_a")
            .setLogicalType("BIGINT")
            .setMin("1")
            .build();
    ScalarStats second = first.toBuilder().setDisplayName("alias_name").build();

    assertThat(StatsCanonicalizer.canonicalFingerprint(first))
        .isEqualTo(StatsCanonicalizer.canonicalFingerprint(second));
  }

  @Test
  void fileFingerprintTellsKnownDeletesFromUnknown() {
    FileTargetStats unknown = FileTargetStats.newBuilder().setFilePath("/data/f.parquet").build();
    FileTargetStats none =
        unknown.toBuilder().setAppliedDeletes(AppliedDeletes.getDefaultInstance()).build();
    FileTargetStats one =
        unknown.toBuilder()
            .setAppliedDeletes(AppliedDeletes.newBuilder().addFilePaths("/data/d.parquet"))
            .build();

    assertThat(StatsCanonicalizer.canonicalFingerprint(unknown))
        .isNotEqualTo(StatsCanonicalizer.canonicalFingerprint(none));
    assertThat(StatsCanonicalizer.canonicalFingerprint(none))
        .isNotEqualTo(StatsCanonicalizer.canonicalFingerprint(one));
    assertThat(StatsCanonicalizer.canonicalFingerprint(none))
        .isNotEqualTo(
            StatsCanonicalizer.canonicalFingerprint(none.toBuilder().setLiveRowCount(3).build()));
  }

  @Test
  void tableFingerprintTellsAnExactRowCountFromAnEstimate() {
    TableValueStats estimate = TableValueStats.newBuilder().setRowCount(5L).build();
    TargetStatsRecord record =
        TargetStatsRecord.newBuilder()
            .setTarget(StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()))
            .setTable(estimate)
            .build();

    assertThat(StatsCanonicalizer.canonicalFingerprint(record))
        .isNotEqualTo(
            StatsCanonicalizer.canonicalFingerprint(
                record.toBuilder().setTable(estimate.toBuilder().setRowCountExact(true)).build()));
  }

  @Test
  void scalarFingerprintsTellAnAbsentCountOrBoundFromAZeroOrEmptyOne() {
    ScalarStats absent = ScalarStats.newBuilder().setLogicalType("DOUBLE").setRowCount(2L).build();
    TargetStatsRecord.Builder record =
        TargetStatsRecord.newBuilder()
            .setTarget(
                StatsTarget.newBuilder().setColumn(ColumnStatsTarget.newBuilder().setColumnId(1L)));
    for (ScalarStats explicit :
        List.of(
            absent.toBuilder().setNullCount(0L).build(),
            absent.toBuilder().setNanCount(0L).build(),
            absent.toBuilder().setMin("").build(),
            absent.toBuilder().setMax("").build(),
            absent.toBuilder().setBoundsExact(true).build())) {
      assertThat(StatsCanonicalizer.canonicalFingerprint(explicit))
          .isNotEqualTo(StatsCanonicalizer.canonicalFingerprint(absent));
      assertThat(StatsCanonicalizer.canonicalFingerprint(record.setScalar(explicit).build()))
          .isNotEqualTo(StatsCanonicalizer.canonicalFingerprint(record.setScalar(absent).build()));
    }
  }
}
