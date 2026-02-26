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

package ai.floedb.floecat.connector.common;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import org.junit.jupiter.api.Test;

class ParquetFooterStatsTest {

  @Test
  void colAgg_skipsMinMaxForNonOrderableLogicalTypes() {
    ParquetFooterStats.ColAgg agg = new ParquetFooterStats.ColAgg();
    LogicalType interval = LogicalType.of(LogicalKind.INTERVAL);

    agg.mergeMin("a", interval);
    agg.mergeMax("z", interval);
    agg.mergeMin("b", interval);
    agg.mergeMax("y", interval);

    assertThat(agg.min).isNull();
    assertThat(agg.max).isNull();
  }

  @Test
  void colAgg_preservesOrderingForOrderableLogicalTypes() {
    ParquetFooterStats.ColAgg agg = new ParquetFooterStats.ColAgg();
    LogicalType intType = LogicalType.of(LogicalKind.INT);

    agg.mergeMin(9L, intType);
    agg.mergeMax(9L, intType);
    agg.mergeMin(2L, intType);
    agg.mergeMax(12L, intType);

    assertThat(agg.min).isEqualTo(2L);
    assertThat(agg.max).isEqualTo(12L);
  }
}
