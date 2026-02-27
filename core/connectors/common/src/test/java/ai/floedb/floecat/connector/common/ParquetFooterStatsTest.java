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
import ai.floedb.floecat.types.TemporalCoercions;
import java.lang.reflect.Method;
import java.time.LocalTime;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
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

  @Test
  void timeStatsDropOutOfRangeValues() throws Exception {
    Object lta = timeLogicalTypeAnnotation(TimeUnit.MICROS);
    Method m =
        ParquetFooterStats.class.getDeclaredMethod("timeStatValue", Object.class, Object.class);
    m.setAccessible(true);

    long nanosPerDay = TemporalCoercions.NANOS_PER_DAY;
    long outOfRangeMicros = nanosPerDay / 1_000L;
    Object outOfRange = m.invoke(null, lta, outOfRangeMicros);
    assertThat(outOfRange).isNull();

    long inRangeNanos = nanosPerDay - 1;
    long inRangeMicros = inRangeNanos / 1_000L;
    Object inRange = m.invoke(null, lta, inRangeMicros);
    assertThat(inRange).isEqualTo(LocalTime.ofNanoOfDay(inRangeMicros * 1_000L));
  }

  private static Object timeLogicalTypeAnnotation(TimeUnit unit) throws Exception {
    for (Method method : LogicalTypeAnnotation.class.getMethods()) {
      if (!method.getName().equals("timeType")) {
        continue;
      }
      Class<?>[] params = method.getParameterTypes();
      if (params.length == 2 && params[0] == boolean.class && params[1] == TimeUnit.class) {
        return method.invoke(null, false, unit);
      }
    }
    throw new IllegalStateException("Unable to locate LogicalTypeAnnotation.timeType");
  }
}
