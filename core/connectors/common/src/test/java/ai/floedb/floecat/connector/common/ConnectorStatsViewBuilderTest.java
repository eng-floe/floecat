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

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.NdvApprox;
import ai.floedb.floecat.connector.common.ndv.ColumnNdv;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.rpc.ScalarValue;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConnectorStatsViewBuilderTest {

  private static final class Agg implements StatsEngine.ColumnAgg {
    private final Long rowCount;
    private final Long nullCount;
    private final Long nanCount;
    private final Object min;
    private final Object max;
    private final Long ndvExact;
    private final ColumnNdv ndv;

    Agg(
        Long rowCount,
        Long nullCount,
        Long nanCount,
        Object min,
        Object max,
        Long ndvExact,
        ColumnNdv ndv) {
      this.rowCount = rowCount;
      this.nullCount = nullCount;
      this.nanCount = nanCount;
      this.min = min;
      this.max = max;
      this.ndvExact = ndvExact;
      this.ndv = ndv;
    }

    @Override
    public Long ndvExact() {
      return ndvExact;
    }

    @Override
    public ColumnNdv ndv() {
      return ndv;
    }

    @Override
    public Long rowCount() {
      return rowCount;
    }

    @Override
    public Long nullCount() {
      return nullCount;
    }

    @Override
    public Long nanCount() {
      return nanCount;
    }

    @Override
    public Object min() {
      return min;
    }

    @Override
    public Object max() {
      return max;
    }
  }

  @Test
  void toTableValueStats_countsTheRowsTheDeletesLeave() {
    StatsEngine.Result<String> result =
        new StatsEngine.Result<>() {
          @Override
          public long totalRowCount() {
            return 10;
          }

          @Override
          public long liveRowCount() {
            return 7;
          }

          @Override
          public long totalSizeBytes() {
            return 0;
          }

          @Override
          public long fileCount() {
            return 1;
          }

          @Override
          public Map<String, StatsEngine.ColumnAgg> columns() {
            return Map.of();
          }
        };

    assertEquals(
        7L,
        ConnectorStatsViewBuilder.toTableValueStats(
                1L, 0L, ai.floedb.floecat.catalog.rpc.TableFormat.TF_ICEBERG, result)
            .getRowCount());
  }

  @Test
  void toColumnStatsView_populatesColumnRefFields() {
    Map<Integer, StatsEngine.ColumnAgg> cols = new LinkedHashMap<>();
    cols.put(10, new Agg(100L, 2L, 0L, null, null, 7L, null));

    List<FloecatConnector.ColumnStatsView> out =
        ConnectorStatsViewBuilder.toColumnStatsView(
            cols,
            id -> "c" + id,
            id -> "p.c" + id,
            id -> 3,
            id -> id, // fieldId = key
            id -> null, // no logical type
            0L);

    assertEquals(1, out.size());
    var v = out.get(0);

    assertEquals("c10", v.ref().name());
    assertEquals("p.c10", v.ref().physicalPath());
    assertEquals(3, v.ref().ordinal());
    assertEquals(10, v.ref().fieldId());

    assertEquals(100L, v.rowCount());
    assertEquals(2L, v.nullCount());
    assertEquals(0L, v.nanCount());
    assertNotNull(v.ndv());
    assertEquals(7L, v.ndv().getExact());
  }

  @Test
  void toColumnStatsView_ndvApprox_rowsTotal_fallsBackToTableTotalRows() {
    // Build a ColumnNdv with approx but without rowsTotal set (or set to 0)
    ColumnNdv ndvModel = new ColumnNdv();
    ndvModel.approx = new ai.floedb.floecat.connector.common.ndv.NdvApprox();
    ndvModel.approx.estimate = 123.0;
    ndvModel.approx.rowsTotal = 0L; // triggers fallback

    Map<String, StatsEngine.ColumnAgg> cols = new LinkedHashMap<>();
    cols.put("col", new Agg(null, null, null, null, null, null, ndvModel));

    long tableTotalRows = 999L;

    List<FloecatConnector.ColumnStatsView> out =
        ConnectorStatsViewBuilder.toColumnStatsView(
            cols, k -> k, k -> k, k -> 1, k -> 0, k -> (LogicalType) null, tableTotalRows);

    Ndv ndv = out.get(0).ndv();
    assertNotNull(ndv);
    assertTrue(ndv.hasApprox());

    NdvApprox approx = ndv.getApprox();
    assertEquals(123.0, approx.getEstimate());
    assertEquals(
        tableTotalRows, approx.getRowsTotal(), "rowsTotal should fallback to tableTotalRows");
  }

  @Test
  void toColumnStatsView_encodesTypedBounds() {
    Map<String, StatsEngine.ColumnAgg> cols = new LinkedHashMap<>();
    cols.put("col", new Agg(10L, 0L, 0L, 1L, 5L, null, null));

    var out =
        ConnectorStatsViewBuilder.toColumnStatsView(
            cols, k -> k, k -> k, k -> 1, k -> 0, k -> LogicalType.of(LogicalKind.INT), 10L);

    assertEquals(ScalarValue.newBuilder().setI64(1).build(), out.get(0).min());
    assertEquals(ScalarValue.newBuilder().setI64(5).build(), out.get(0).max());

    // A maximum the type cannot hold is rounded up.
    double aboveFloat = Math.nextUp((double) 0.1f);
    cols.put("col", new Agg(10L, 0L, 0L, aboveFloat, aboveFloat, null, null));
    var floats =
        ConnectorStatsViewBuilder.toColumnStatsView(
            cols, k -> k, k -> k, k -> 1, k -> 0, k -> LogicalType.of(LogicalKind.FLOAT), 10L);
    assertEquals((double) Math.nextUp(0.1f), floats.get(0).max().getF64());
  }

  @Test
  void toColumnStatsView_countsNullsOnlyBesideTheColumnsValueCount() {
    Map<String, StatsEngine.ColumnAgg> cols = new LinkedHashMap<>();
    cols.put("col", new Agg(null, 2L, 0L, null, null, null, null));

    var out =
        ConnectorStatsViewBuilder.toColumnStatsView(
            cols, k -> k, k -> k, k -> 1, k -> 0, k -> LogicalType.of(LogicalKind.INT), 10L);

    assertEquals(10L, out.get(0).rowCount());
    assertNull(out.get(0).nullCount());
    assertNull(out.get(0).nanCount());
  }

  @Test
  void toColumnStatsView_skipsMinMaxForNonOrderableComplexTypes() {
    Map<String, StatsEngine.ColumnAgg> cols = new LinkedHashMap<>();
    cols.put("arr_col", new Agg(10L, 0L, 0L, List.of(1, 2), List.of(9, 10), null, null));

    List<FloecatConnector.ColumnStatsView> out =
        ConnectorStatsViewBuilder.toColumnStatsView(
            cols, k -> k, k -> k, k -> 1, k -> 0, k -> LogicalType.of(LogicalKind.ARRAY), 10L);

    assertEquals(1, out.size());
    assertNull(out.get(0).min());
    assertNull(out.get(0).max());
  }
}
