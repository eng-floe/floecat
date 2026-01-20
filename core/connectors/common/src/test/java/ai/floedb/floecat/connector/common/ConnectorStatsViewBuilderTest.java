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
import ai.floedb.floecat.types.LogicalType;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConnectorStatsViewBuilderTest {

  private static final class Agg implements StatsEngine.ColumnAgg {
    private final Long valueCount;
    private final Long nullCount;
    private final Long nanCount;
    private final Object min;
    private final Object max;
    private final Long ndvExact;
    private final ColumnNdv ndv;

    Agg(
        Long valueCount,
        Long nullCount,
        Long nanCount,
        Object min,
        Object max,
        Long ndvExact,
        ColumnNdv ndv) {
      this.valueCount = valueCount;
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
    public Long valueCount() {
      return valueCount;
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

    assertEquals(100L, v.valueCount());
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
}
