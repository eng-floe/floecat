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

package ai.floedb.floecat.service.query.impl.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.arrow.ArrowScanPlan;
import ai.floedb.floecat.arrow.ColumnarBatch;
import ai.floedb.floecat.common.rpc.Operator;
import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import ai.floedb.floecat.scanner.spi.SystemScanRequest;
import ai.floedb.floecat.service.query.system.SystemRowFilter;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.junit.jupiter.api.Test;

class ArrowScanPlannerTest {

  @Test
  void rowBackedScanner_usesProjectedSchemaForProjectedRows() {
    List<SchemaColumn> schema =
        List.of(col("catalog_id"), col("catalog"), col("table_id"), col("table"), col("is_system"));
    List<String> requiredColumns = List.of("catalog", "table");
    SystemObjectScanner scanner =
        new SystemObjectScanner() {
          @Override
          public List<SchemaColumn> schema() {
            return schema;
          }

          @Override
          public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
            return scan(ctx, SystemScanRequest.empty());
          }

          @Override
          public Stream<SystemObjectRow> scan(
              SystemObjectScanContext ctx, SystemScanRequest request) {
            return Stream.of(
                new SystemObjectRow(
                    new Object[] {"cat-id", "examples", "table-id", "orders", false}));
          }
        };

    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        ArrowScanPlan plan =
            new ArrowScanPlanner()
                .plan(
                    scanner,
                    null,
                    schema,
                    List.<Predicate>of(),
                    requiredColumns,
                    SystemScanRequest.of(null, requiredColumns),
                    allocator)) {
      assertThat(plan.schema().getFields())
          .extracting(field -> field.getName())
          .containsExactly("catalog", "table");

      try (ColumnarBatch batch = plan.iterator().next()) {
        assertThat(batch.root().getFieldVectors())
            .extracting(vector -> vector.getField().getName())
            .containsExactly("catalog", "table");
        assertThat(batch.root().getRowCount()).isEqualTo(1);
        assertThat(stringValue(batch, 0, 0)).isEqualTo("examples");
        assertThat(stringValue(batch, 1, 0)).isEqualTo("orders");
      }
    }
  }

  @Test
  void rowBackedScanner_filtersBeforeProjectionWhenPredicateColumnIsNotProjected() {
    List<SchemaColumn> schema = List.of(col("catalog"), col("namespace"), col("table"));
    List<String> requiredColumns = List.of("table");
    List<Predicate> predicates =
        List.of(
            Predicate.newBuilder()
                .setColumn("namespace")
                .setOp(Operator.OP_EQ)
                .addValues("sys")
                .build());
    SystemObjectScanner scanner =
        new SystemObjectScanner() {
          @Override
          public List<SchemaColumn> schema() {
            return schema;
          }

          @Override
          public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
            return scan(ctx, SystemScanRequest.empty());
          }

          @Override
          public Stream<SystemObjectRow> scan(
              SystemObjectScanContext ctx, SystemScanRequest request) {
            return Stream.of(
                new SystemObjectRow(new Object[] {"examples", "sys", "table"}),
                new SystemObjectRow(new Object[] {"examples", "iceberg", "orders"}));
          }
        };

    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        ArrowScanPlan plan =
            new ArrowScanPlanner()
                .plan(
                    scanner,
                    null,
                    schema,
                    predicates,
                    requiredColumns,
                    SystemScanRequest.of(
                        SystemRowFilter.EXPRESSION_PROVIDER.toExpr(predicates), requiredColumns),
                    allocator)) {
      assertThat(plan.schema().getFields())
          .extracting(field -> field.getName())
          .containsExactly("table");

      try (ColumnarBatch batch = plan.iterator().next()) {
        assertThat(batch.root().getFieldVectors())
            .extracting(vector -> vector.getField().getName())
            .containsExactly("table");
        assertThat(batch.root().getRowCount()).isEqualTo(1);
        assertThat(stringValue(batch, 0, 0)).isEqualTo("table");
      }
    }
  }

  private static SchemaColumn col(String name) {
    return SchemaColumn.newBuilder().setName(name).setLogicalType("VARCHAR").build();
  }

  private static String stringValue(ColumnarBatch batch, int column, int row) {
    VarCharVector vector = (VarCharVector) batch.root().getVector(column);
    return new String(vector.get(row));
  }
}
