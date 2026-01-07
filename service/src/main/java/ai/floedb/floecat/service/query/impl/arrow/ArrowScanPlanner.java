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

import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.service.query.system.SystemRowFilter;
import ai.floedb.floecat.service.query.system.SystemRowProjector;
import ai.floedb.floecat.systemcatalog.columnar.ArrowFilterOperator;
import ai.floedb.floecat.systemcatalog.columnar.ArrowProjectOperator;
import ai.floedb.floecat.systemcatalog.columnar.ArrowSchemaUtil;
import ai.floedb.floecat.systemcatalog.columnar.ColumnarBatch;
import ai.floedb.floecat.systemcatalog.columnar.RowStreamToArrowBatchAdapter;
import ai.floedb.floecat.systemcatalog.expr.Expr;
import ai.floedb.floecat.systemcatalog.spi.scanner.ScanOutputFormat;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Builds the Arrow execution plan for a system scan request.
 *
 * <p>The planner prefers native Arrow-capable scanners but can wrap the legacy row stream via
 * {@link RowStreamToArrowBatchAdapter}. Every plan also runs the columnar filter/projection
 * operators so callers can rely on a consistent streaming schema.
 */
public final class ArrowScanPlanner {
  private static final int DEFAULT_ARROW_BATCH_SIZE = 512;

  /** Creates an Arrow scan plan (schema + batch stream) for the given scanner invocation. */
  public ArrowScanPlan plan(
      SystemObjectScanner scanner,
      SystemObjectScanContext ctx,
      List<SchemaColumn> schemaColumns,
      List<Predicate> predicates,
      List<String> requiredColumns,
      Expr arrowExpr,
      BufferAllocator allocator) {
    Schema schema = ArrowSchemaUtil.toArrowSchema(schemaColumns);
    Stream<ColumnarBatch> batches;
    if (scanner.supportedFormats().contains(ScanOutputFormat.ARROW_IPC)) {
      batches =
          scanner
              .scanArrow(ctx, arrowExpr, requiredColumns, allocator)
              .map(batch -> ArrowFilterOperator.filter(batch, arrowExpr, allocator))
              .map(batch -> ArrowProjectOperator.project(batch, requiredColumns, allocator));
    } else {
      Stream<SystemObjectRow> rows = scanner.scan(ctx);
      Stream<SystemObjectRow> filtered = SystemRowFilter.filter(rows, schemaColumns, predicates);
      Stream<SystemObjectRow> projected =
          SystemRowProjector.project(filtered, schemaColumns, requiredColumns);

      RowStreamToArrowBatchAdapter adapter =
          new RowStreamToArrowBatchAdapter(allocator, schemaColumns, DEFAULT_ARROW_BATCH_SIZE);
      Stream<ColumnarBatch> adapted =
          adapter
              .adapt(projected)
              .map(batch -> ArrowFilterOperator.filter(batch, arrowExpr, allocator))
              .map(batch -> ArrowProjectOperator.project(batch, requiredColumns, allocator));
      batches = adapted.onClose(projected::close).onClose(filtered::close).onClose(rows::close);
    }
    return new ArrowScanPlan(schema, batches);
  }
}
