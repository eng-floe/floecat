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

package ai.floedb.floecat.systemcatalog.spi.scanner;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.columnar.ColumnarBatch;
import ai.floedb.floecat.systemcatalog.expr.Expr;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Scanner for system objects (information_schema, floecat.system.*, plugin-provided).
 *
 * <p>Performance constraints: - Must be lazy - Must avoid allocations - Must avoid boxing in the
 * hot path - Must match SchemaColumn[] exactly on every scan
 */
public interface SystemObjectScanner {

  /** Returns the fixed schema (Arrow compatible). */
  List<SchemaColumn> schema();

  /** Lazily streams rows as lightweight wrappers around Object[]. */
  Stream<SystemObjectRow> scan(SystemObjectScanContext ctx);

  /**
   * Arrow-native scan.
   *
   * <p>The {@code predicate} argument represents the expression that will be evaluated downstream
   * by the engine; implementations are free to ignore it today since filtering happens later.
   * {@code requiredColumns} may be restricted by the caller and duplicates/unknown names are
   * allowed—they will be filtered out by the projector.
   */
  default Stream<ColumnarBatch> scanArrow(
      SystemObjectScanContext ctx,
      Expr predicate,
      List<String> requiredColumns,
      BufferAllocator allocator) {
    return Stream.empty();
  }

  /** Declares supported execution formats. */
  default EnumSet<ScanOutputFormat> supportedFormats() {
    return EnumSet.of(ScanOutputFormat.ROWS);
  }
}
