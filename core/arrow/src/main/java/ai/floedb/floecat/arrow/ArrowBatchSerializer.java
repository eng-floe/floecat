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

package ai.floedb.floecat.arrow;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SequencedSet;
import java.util.function.Supplier;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Shared Arrow batch serialisation loop.
 *
 * <p>Drives an {@link ArrowScanPlan} through an {@link ArrowBatchSink}, ensuring:
 *
 * <ul>
 *   <li>{@link ArrowBatchSink#onSchema} is called exactly once with the projected schema.
 *   <li>{@link ArrowBatchSink#onBatch} is called for each {@link ColumnarBatch}.
 *   <li>{@link ArrowBatchSink#onComplete} is called on clean termination.
 *   <li>The plan and allocator are closed on both success and failure.
 * </ul>
 *
 * <p>Using this class for both gRPC and Arrow Flight transports ensures that any future
 * optimisation (compression, statistics headers, batch-size tuning) applies to all protocols
 * automatically.
 */
public final class ArrowBatchSerializer {

  private ArrowBatchSerializer() {}

  /**
   * Drives the plan into the sink. Closes the plan and calls {@code cleanup} regardless of outcome.
   *
   * @param plan the scan plan to drain
   * @param sink the protocol-specific sink
   * @param isCancelled supplier that returns {@code true} when the caller has abandoned the stream
   * @param cleanup called once when the stream terminates (success or failure); must release the
   *     allocator and any other resources
   */
  public static void serialize(
      ArrowScanPlan plan, ArrowBatchSink sink, Supplier<Boolean> isCancelled, Runnable cleanup) {
    try {
      sink.onSchema(plan.schema());
      Iterator<ColumnarBatch> iterator = plan.iterator();
      while (!isCancelled.get() && iterator.hasNext()) {
        ColumnarBatch batch;
        try {
          batch = iterator.next();
        } catch (java.util.NoSuchElementException ignored) {
          break;
        }
        try {
          sink.onBatch(batch.root());
        } finally {
          batch.close();
        }
      }
      if (!isCancelled.get()) {
        sink.onComplete();
      }
    } finally {
      try {
        plan.close();
      } catch (Exception ignored) {
      }
      cleanup.run();
    }
  }

  /**
   * Returns the projected Arrow schema for the given scanner schema and required columns.
   *
   * <p>Semantics:
   *
   * <ul>
   *   <li>If {@code requiredColumns} is empty, the full schema is returned.
   *   <li>Unknown column names are silently dropped.
   *   <li>Duplicates in {@code requiredColumns} are de-duplicated preserving first occurrence.
   *   <li>Column ordering in the result follows {@code requiredColumns} ordering.
   * </ul>
   */
  public static Schema schemaForColumns(
      List<SchemaColumn> scannerSchema, List<String> requiredColumns) {
    if (requiredColumns == null || requiredColumns.isEmpty()) {
      return ArrowSchemaUtil.toArrowSchema(scannerSchema);
    }
    // Build ordered de-duplicated set of lower-cased requested column names.
    SequencedSet<String> requested = new LinkedHashSet<>();
    for (String col : requiredColumns) {
      if (col != null && !col.isBlank()) {
        requested.add(col.trim().toLowerCase(Locale.ROOT));
      }
    }
    if (requested.isEmpty()) {
      return ArrowSchemaUtil.toArrowSchema(scannerSchema);
    }
    // Build a lookup map once to avoid re-scanning the schema for each requested column.
    Map<String, SchemaColumn> schemaByName = new HashMap<>(scannerSchema.size());
    for (SchemaColumn col : scannerSchema) {
      schemaByName.putIfAbsent(col.getName().toLowerCase(Locale.ROOT), col);
    }
    List<SchemaColumn> projected = new ArrayList<>(requested.size());
    for (String reqName : requested) {
      SchemaColumn match = schemaByName.get(reqName);
      if (match != null) {
        projected.add(match);
      }
      // unknown names are silently dropped
    }
    return ArrowSchemaUtil.toArrowSchema(projected);
  }
}
