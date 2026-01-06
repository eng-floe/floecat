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

import ai.floedb.floecat.systemcatalog.columnar.ColumnarBatch;
import java.util.Iterator;
import java.util.stream.Stream;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Simple holder for the Arrow schema and the lazily produced batch stream.
 *
 * <p>Callers must close the plan to release the underlying stream resources.
 */
public final class ArrowScanPlan implements AutoCloseable {
  private final Schema schema;
  private final Stream<ColumnarBatch> batches;

  ArrowScanPlan(Schema schema, Stream<ColumnarBatch> batches) {
    this.schema = schema;
    this.batches = batches;
  }

  public Schema schema() {
    return schema;
  }

  public Iterator<ColumnarBatch> iterator() {
    return batches.iterator();
  }

  @Override
  public void close() {
    batches.close();
  }
}
