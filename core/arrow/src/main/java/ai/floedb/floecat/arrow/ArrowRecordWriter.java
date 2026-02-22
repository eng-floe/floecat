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

import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/** Strategy for writing a sequence of Java records into an Arrow {@link VectorSchemaRoot}. */
public interface ArrowRecordWriter<T extends Record> {

  /** The Arrow schema produced by this writer. */
  Schema schema();

  /**
   * Writes {@code rows} into the supplied {@link VectorSchemaRoot}. The root has already been
   * allocated and the implementation must call {@code root.setRowCount(...)} when data is ready;
   * the writer must not close the root.
   */
  void write(VectorSchemaRoot root, List<T> rows);
}
