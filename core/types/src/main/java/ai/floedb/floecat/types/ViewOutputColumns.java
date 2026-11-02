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

package ai.floedb.floecat.types;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import java.util.List;

/** A view's output columns: which rows of a mapped schema they are, and their positions. */
public final class ViewOutputColumns {
  private ViewOutputColumns() {}

  /**
   * The output columns of {@code schema}, a view's output schema as a mapper flattens it: exactly
   * its top-level columns, complex or not, each kept as mapped but for its id, which is cleared. A
   * top-level row's physical path is its name; a nested row's goes below it ({@code parent.child},
   * {@code parent[].x}, {@code parent{}.x}). {@code leaf} is a stats flag and decides nothing here.
   */
  public static List<SchemaColumn> of(SchemaDescriptor schema) {
    return schema.getColumnsList().stream()
        .filter(c -> c.getPhysicalPath().isEmpty() || c.getPhysicalPath().equals(c.getName()))
        .map(c -> c.toBuilder().clearId().build())
        .toList();
  }
}
