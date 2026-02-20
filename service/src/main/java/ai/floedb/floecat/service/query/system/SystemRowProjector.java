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

package ai.floedb.floecat.service.query.system;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import java.util.List;
import java.util.stream.Stream;

public final class SystemRowProjector {

  public static List<SystemObjectRow> project(
      List<SystemObjectRow> rows, List<SchemaColumn> schema, List<String> requiredColumns) {

    if (requiredColumns.isEmpty()) return rows;

    return project(rows.stream(), schema, requiredColumns).toList();
  }

  public static Stream<SystemObjectRow> project(
      Stream<SystemObjectRow> rows, List<SchemaColumn> schema, List<String> requiredColumns) {

    if (requiredColumns.isEmpty()) return rows;

    int[] indexes =
        requiredColumns.stream().mapToInt(c -> indexOf(schema, c)).filter(i -> i >= 0).toArray();

    return rows.map(r -> projectRow(r, indexes));
  }

  private static SystemObjectRow projectRow(SystemObjectRow row, int[] idxs) {

    Object[] src = row.values();
    Object[] dst = new Object[idxs.length];

    for (int i = 0; i < idxs.length; i++) {
      dst[i] = src[idxs[i]];
    }

    return new SystemObjectRow(dst);
  }

  private static int indexOf(List<SchemaColumn> schema, String name) {

    for (int i = 0; i < schema.size(); i++) {
      if (schema.get(i).getName().equalsIgnoreCase(name)) {
        return i;
      }
    }
    return -1;
  }

  private SystemRowProjector() {}
}
