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

package ai.floedb.floecat.systemcatalog.util;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.def.SystemColumnDef;
import java.util.List;
import java.util.Objects;

/** Helper that adapts between {@link SystemColumnDef} and {@link SchemaColumn}. */
public final class SystemSchemaMapper {

  private SystemSchemaMapper() {}

  public static SchemaColumn toSchemaColumn(SystemColumnDef column) {
    Objects.requireNonNull(column, "column");
    SchemaColumn.Builder builder =
        SchemaColumn.newBuilder()
            .setName(column.name())
            .setLogicalType(column.type().getName())
            .setNullable(column.nullable())
            .setOrdinal(column.ordinal());
    if (column.hasId()) {
      builder.setId(column.id());
    }
    return builder.build();
  }

  public static List<SchemaColumn> toSchemaColumns(List<SystemColumnDef> columns) {
    if (columns == null || columns.isEmpty()) {
      return List.of();
    }
    return columns.stream().map(SystemSchemaMapper::toSchemaColumn).toList();
  }

  private static SystemColumnDef fromSchemaColumn(SchemaColumn column, int ordinal) {
    Objects.requireNonNull(column, "column");
    String logicalType = column.getLogicalType();
    if (logicalType == null || logicalType.isBlank()) {
      throw new IllegalArgumentException(
          "logicalType must be provided for column '" + column.getName() + "'");
    }
    NameRef type = NameRef.newBuilder().setName(logicalType).build();
    Long columnId = column.getId() != 0 ? column.getId() : null;
    return new SystemColumnDef(
        column.getName(), type, column.getNullable(), ordinal, columnId, List.of());
  }

  public static List<SystemColumnDef> fromSchemaColumns(List<SchemaColumn> columns) {
    if (columns == null || columns.isEmpty()) {
      return List.of();
    }
    List<SystemColumnDef> defs = new java.util.ArrayList<>(columns.size());
    for (int i = 0; i < columns.size(); i++) {
      defs.add(fromSchemaColumn(columns.get(i), i + 1));
    }
    return List.copyOf(defs);
  }
}
