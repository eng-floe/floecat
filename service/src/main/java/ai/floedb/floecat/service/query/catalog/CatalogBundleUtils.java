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

package ai.floedb.floecat.service.query.catalog;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.query.rpc.ColumnInfo;
import ai.floedb.floecat.query.rpc.Origin;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.TableReferenceCandidate;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Shared helpers used by catalog bundle builders. */
public final class CatalogBundleUtils {

  private CatalogBundleUtils() {}

  public static NameRef applyDefaultCatalog(NameRef ref, String defaultCatalog) {
    NameRef.Builder builder = ref.toBuilder();
    if (builder.getCatalog().isEmpty() && defaultCatalog != null && !defaultCatalog.isBlank()) {
      builder.setCatalog(defaultCatalog);
    }
    return builder.build();
  }

  public static List<SchemaColumn> pruneSchema(
      List<SchemaColumn> schema, TableReferenceCandidate candidate, String correlationId) {
    if (candidate.getWantsAllColumns() || candidate.getInitialColumnsCount() == 0) {
      return schema;
    }
    var columns = new ArrayList<SchemaColumn>();
    var lookup = new HashMap<String, SchemaColumn>();
    for (SchemaColumn column : schema) {
      if (lookup.put(column.getName(), column) != null) {
        throw GrpcErrors.invalidArgument(
            correlationId,
            "catalog_bundle.schema.duplicate_column",
            Map.of("column", column.getName()));
      }
    }
    for (String name : candidate.getInitialColumnsList()) {
      SchemaColumn column = lookup.get(name);
      if (column == null) {
        throw GrpcErrors.invalidArgument(
            correlationId, "catalog_bundle.schema.unknown_column", Map.of("column", name));
      }
      columns.add(column);
    }
    return columns;
  }

  public static ColumnInfo columnInfo(SchemaColumn column, Origin origin) {
    return ColumnInfo.newBuilder()
        .setId(column.getId())
        .setName(column.getName())
        .setType(NameRefUtil.name(column.getLogicalType()))
        .setNullable(column.getNullable())
        .setOrdinal(column.getOrdinal())
        .setOrigin(origin)
        .setFieldId(column.getFieldId())
        .setPhysicalPath(column.getPhysicalPath())
        .build();
  }

  public static List<ColumnInfo> columnsFor(
      List<SchemaColumn> schema, List<SchemaColumn> pruned, Origin origin, String correlationId) {
    List<ColumnInfo> columns = new ArrayList<>(Math.max(0, pruned.size()));

    for (SchemaColumn column : pruned) {
      columns.add(columnInfo(column, origin));
    }

    return columns;
  }
}
