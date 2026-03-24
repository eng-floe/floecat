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

package ai.floedb.floecat.systemcatalog.informationschema;

import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import java.util.List;
import java.util.stream.Stream;

/** information_schema.constraint_column_usage */
public final class ConstraintColumnUsageScanner implements SystemObjectScanner {

  public static final List<SchemaColumn> SCHEMA =
      List.of(
          col("constraint_catalog", false),
          col("constraint_schema", false),
          col("constraint_name", false),
          col("table_catalog", false),
          col("table_schema", false),
          col("table_name", false),
          col("column_name", false));

  @Override
  public List<SchemaColumn> schema() {
    return SCHEMA;
  }

  @Override
  public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
    return ConstraintScanIndex.build(ctx).entries().stream()
        .flatMap(
            entry -> {
              if (entry.type() == ConstraintType.CT_FOREIGN_KEY) {
                ConstraintScanIndex.TableRef referenced = entry.referencedTable();
                if (referenced == null) {
                  return Stream.empty();
                }
                return entry.referencedColumns().stream()
                    .map(
                        ref ->
                            new SystemObjectRow(
                                new Object[] {
                                  entry.table().catalog(),
                                  entry.table().schema(),
                                  entry.name(),
                                  referenced.catalog(),
                                  referenced.schema(),
                                  referenced.name(),
                                  ref.name()
                                }));
              }
              return entry.localColumns().stream()
                  .map(
                      local ->
                          new SystemObjectRow(
                              new Object[] {
                                entry.table().catalog(),
                                entry.table().schema(),
                                entry.name(),
                                entry.table().catalog(),
                                entry.table().schema(),
                                entry.table().name(),
                                local.name()
                              }));
            });
  }

  private static SchemaColumn col(String name, boolean nullable) {
    return SchemaColumn.newBuilder()
        .setName(name)
        .setLogicalType("VARCHAR")
        .setNullable(nullable)
        .build();
  }
}
