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

import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import java.util.List;
import java.util.stream.Stream;

/** information_schema.table_constraints */
public final class TableConstraintsScanner implements SystemObjectScanner {

  public static final List<SchemaColumn> SCHEMA =
      List.of(
          col("constraint_catalog", false),
          col("constraint_schema", false),
          col("constraint_name", false),
          col("table_catalog", false),
          col("table_schema", false),
          col("table_name", false),
          col("constraint_type", false),
          col("is_deferrable", false),
          col("initially_deferred", false),
          col("enforced", true));

  @Override
  public List<SchemaColumn> schema() {
    return SCHEMA;
  }

  @Override
  public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
    return ConstraintScanIndex.build(ctx).entries().stream()
        .map(
            e ->
                new SystemObjectRow(
                    new Object[] {
                      e.table().catalog(),
                      e.table().schema(),
                      e.name(),
                      e.table().catalog(),
                      e.table().schema(),
                      e.table().name(),
                      e.typeName(),
                      "NO",
                      "NO",
                      e.enforced()
                    }));
  }

  private static SchemaColumn col(String name, boolean nullable) {
    return SchemaColumn.newBuilder()
        .setName(name)
        .setLogicalType("VARCHAR")
        .setNullable(nullable)
        .build();
  }
}
