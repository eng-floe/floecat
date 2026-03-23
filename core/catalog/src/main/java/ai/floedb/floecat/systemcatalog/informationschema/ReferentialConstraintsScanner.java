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

/** information_schema.referential_constraints */
public final class ReferentialConstraintsScanner implements SystemObjectScanner {

  public static final List<SchemaColumn> SCHEMA =
      List.of(
          col("constraint_catalog", false),
          col("constraint_schema", false),
          col("constraint_name", false),
          col("unique_constraint_catalog", true),
          col("unique_constraint_schema", true),
          col("unique_constraint_name", true),
          col("match_option", false),
          col("update_rule", false),
          col("delete_rule", false));

  @Override
  public List<SchemaColumn> schema() {
    return SCHEMA;
  }

  @Override
  public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
    return ConstraintScanIndex.build(ctx).entries().stream()
        .filter(e -> e.type() == ConstraintType.CT_FOREIGN_KEY)
        .map(
            e -> {
              boolean hasReferencedConstraint =
                  e.referencedConstraintName() != null && !e.referencedConstraintName().isBlank();
              String uniqueCatalog =
                  hasReferencedConstraint && e.referencedTable() != null
                      ? e.referencedTable().catalog()
                      : null;
              String uniqueSchema =
                  hasReferencedConstraint && e.referencedTable() != null
                      ? e.referencedTable().schema()
                      : null;
              String uniqueName = hasReferencedConstraint ? e.referencedConstraintName() : null;
              return new SystemObjectRow(
                  new Object[] {
                    e.table().catalog(),
                    e.table().schema(),
                    e.name(),
                    uniqueCatalog,
                    uniqueSchema,
                    uniqueName,
                    e.matchOption(),
                    e.updateRule(),
                    e.deleteRule()
                  });
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
