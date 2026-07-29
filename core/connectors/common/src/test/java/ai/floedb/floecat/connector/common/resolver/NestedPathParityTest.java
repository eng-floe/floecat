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

package ai.floedb.floecat.connector.common.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Schema construction and the stats-side fieldId→path maps must emit the same canonical path set.
 * Both go through {@link IcebergNestedPaths}; these tests pin the complete emitted path sets so an
 * independent reimplementation of either walk cannot silently drift.
 */
class NestedPathParityTest {

  private static Set<String> schemaPaths(Schema schema) {
    Set<String> paths = new LinkedHashSet<>();
    IcebergSchemaMapper.map(ColumnIdAlgorithm.CID_FIELD_ID, SchemaParser.toJson(schema), Set.of())
        .getColumnsList()
        .stream()
        .map(SchemaColumn::getPhysicalPath)
        .forEach(paths::add);
    return paths;
  }

  private static Set<String> statsPaths(Schema schema) {
    Set<String> paths = new LinkedHashSet<>();
    IcebergNestedPaths.walk(schema, (field, path, ordinal) -> paths.add(path));
    return paths;
  }

  @Test
  void listOfListPathsMatch() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1,
                "matrix",
                Types.ListType.ofOptional(
                    2, Types.ListType.ofRequired(3, Types.IntegerType.get()))));

    assertThat(schemaPaths(schema))
        .containsExactly("matrix", "matrix[]", "matrix[][]")
        .isEqualTo(statsPaths(schema));
  }

  @Test
  void mapOfListOfStructPathsMatch() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1,
                "attrs",
                Types.MapType.ofOptional(
                    2,
                    3,
                    Types.StringType.get(),
                    Types.ListType.ofOptional(
                        4,
                        Types.StructType.of(
                            Types.NestedField.required(5, "x", Types.IntegerType.get()))))));

    assertThat(schemaPaths(schema))
        .containsExactly("attrs", "attrs.key", "attrs{}", "attrs{}[]", "attrs{}[].x")
        .isEqualTo(statsPaths(schema));
  }

  @Test
  void arrayOfStructOfArrayPathsMatch() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1,
                "items",
                Types.ListType.ofOptional(
                    2,
                    Types.StructType.of(
                        Types.NestedField.optional(
                            3, "a", Types.ListType.ofOptional(4, Types.IntegerType.get()))))));

    assertThat(schemaPaths(schema))
        .containsExactly("items", "items[]", "items[].a", "items[].a[]")
        .isEqualTo(statsPaths(schema));
  }
}
