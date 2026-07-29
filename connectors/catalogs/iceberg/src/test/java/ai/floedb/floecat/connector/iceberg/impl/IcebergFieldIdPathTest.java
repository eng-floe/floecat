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

package ai.floedb.floecat.connector.iceberg.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Verifies the stats-side fieldId→path traversal produces the same canonical paths as
 * IcebergSchemaMapper, i.e. no synthetic "element"/"key"/"value" names leak into paths.
 */
class IcebergFieldIdPathTest {

  @Test
  void listOfStructChildrenUseCanonicalBracketPaths() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1,
                "arr",
                Types.ListType.ofOptional(
                    2,
                    Types.StructType.of(
                        Types.NestedField.required(3, "x", Types.IntegerType.get())))));

    Map<Integer, String> paths = IcebergConnector.fieldIdMaps(schema).getKey();

    assertThat(paths.get(1)).isEqualTo("arr");
    // The list's element field is recorded at "arr[]" — not "arr[].element".
    assertThat(paths.get(2)).isEqualTo("arr[]");
    // Struct-element children match IcebergSchemaMapper's "arr[].x", not "arr[].element.x".
    assertThat(paths.get(3)).isEqualTo("arr[].x");
  }

  @Test
  void mapValueStructChildrenUseCanonicalBracePaths() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1,
                "attrs",
                Types.MapType.ofOptional(
                    2,
                    3,
                    Types.StringType.get(),
                    Types.StructType.of(
                        Types.NestedField.optional(4, "v", Types.LongType.get())))));

    Map<Integer, String> paths = IcebergConnector.fieldIdMaps(schema).getKey();

    assertThat(paths.get(1)).isEqualTo("attrs");
    assertThat(paths.get(2)).isEqualTo("attrs.key");
    // The map's value field is recorded at "attrs{}" — not "attrs.value.value".
    assertThat(paths.get(3)).isEqualTo("attrs{}");
    // Value-struct children match IcebergSchemaMapper's "attrs{}.v".
    assertThat(paths.get(4)).isEqualTo("attrs{}.v");
  }

  @Test
  void nestedListsStackBrackets() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1,
                "matrix",
                Types.ListType.ofOptional(
                    2, Types.ListType.ofRequired(3, Types.IntegerType.get()))));

    Map<Integer, String> paths = IcebergConnector.fieldIdMaps(schema).getKey();

    assertThat(paths.get(2)).isEqualTo("matrix[]");
    assertThat(paths.get(3)).isEqualTo("matrix[][]");
  }
}
