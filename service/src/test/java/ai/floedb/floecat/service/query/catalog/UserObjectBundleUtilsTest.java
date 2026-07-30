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

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.query.rpc.Origin;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.types.LogicalTypeProtoAdapter;
import java.util.List;
import org.junit.jupiter.api.Test;

class UserObjectBundleUtilsTest {

  @Test
  void columnInfoPropagatesCoreFields() {
    SchemaColumn schema =
        SchemaColumn.newBuilder()
            .setId(42)
            .setName("ts")
            .setType(LogicalTypeProtoAdapter.parseToProto("TIMESTAMPTZ"))
            .setNullable(true)
            .setOrdinal(1)
            .build();

    var info = UserObjectBundleUtils.columnInfo(schema, Origin.ORIGIN_USER);

    assertThat(info.getId()).isEqualTo(42);
    assertThat(info.getName()).isEqualTo("ts");
    assertThat(info.getNullable()).isTrue();
    assertThat(info.getOrdinal()).isEqualTo(1);
  }

  @Test
  void qualifyNestedColumnNamesUsesPhysicalPathForNestedLeaves() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder().setName("flags").setPhysicalPath("flags").build(),
            SchemaColumn.newBuilder().setName("links").setPhysicalPath("links").build(),
            SchemaColumn.newBuilder().setName("flags").setPhysicalPath("links[].flags").build(),
            SchemaColumn.newBuilder()
                .setName("span_schema_url")
                .setPhysicalPath("span_schema_url")
                .build(),
            SchemaColumn.newBuilder().setName("message").setPhysicalPath("status.message").build());

    assertThat(UserObjectBundleUtils.qualifyNestedColumnNames(schema))
        .extracting(SchemaColumn::getName)
        .containsExactly(
            "flags", "links", "links.element.flags", "span_schema_url", "status.message");
  }

  @Test
  void toCatalystPathRewritesCollectionSegments() {
    assertThat(UserObjectBundleUtils.toCatalystPath("tags[]")).isEqualTo("tags.element");
    assertThat(UserObjectBundleUtils.toCatalystPath("links[].flags"))
        .isEqualTo("links.element.flags");
    assertThat(UserObjectBundleUtils.toCatalystPath("props{}.value"))
        .isEqualTo("props.value.value");
  }

  private static SchemaColumn nested(String name, String path, String type) {
    return SchemaColumn.newBuilder()
        .setName(name)
        .setPhysicalPath(path)
        .setType(LogicalTypeProtoAdapter.parseToProto(type))
        .setNullable(true)
        .build();
  }

  @Test
  void qualifiesTopLevelContainerPlaceholders() {
    // Placeholder paths contain no dot, but their bare names ("element"/"value") collide across
    // columns; they must be qualified like every other nested row.
    assertThat(
            UserObjectBundleUtils.withQualifiedNestedName(nested("element", "arr[]", "INT"))
                .getName())
        .isEqualTo("arr.element");
    assertThat(
            UserObjectBundleUtils.withQualifiedNestedName(nested("value", "m{}", "INT")).getName())
        .isEqualTo("m.value");
    // Top-level rows (path == name) stay untouched.
    assertThat(
            UserObjectBundleUtils.withQualifiedNestedName(nested("arr", "arr", "ARRAY<INT>"))
                .getName())
        .isEqualTo("arr");
  }

  @Test
  void pruneSchemaAcceptsTwoArrayColumnsAfterQualification() {
    // Two arrays used to produce two rows named "element", making pruneSchema throw
    // CATALOG_BUNDLE_SCHEMA_DUPLICATE_COLUMN for any projection request.
    var schema =
        UserObjectBundleUtils.qualifyNestedColumnNames(
            java.util.List.of(
                nested("a", "a", "ARRAY<INT>"),
                nested("element", "a[]", "INT"),
                nested("b", "b", "ARRAY<INT>"),
                nested("element", "b[]", "INT")));
    var candidate =
        ai.floedb.floecat.query.rpc.TableReferenceCandidate.newBuilder()
            .addInitialColumns("a")
            .build();
    var pruned = UserObjectBundleUtils.pruneSchema(schema, candidate, "corr");
    assertThat(pruned).extracting(SchemaColumn::getName).contains("a");
  }
}
