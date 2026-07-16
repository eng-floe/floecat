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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.def.SystemColumnDef;
import java.util.List;
import org.junit.jupiter.api.Test;

class SystemSchemaMapperTest {

  @Test
  void toSchemaColumn_defaultsMissingIdToOrdinal() {
    SystemColumnDef column =
        new SystemColumnDef("table_name", name("VARCHAR"), false, 3, null, List.of());

    SchemaColumn schemaColumn = SystemSchemaMapper.toSchemaColumn(column);

    assertThat(schemaColumn.getId()).isEqualTo(3);
    assertThat(schemaColumn.getOrdinal()).isEqualTo(3);
  }

  @Test
  void toSchemaColumn_preservesExplicitId() {
    SystemColumnDef column =
        new SystemColumnDef("table_name", name("VARCHAR"), false, 3, 42L, List.of());

    SchemaColumn schemaColumn = SystemSchemaMapper.toSchemaColumn(column);

    assertThat(schemaColumn.getId()).isEqualTo(42);
    assertThat(schemaColumn.getOrdinal()).isEqualTo(3);
  }

  @Test
  void toSchemaColumns_rejectsExplicitIdCollidingWithAFallbackOrdinalId() {
    // Explicit ids and ordinal-derived fallback ids share one positive integer space: column 1
    // has no id (falls back to ordinal 1) while column 2 explicitly claims id 1. Serving would
    // key both columns' stats on the same target id, so the mix must fail at build time.
    List<SystemColumnDef> mixed =
        List.of(
            new SystemColumnDef("a", name("VARCHAR"), false, 1, null, List.of()),
            new SystemColumnDef("b", name("VARCHAR"), false, 2, 1L, List.of()));

    assertThatThrownBy(() -> SystemSchemaMapper.toSchemaColumns(mixed))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("duplicate column id 1");
  }

  @Test
  void toSchemaColumns_acceptsDisjointExplicitAndFallbackIds() {
    List<SystemColumnDef> mixed =
        List.of(
            new SystemColumnDef("a", name("VARCHAR"), false, 1, null, List.of()),
            new SystemColumnDef("b", name("VARCHAR"), false, 2, 42L, List.of()));

    assertThat(SystemSchemaMapper.toSchemaColumns(mixed))
        .extracting(SchemaColumn::getId)
        .containsExactly(1L, 42L);
  }

  @Test
  void fromSchemaColumns_rejectsBlankLogicalType() {
    SchemaColumn column =
        SchemaColumn.newBuilder()
            .setName("id")
            .setLogicalType("")
            .setNullable(false)
            .setOrdinal(1)
            .build();

    assertThatThrownBy(() -> SystemSchemaMapper.fromSchemaColumns(List.of(column)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("logicalType");
  }

  private static NameRef name(String name) {
    return NameRef.newBuilder().setName(name).build();
  }
}
