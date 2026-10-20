/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.client.unity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class UnityCatalogTypeTextTest {
  @Test
  void normalizesSqlScalarAliasesToDeltaNames() {
    assertThat(UnityCatalogTypeText.toDeltaType("int").asText()).isEqualTo("integer");
    assertThat(UnityCatalogTypeText.toDeltaType("BIGINT").asText()).isEqualTo("long");
    assertThat(UnityCatalogTypeText.toDeltaType("smallint").asText()).isEqualTo("short");
    assertThat(UnityCatalogTypeText.toDeltaType("tinyint").asText()).isEqualTo("byte");
    assertThat(UnityCatalogTypeText.toDeltaType("varchar(20)").asText()).isEqualTo("string");
    assertThat(UnityCatalogTypeText.toDeltaType("char(4)").asText()).isEqualTo("string");
    assertThat(UnityCatalogTypeText.toDeltaType("VARCHAR").asText()).isEqualTo("string");
    assertThat(UnityCatalogTypeText.toDeltaType("decimal( 38, 9 )").asText())
        .isEqualTo("decimal(38,9)");
  }

  @Test
  void convertsNestedCatalogStringsToDeltaTypeJson() {
    assertThat(
            UnityCatalogTypeText.toDeltaType(
                    "struct<`order.id`:bigint,items:array<map<string,decimal(10,2)>>> ")
                .toString())
        .isEqualTo(
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"order.id\",\"type\":\"long\","
                + "\"nullable\":true,\"metadata\":{}},{\"name\":\"items\",\"type\":{"
                + "\"type\":\"array\",\"elementType\":{\"type\":\"map\",\"keyType\":"
                + "\"string\",\"valueType\":\"decimal(10,2)\",\"valueContainsNull\":true},"
                + "\"containsNull\":true},\"nullable\":true,\"metadata\":{}}]}");
  }

  @Test
  void rejectsUnsupportedOrPartialTypes() {
    assertThatThrownBy(() -> UnityCatalogTypeText.toDeltaType("interval day to second"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unsupported Unity column type");
    assertThatThrownBy(() -> UnityCatalogTypeText.toDeltaType("array<int"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expected '>'");
  }
}
