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

package ai.floedb.floecat.schema.identity.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.schema.identity.ColumnPath;
import ai.floedb.floecat.schema.identity.NodeKind;
import ai.floedb.floecat.schema.identity.SchemaFixtures;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;

class IcebergSchemaResolverTest {

  @Test
  void everyNodeCarriesItsIcebergFieldId() {
    var schema = IcebergSchemaResolver.resolve(SchemaFixtures.ICEBERG_JSON);

    assertThat(schema.format()).isEqualTo(TableFormat.TF_ICEBERG);
    assertThat(schema.hasTotalNativeIds()).isTrue();
    assertThat(schema.byPath(ColumnPath.ROOT.field("customer")).orElseThrow().nativeFieldId())
        .isEqualTo(OptionalInt.of(1));
  }

  @Test
  void collectionInteriorsCarryElementKeyAndValueIds() {
    var schema = IcebergSchemaResolver.resolve(SchemaFixtures.ICEBERG_JSON);
    ColumnPath orders = ColumnPath.ROOT.field("customer").field("orders");
    ColumnPath attributes = ColumnPath.ROOT.field("customer").field("attributes");

    assertThat(schema.byPath(orders.arrayElement()).orElseThrow())
        .returns(OptionalInt.of(5), n -> n.nativeFieldId())
        .returns(NodeKind.ARRAY_ELEMENT, n -> n.kind());
    assertThat(schema.byPath(attributes.mapKey()).orElseThrow().nativeFieldId())
        .isEqualTo(OptionalInt.of(8));
    assertThat(schema.byPath(attributes.mapValue()).orElseThrow().nativeFieldId())
        .isEqualTo(OptionalInt.of(9));
  }

  @Test
  void synthesizedNamesNeverEnterPaths() {
    var schema = IcebergSchemaResolver.resolve(SchemaFixtures.ICEBERG_JSON);

    assertThat(schema.nodes())
        .noneMatch(n -> n.path().display().contains(".element"))
        .noneMatch(n -> n.path().display().contains(".value"));
  }

  @Test
  void collectionInteriorsHaveNoName() {
    var schema = IcebergSchemaResolver.resolve(SchemaFixtures.ICEBERG_JSON);
    ColumnPath orders = ColumnPath.ROOT.field("customer").field("orders");

    assertThat(schema.byPath(orders.arrayElement()).orElseThrow().name()).isEmpty();
    assertThat(schema.byPath(orders).orElseThrow().name()).contains("orders");
  }

  @Test
  void icebergHasNoPerNodePhysicalPath() {
    var schema = IcebergSchemaResolver.resolve(SchemaFixtures.ICEBERG_JSON);

    assertThat(schema.nodes()).allMatch(n -> n.sourcePhysicalPath().isEmpty());
  }

  @Test
  void ordinalsAreOneBasedWithinTheParent() {
    var schema = IcebergSchemaResolver.resolve(SchemaFixtures.ICEBERG_JSON);
    ColumnPath customer = ColumnPath.ROOT.field("customer");

    assertThat(schema.byPath(customer).orElseThrow().ordinal()).isEqualTo(1);
    assertThat(schema.byPath(customer.field("address")).orElseThrow().ordinal()).isEqualTo(1);
    assertThat(schema.byPath(customer.field("orders")).orElseThrow().ordinal()).isEqualTo(2);
    assertThat(schema.byPath(customer.field("attributes")).orElseThrow().ordinal()).isEqualTo(3);
    assertThat(schema.byPath(customer.field("attributes").mapKey()).orElseThrow().ordinal())
        .isEqualTo(1);
    assertThat(schema.byPath(customer.field("attributes").mapValue()).orElseThrow().ordinal())
        .isEqualTo(2);
  }

  @Test
  void blankSchemaYieldsNoNodes() {
    assertThat(IcebergSchemaResolver.resolve("").nodes()).isEmpty();
    assertThat(IcebergSchemaResolver.resolve((String) null).nodes()).isEmpty();
  }

  @Test
  void malformedSchemaFailsFast() {
    assertThatThrownBy(() -> IcebergSchemaResolver.resolve("{not json"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to parse Iceberg schema JSON");
  }
}
