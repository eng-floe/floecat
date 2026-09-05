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

package ai.floedb.floecat.service.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.connector.common.resolver.IcebergSchemaMapper;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The contract {@code CatalogOverlayReconciler.viewFor} imposes on a provider's view schema.
 *
 * <p>It maps {@code CatalogView.outputSchemaJson} with {@code IcebergSchemaMapper}, which parses
 * through Iceberg's {@code SchemaParser} -- so the string has to be Iceberg schema JSON, with an
 * {@code id} and {@code required} on every field. Nothing in the SPI says so, and a provider that
 * emits its own native form does not fail at {@code loadView}: it fails inside the reconciler, and
 * takes the whole overlay down with it, every table included.
 *
 * <p>The fixtures are literal rather than produced by the provider because {@code
 * catalog-access/unity} cannot see Iceberg and this module cannot see the provider's emitter. What
 * is pinned here is the shape; {@code UnityCatalogAccessClientTest} pins that the provider emits
 * it.
 */
class UnityViewSchemaConsumerContractTest {

  private static final String ICEBERG_FORM =
      """
      {"type":"struct","schema-id":0,"fields":[\
      {"id":1,"name":"id","required":true,"type":"long"},\
      {"id":2,"name":"name","required":false,"type":"string"},\
      {"id":3,"name":"amount","required":false,"type":"decimal(10, 2)"}]}\
      """;

  /** Delta/Spark form: fields carry nullable and no id, which is what Unity used to emit. */
  private static final String DELTA_FORM =
      """
      {"type":"struct","fields":[\
      {"name":"id","type":"long","nullable":false},\
      {"name":"name","type":"string","nullable":true}]}\
      """;

  /**
   * The nested form the provider emits for array, map and struct output columns. Iceberg needs an
   * id on every member of a container, not just on top-level fields, and it rejects the schema if
   * any is missing or duplicated -- which is the whole reason the provider draws nested ids from
   * the same counter as the fields.
   *
   * <p>Literal here for the same reason as the flat fixture: catalog-access/unity cannot see
   * Iceberg. UnityCatalogAccessClientTest pins that the provider emits this; this pins that the
   * consumer accepts it.
   */
  private static final String NESTED_FORM =
      """
      {"type":"struct","schema-id":0,"fields":[\
      {"id":1,"name":"id","required":true,"type":"long"},\
      {"id":2,"name":"tags","required":false,"type":\
      {"type":"list","element-id":3,"element-required":false,"element":"string"}},\
      {"id":4,"name":"props","required":false,"type":\
      {"type":"map","key-id":5,"key":"string","value-id":6,"value-required":true,"value":"long"}},\
      {"id":7,"name":"addr","required":false,"type":{"type":"struct","fields":[\
      {"id":8,"name":"city","required":false,"type":"string"},\
      {"id":9,"name":"zip","required":true,"type":"int"}]}}]}\
      """;

  /**
   * The nested fixture is the one case where field ids are not positional -- {@code tags} is 2,
   * {@code props} 4 and {@code addr} 7, because container members draw from the same counter as the
   * fields. Asserting the ids that come out is the point: {@code map} throws on a schema it cannot
   * parse, so a non-null check passed whatever the provider emitted, and a regression in id
   * assignment or container translation would have gone straight through.
   */
  @Test
  void nestedIcebergFormMapsToOutputColumns() {
    var schema = IcebergSchemaMapper.map(ColumnIdAlgorithm.CID_FIELD_ID, NESTED_FORM, Set.of());

    // Nine, not four: each container member is its own row.
    assertThat(schema.getColumnsList()).hasSize(9);
    assertThat(schema.getColumnsList())
        .extracting(c -> c.getPhysicalPath() + "#" + c.getFieldId())
        .containsExactly(
            "id#1",
            "tags#2",
            // The element id the provider drew from the shared counter, on the path the planner
            // addresses it by.
            "tags[]#3",
            "props#4",
            "props.key#5",
            "props{}#6",
            "addr#7",
            "addr.city#8",
            "addr.zip#9");
  }

  @Test
  void icebergFormMapsToOutputColumns() {
    var schema = IcebergSchemaMapper.map(ColumnIdAlgorithm.CID_FIELD_ID, ICEBERG_FORM, Set.of());

    assertThat(schema.getColumnsList())
        .extracting(c -> c.getName() + "#" + c.getFieldId())
        .containsExactly("id#1", "name#2", "amount#3");
  }

  /**
   * And the reason the provider now refuses a relation with no columns. Iceberg's parser accepts an
   * empty field list, and this mapper answers with a descriptor holding no columns rather than
   * throwing -- so a Unity response whose {@code columns} was absent used to be persisted as a
   * relation with no output columns and reported created.
   */
  @Test
  void anEmptyFieldListMapsToNoColumnsRatherThanFailing() {
    var schema =
        IcebergSchemaMapper.map(
            ColumnIdAlgorithm.CID_FIELD_ID,
            "{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}",
            Set.of());

    assertThat(schema.getColumnsList()).isEmpty();
  }

  /**
   * The failure this guards against, stated as a fact rather than a worry. If this ever stops
   * throwing, Iceberg's parser has become lenient and the provider is free to emit its native form.
   */
  @Test
  void deltaFormIsRejectedByTheConsumer() {
    assertThatThrownBy(
            () -> IcebergSchemaMapper.map(ColumnIdAlgorithm.CID_FIELD_ID, DELTA_FORM, Set.of()))
        .isInstanceOf(RuntimeException.class);
  }
}
