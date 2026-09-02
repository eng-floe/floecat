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

package ai.floedb.floecat.schema.identity;

/**
 * The same logical schema expressed in each supported format, so tests can assert that the
 * producers agree on the node universe:
 *
 * <pre>
 *   customer
 *   customer.address
 *   customer.address.city
 *   customer.orders
 *   customer.orders[]
 *   customer.orders[].product
 *   customer.attributes
 *   customer.attributes.key
 *   customer.attributes{}
 * </pre>
 */
public final class SchemaFixtures {

  private SchemaFixtures() {}

  public static final java.util.List<String> EXPECTED_PATHS =
      java.util.List.of(
          "customer",
          "customer.address",
          "customer.address.city",
          "customer.orders",
          "customer.orders[]",
          "customer.orders[].product",
          "customer.attributes",
          "customer.attributes.key",
          "customer.attributes{}");

  public static final java.util.List<NodeKind> EXPECTED_KINDS =
      java.util.List.of(
          NodeKind.FIELD,
          NodeKind.FIELD,
          NodeKind.FIELD,
          NodeKind.FIELD,
          NodeKind.ARRAY_ELEMENT,
          NodeKind.FIELD,
          NodeKind.FIELD,
          NodeKind.MAP_KEY,
          NodeKind.MAP_VALUE);

  public static final String ICEBERG_JSON =
      """
      {"type":"struct","schema-id":0,"fields":[
        {"id":1,"name":"customer","required":false,"type":{"type":"struct","fields":[
          {"id":2,"name":"address","required":false,"type":{"type":"struct","fields":[
            {"id":3,"name":"city","required":false,"type":"string"}
          ]}},
          {"id":4,"name":"orders","required":false,"type":{"type":"list","element-id":5,
            "element":{"type":"struct","fields":[
              {"id":6,"name":"product","required":false,"type":"string"}
            ]},"element-required":false}},
          {"id":7,"name":"attributes","required":false,"type":{"type":"map","key-id":8,
            "key":"string","value-id":9,"value":"string","value-required":false}}
        ]}}
      ]}
      """;

  /** The same shape in Delta, with column mapping off. */
  public static final String DELTA_UNMAPPED_JSON =
      """
      {"type":"struct","fields":[
        {"name":"customer","nullable":true,"metadata":{},"type":{"type":"struct","fields":[
          {"name":"address","nullable":true,"metadata":{},"type":{"type":"struct","fields":[
            {"name":"city","type":"string","nullable":true,"metadata":{}}
          ]}},
          {"name":"orders","nullable":true,"metadata":{},"type":{"type":"array",
            "containsNull":true,"elementType":{"type":"struct","fields":[
              {"name":"product","type":"string","nullable":true,"metadata":{}}
            ]}}},
          {"name":"attributes","nullable":true,"metadata":{},"type":{"type":"map",
            "valueContainsNull":true,"keyType":"string","valueType":"string"}}
        ]}}
      ]}
      """;

  /**
   * The same shape in Delta with column mapping on and nested ids present for every collection
   * interior, so native ids are total and can be adopted as canonical.
   */
  public static final String DELTA_MAPPED_JSON =
      """
      {"type":"struct","fields":[
        {"name":"customer","nullable":true,
         "metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-1"},
         "type":{"type":"struct","fields":[
          {"name":"address","nullable":true,
           "metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"col-2"},
           "type":{"type":"struct","fields":[
            {"name":"city","type":"string","nullable":true,
             "metadata":{"delta.columnMapping.id":3,"delta.columnMapping.physicalName":"col-3"}}
          ]}},
          {"name":"orders","nullable":true,
           "metadata":{"delta.columnMapping.id":4,"delta.columnMapping.physicalName":"col-4",
                       "delta.columnMapping.nested.ids":{"col-4.element":5}},
           "type":{"type":"array","containsNull":true,"elementType":{"type":"struct","fields":[
             {"name":"product","type":"string","nullable":true,
              "metadata":{"delta.columnMapping.id":6,"delta.columnMapping.physicalName":"col-6"}}
           ]}}},
          {"name":"attributes","nullable":true,
           "metadata":{"delta.columnMapping.id":7,"delta.columnMapping.physicalName":"col-7",
                       "delta.columnMapping.nested.ids":{"col-7.key":8,"col-7.value":9}},
           "type":{"type":"map","valueContainsNull":true,
             "keyType":"string","valueType":"string"}}
        ]}}
      ]}
      """;
}
