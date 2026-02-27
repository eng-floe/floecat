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

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import org.junit.jupiter.api.Test;

class LogicalSchemaMapperTest {

  private LogicalSchemaMapper mapper = new LogicalSchemaMapper();

  private Table baseTable(String schemaJson, UpstreamRef upstream) {
    return Table.newBuilder()
        .setResourceId(ResourceId.newBuilder().setId("tbl-1").setKind(ResourceKind.RK_TABLE))
        .setDisplayName("tbl")
        .setCatalogId(ResourceId.newBuilder().setId("cat").setKind(ResourceKind.RK_CATALOG))
        .setNamespaceId(ResourceId.newBuilder().setId("ns").setKind(ResourceKind.RK_NAMESPACE))
        .setSchemaJson(schemaJson)
        .setUpstream(upstream)
        .build();
  }

  // -------------------------------------------------------------------------
  // GENERIC schema
  // -------------------------------------------------------------------------
  @Test
  void genericSchemaParsesSimpleCols() {
    String json =
        """
            {"cols":[
                {"name":"id","type":"int"},
                {"name":"name","type":"string"}
            ]}
        """;

    UpstreamRef upstream = UpstreamRef.newBuilder().setFormat(TableFormat.TF_UNSPECIFIED).build();

    Table t = baseTable(json, upstream);

    SchemaDescriptor desc = mapper.map(t, json);

    assertEquals(2, desc.getColumnsCount());

    SchemaColumn c0 = desc.getColumns(0);
    assertEquals("id", c0.getName());
    assertEquals("INT", c0.getLogicalType());
    assertEquals("id", c0.getPhysicalPath());
    assertTrue(c0.getLeaf());
    assertEquals(1, c0.getOrdinal());
    assertNotEquals(0L, c0.getId());

    SchemaColumn c1 = desc.getColumns(1);
    assertEquals("name", c1.getName());
    assertEquals("STRING", c1.getLogicalType());
    assertTrue(c1.getLeaf());
    assertEquals(2, c1.getOrdinal());
    assertNotEquals(0L, c1.getId());
    assertNotEquals(c0.getId(), c1.getId());
  }

  @Test
  void genericDecimalPrecisionAbove38IsAllowed() {
    String json =
        """
            {"cols":[
                {"name":"x","type":"DECIMAL(39,0)"}
            ]}
        """;

    UpstreamRef upstream = UpstreamRef.newBuilder().setFormat(TableFormat.TF_UNSPECIFIED).build();
    Table t = baseTable(json, upstream);

    SchemaDescriptor desc = mapper.map(t, json);
    assertEquals(1, desc.getColumnsCount());
    assertEquals("DECIMAL(39,0)", desc.getColumns(0).getLogicalType());
  }

  // -------------------------------------------------------------------------
  // ICEBERG schema
  // -------------------------------------------------------------------------
  @Test
  void icebergSchemaParsesNestedStructs() {
    String icebergJson =
        """
        {
          "type":"struct",
          "fields":[
            {"id":1, "name":"id", "required":true, "type":"int"},
            {"id":2, "name":"info", "required":false,
             "type":{
                "type":"struct",
                "fields":[
                  {"id":3,"name":"city","required":false,"type":"string"}
                ]
             }}
          ]
        }
        """;

    UpstreamRef upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_ICEBERG)
            .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_FIELD_ID)
            .build();

    Table t = baseTable(icebergJson, upstream);

    SchemaDescriptor desc = mapper.map(t, icebergJson);

    assertEquals(3, desc.getColumnsCount()); // id, info, info.city

    SchemaColumn id = desc.getColumns(0);
    assertEquals("id", id.getName());
    assertEquals("id", id.getPhysicalPath());
    assertEquals(1, id.getFieldId());
    assertTrue(id.getLeaf());
    assertEquals(1, id.getOrdinal());
    assertEquals(1L, id.getId(), "CID_FIELD_ID policy should use field_id directly");

    SchemaColumn info = desc.getColumns(1);
    assertEquals("info", info.getName());
    assertEquals("info", info.getPhysicalPath());
    assertEquals(2, info.getFieldId());
    assertFalse(info.getLeaf());
    assertEquals(2, info.getOrdinal());
    assertEquals(2L, info.getId());

    SchemaColumn city = desc.getColumns(2);
    assertEquals("city", city.getName());
    assertEquals("info.city", city.getPhysicalPath());
    assertEquals(3, city.getFieldId());
    assertTrue(city.getLeaf());
    assertEquals(1, city.getOrdinal());
    assertEquals(3L, city.getId());
  }

  // -------------------------------------------------------------------------
  // DELTA schema
  // -------------------------------------------------------------------------
  @Test
  void deltaSchemaParsesStructs() {
    String deltaJson =
        """
        {
          "fields":[
            {"name":"id","type":"long","nullable":false},
            {"name":"location","type":{
                 "type":"struct",
                 "fields":[
                   {"name":"lat","type":"double"},
                   {"name":"lon","type":"double"}
                 ]
              }}
          ]
        }
        """;

    UpstreamRef upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_DELTA)
            .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_PATH_ORDINAL)
            .build();

    Table t = baseTable(deltaJson, upstream);

    SchemaDescriptor desc = mapper.map(t, deltaJson);

    // All columns should have deterministic, non-zero ids for CID_PATH_ORDINAL
    for (var c : desc.getColumnsList()) {
      assertNotEquals(0L, c.getId(), "expected non-zero id for " + c.getPhysicalPath());
    }

    assertEquals(4, desc.getColumnsCount()); // id, location, location.lat, location.lon

    SchemaColumn id = desc.getColumns(0);
    assertEquals("id", id.getName());
    assertEquals(0, id.getFieldId());
    assertTrue(id.getLeaf());
    assertEquals(1, id.getOrdinal());
    assertEquals("id", id.getPhysicalPath());

    SchemaColumn location = desc.getColumns(1);
    assertEquals("location", location.getName());
    assertEquals("location", location.getPhysicalPath());
    assertFalse(location.getLeaf());
    assertEquals(2, location.getOrdinal());

    SchemaColumn lat = desc.getColumns(2);
    assertEquals("lat", lat.getName());
    assertEquals("location.lat", lat.getPhysicalPath());
    assertEquals(0, lat.getFieldId());
    assertEquals(1, lat.getOrdinal());
    assertTrue(lat.getLeaf());

    SchemaColumn lon = desc.getColumns(3);
    assertEquals("lon", lon.getName());
    assertEquals("location.lon", lon.getPhysicalPath());
    assertEquals(0, lon.getFieldId());
    assertEquals(2, lon.getOrdinal());
    assertTrue(lon.getLeaf());

    assertNotEquals(lat.getId(), lon.getId());
  }

  // -------------------------------------------------------------------------
  // ICEBERG fallback to generic
  // -------------------------------------------------------------------------
  @Test
  void genericSchemaParsesWhenFormatUnspecified() {
    String genericJson =
        """
            {"cols":[
                {"name":"id","type":"int"},
                {"name":"name","type":"string"}
            ]}
        """;

    UpstreamRef upstream = UpstreamRef.newBuilder().setFormat(TableFormat.TF_UNSPECIFIED).build();

    Table t = baseTable(genericJson, upstream);

    SchemaDescriptor desc = mapper.map(t, genericJson);

    assertEquals(2, desc.getColumnsCount());

    SchemaColumn id = desc.getColumns(0);
    assertEquals("id", id.getName());
    assertEquals("INT", id.getLogicalType());
    assertTrue(id.getLeaf());
    assertEquals(1, id.getOrdinal());
    assertNotEquals(0L, id.getId());

    SchemaColumn name = desc.getColumns(1);
    assertEquals("name", name.getName());
    assertEquals("STRING", name.getLogicalType());
    assertTrue(name.getLeaf());
    assertEquals(2, name.getOrdinal());
    assertNotEquals(0L, name.getId());
    assertNotEquals(id.getId(), name.getId());
  }

  @Test
  void icebergSchemaCanonicalizesElementPathsForPathOrdinal() {
    // This test ensures that Iceberg list element paths (".element") are stable
    // when later canonicalized by ColumnIdComputer.
    String icebergJson =
        """
        {
          "type":"struct",
          "fields":[
            {"id":1, "name":"arr", "required":false,
             "type":{
               "type":"list",
               "element-id":2,
               "element-required":false,
               "element":{
                 "type":"struct",
                 "fields":[
                   {"id":3,"name":"x","required":false,"type":"int"}
                 ]
               }
             }
            }
          ]
        }
        """;

    UpstreamRef upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_ICEBERG)
            // Use PATH_ORDINAL here so the mapper computes ids based on physical path + ordinal.
            .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_PATH_ORDINAL)
            .build();

    Table t = baseTable(icebergJson, upstream);
    SchemaDescriptor desc = mapper.map(t, icebergJson);

    assertEquals(2, desc.getColumnsCount()); // arr, arr[].x (no explicit element container node)
    SchemaColumn arr = desc.getColumns(0);
    assertEquals("arr", arr.getPhysicalPath());
    assertFalse(arr.getLeaf());
    assertEquals(1, arr.getOrdinal());

    SchemaColumn x = desc.getColumns(1);

    // Depending on mapper style it may be "arr[].x" or "arr.element.x"
    String p = x.getPhysicalPath();
    assertTrue(p.equals("arr[].x") || p.equals("arr.element.x"), "unexpected path: " + p);

    assertTrue(x.getLeaf());
    assertEquals(1, x.getOrdinal());

    // Sanity: ids should be present under PATH_ORDINAL.
    assertNotEquals(0L, arr.getId());
    assertNotEquals(0L, x.getId());

    // Canonicalization should treat arr.element.x and arr[].x equivalently.
    long canonical =
        ColumnIdComputer.compute(ColumnIdAlgorithm.CID_PATH_ORDINAL, "x", "arr[].x", 1, 0);
    long fromMapper =
        ColumnIdComputer.compute(
            ColumnIdAlgorithm.CID_PATH_ORDINAL,
            x.getName(),
            x.getPhysicalPath(),
            x.getOrdinal(),
            0);
    assertEquals(canonical, fromMapper);
  }

  @Test
  void pathOrdinalIdsAreStableAcrossRecompute() {
    String json =
        """
        {"cols":[
            {"name":"id","type":"int"},
            {"name":"name","type":"string"}
        ]}
        """;

    UpstreamRef upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_UNSPECIFIED)
            .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_PATH_ORDINAL)
            .build();

    Table t = baseTable(json, upstream);

    SchemaDescriptor d1 = mapper.map(t, json);
    SchemaDescriptor d2 = mapper.map(t, json);

    assertEquals(d1.getColumnsCount(), d2.getColumnsCount());
    for (int i = 0; i < d1.getColumnsCount(); i++) {
      SchemaColumn a = d1.getColumns(i);
      SchemaColumn b = d2.getColumns(i);
      assertEquals(a.getPhysicalPath(), b.getPhysicalPath());
      assertEquals(a.getOrdinal(), b.getOrdinal());
      assertEquals(a.getId(), b.getId(), "id must be deterministic");
    }
  }

  @Test
  void genericSchemaFailsFastOnUnknownLogicalType() {
    String json =
        """
            {"cols":[
                {"name":"id","type":"not_a_real_type"}
            ]}
        """;

    UpstreamRef upstream = UpstreamRef.newBuilder().setFormat(TableFormat.TF_UNSPECIFIED).build();
    Table t = baseTable(json, upstream);

    IllegalArgumentException err =
        assertThrows(IllegalArgumentException.class, () -> mapper.map(t, json));
    assertTrue(err.getMessage().contains("Failed to parse generic schema JSON"));
  }

  @Test
  void genericSchemaFailsFastWhenColsMissing() {
    String json = "{\"wrong\":\"shape\"}";
    UpstreamRef upstream = UpstreamRef.newBuilder().setFormat(TableFormat.TF_UNSPECIFIED).build();
    Table t = baseTable(json, upstream);

    IllegalArgumentException err =
        assertThrows(IllegalArgumentException.class, () -> mapper.map(t, json));
    assertTrue(err.getMessage().contains("Failed to parse generic schema JSON"));
  }

  @Test
  void fieldIdPolicyDoesNotDependOnOrdinalOrPath() {
    // Under CID_FIELD_ID, only field_id contributes to column_id.
    SchemaColumn c1 =
        SchemaColumn.newBuilder()
            .setName("x")
            .setPhysicalPath("a.b")
            .setOrdinal(1)
            .setFieldId(42)
            .build();

    SchemaColumn c2 =
        SchemaColumn.newBuilder()
            .setName("x")
            .setPhysicalPath("totally.different")
            .setOrdinal(999)
            .setFieldId(42)
            .build();

    assertEquals(
        ColumnIdComputer.compute(ColumnIdAlgorithm.CID_FIELD_ID, c1),
        ColumnIdComputer.compute(ColumnIdAlgorithm.CID_FIELD_ID, c2));
    assertEquals(42L, ColumnIdComputer.compute(ColumnIdAlgorithm.CID_FIELD_ID, c1));
  }
}
