package ai.floedb.metacat.service.query.resolve;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.catalog.rpc.UpstreamRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.query.rpc.SchemaColumn;
import ai.floedb.metacat.query.rpc.SchemaDescriptor;
import org.junit.jupiter.api.Test;

class LogicalSchemaMapperTest {

  private LogicalSchemaMapper mapper = new LogicalSchemaMapper();

  private Table baseTable(String schemaJson, UpstreamRef upstream) {
    return Table.newBuilder()
        .setResourceId(ResourceId.newBuilder().setId("tbl-1"))
        .setDisplayName("tbl")
        .setCatalogId(ResourceId.newBuilder().setId("cat"))
        .setNamespaceId(ResourceId.newBuilder().setId("ns"))
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
    assertEquals("int", c0.getLogicalType());
    assertEquals("id", c0.getPhysicalPath());

    SchemaColumn c1 = desc.getColumns(1);
    assertEquals("name", c1.getName());
    assertEquals("string", c1.getLogicalType());
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

    UpstreamRef upstream = UpstreamRef.newBuilder().setFormat(TableFormat.TF_ICEBERG).build();

    Table t = baseTable(icebergJson, upstream);

    SchemaDescriptor desc = mapper.map(t, icebergJson);

    assertEquals(3, desc.getColumnsCount()); // id, info, info.city

    SchemaColumn id = desc.getColumns(0);
    assertEquals("id", id.getName());

    SchemaColumn info = desc.getColumns(1);
    assertEquals("info", info.getName());
    assertEquals("info", info.getPhysicalPath());

    SchemaColumn city = desc.getColumns(2);
    assertEquals("city", city.getName());
    assertEquals("info.city", city.getPhysicalPath());
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
            .putFieldIdByPath("id", 10)
            .putFieldIdByPath("location.lat", 11)
            .putFieldIdByPath("location.lon", 12)
            .build();

    Table t = baseTable(deltaJson, upstream);

    SchemaDescriptor desc = mapper.map(t, deltaJson);

    assertEquals(4, desc.getColumnsCount()); // id, location, location.lat, location.lon

    SchemaColumn id = desc.getColumns(0);
    assertEquals("id", id.getName());
    assertEquals(10, id.getFieldId());

    SchemaColumn location = desc.getColumns(1);
    assertEquals("location", location.getName());
    assertEquals("location", location.getPhysicalPath());

    SchemaColumn lat = desc.getColumns(2);
    assertEquals("lat", lat.getName());
    assertEquals("location.lat", lat.getPhysicalPath());
    assertEquals(11, lat.getFieldId());

    SchemaColumn lon = desc.getColumns(3);
    assertEquals("lon", lon.getName());
    assertEquals("location.lon", lon.getPhysicalPath());
    assertEquals(12, lon.getFieldId());
  }

  // -------------------------------------------------------------------------
  // ICEBERG fallback to generic
  // -------------------------------------------------------------------------
  @Test
  void icebergInvalidSchemaFallsBackToGeneric() {
    String badJson = "{not-valid-json}";

    UpstreamRef upstream = UpstreamRef.newBuilder().setFormat(TableFormat.TF_ICEBERG).build();

    Table t = baseTable(badJson, upstream);

    SchemaDescriptor desc = mapper.map(t, badJson);

    // generic fallback → empty schema
    assertEquals(0, desc.getColumnsCount());
  }
}
