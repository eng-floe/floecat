package ai.floedb.floecat.gateway.iceberg.rest;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@QuarkusTestResource(value = RealServiceTestResource.class, restrictToAnnotatedClass = true)
class IcebergRestIT {

  private static final String DEFAULT_ACCOUNT = "31a47986-efaf-35f5-b810-09ba18ca81d2";
  private static final String EMPTY_SCHEMA_JSON = "{\"type\":\"struct\",\"fields\":[]}";

  private RequestSpecification spec;

  @BeforeEach
  void setUp() {
    spec =
        new RequestSpecBuilder()
            .addHeader("x-tenant-id", DEFAULT_ACCOUNT)
            .addHeader("authorization", "Bearer integration-test")
            .setContentType(ContentType.JSON)
            .build();
  }

  @Test
  void listsNamespacesFromRealService() {
    given()
        .spec(spec)
        .when()
        .get("/v1/sales/namespaces")
        .then()
        .statusCode(200)
        .body("namespaces*.get(0)", hasItem("core"));
  }

  @Test
  void createsAndDeletesNamespaceViaGateway() {
    String ns = "it_ns_" + UUID.randomUUID().toString().replace("-", "");
    Map<String, Object> payload = Map.of("namespace", ns, "description", "Integration test");

    given()
        .spec(spec)
        .body(payload)
        .when()
        .post("/v1/sales/namespaces")
        .then()
        .statusCode(201)
        .body("namespace[0]", equalTo(ns));

    given()
        .spec(spec)
        .when()
        .get("/v1/sales/namespaces/" + ns)
        .then()
        .statusCode(200)
        .body("namespace[0]", equalTo(ns));

    given()
        .spec(spec)
        .when()
        .delete("/v1/sales/namespaces/" + ns + "?requireEmpty=false")
        .then()
        .statusCode(204);
  }

  @Test
  void listsTablesInSeededNamespace() {
    given()
        .spec(spec)
        .when()
        .get("/v1/sales/namespaces/core/tables")
        .then()
        .statusCode(200)
        .body("identifiers.name", hasItems("orders", "lineitem"));
  }

  @Test
  void loadsTableMetadataFromRealService() {
    given()
        .spec(spec)
        .when()
        .get("/v1/sales/namespaces/core/tables/orders")
        .then()
        .statusCode(200)
        .body("metadata.location", equalTo("s3://seed-data/"))
        .body("metadata.'table-uuid'", notNullValue())
        .body("metadata.'format-version'", equalTo(2));
  }

  @Test
  void createsTableAndCommitsPropertiesViaGateway() {
    String namespace = uniqueName("it_ns_");
    String table = uniqueName("it_tbl_");
    createNamespace(namespace);
    try {
      Map<String, Object> createPayload = Map.of("name", table, "schemaJson", EMPTY_SCHEMA_JSON);

      given()
          .spec(spec)
          .body(createPayload)
          .when()
          .post("/v1/sales/namespaces/" + namespace + "/tables")
          .then()
          .statusCode(200)
          .body("metadata.'table-uuid'", notNullValue());

      Map<String, Object> commitPayload = Map.of("properties", Map.of("owner", "integration"));

      given()
          .spec(spec)
          .body(commitPayload)
          .when()
          .post("/v1/sales/namespaces/" + namespace + "/tables/" + table)
          .then()
          .statusCode(200)
          .body("metadata.properties.owner", equalTo("integration"));

      given()
          .spec(spec)
          .when()
          .get("/v1/sales/namespaces/" + namespace + "/tables/" + table)
          .then()
          .statusCode(200)
          .body("metadata.properties.owner", equalTo("integration"));
    } finally {
      deleteTable(namespace, table);
      deleteNamespace(namespace);
    }
  }

  @Test
  void createsAndReadsViewViaGateway() {
    String namespace = uniqueName("it_ns_");
    String view = uniqueName("it_view_");
    createNamespace(namespace);
    try {
      Map<String, Object> createPayload = createViewPayload(namespace, view);

      given()
          .spec(spec)
          .body(createPayload)
          .when()
          .post("/v1/sales/namespaces/" + namespace + "/views")
          .then()
          .statusCode(200)
          .body(
              "metadata.location",
              equalTo("floecat://views/" + namespace + "/" + view + "/metadata.json"));

      given()
          .spec(spec)
          .when()
          .get("/v1/sales/namespaces/" + namespace + "/views/" + view)
          .then()
          .statusCode(200)
          .body("metadata.versions[0].representations[0].sql", equalTo("SELECT 1"))
          .body("metadata.properties.dialect", equalTo("spark"));
    } finally {
      deleteView(namespace, view);
      deleteNamespace(namespace);
    }
  }

  private static String uniqueName(String prefix) {
    return prefix + UUID.randomUUID().toString().replace("-", "");
  }

  private void createNamespace(String namespace) {
    Map<String, Object> payload =
        Map.of("namespace", namespace, "description", "Integration test namespace");
    given()
        .spec(spec)
        .body(payload)
        .when()
        .post("/v1/sales/namespaces")
        .then()
        .statusCode(201)
        .body("namespace[0]", equalTo(namespace));
  }

  private void deleteNamespace(String namespace) {
    given()
        .spec(spec)
        .when()
        .delete("/v1/sales/namespaces/" + namespace + "?requireEmpty=false")
        .then()
        .statusCode(anyOf(is(204), is(404)));
  }

  private void deleteTable(String namespace, String table) {
    given()
        .spec(spec)
        .when()
        .delete("/v1/sales/namespaces/" + namespace + "/tables/" + table)
        .then()
        .statusCode(anyOf(is(204), is(404)));
  }

  private void deleteView(String namespace, String view) {
    given()
        .spec(spec)
        .when()
        .delete("/v1/sales/namespaces/" + namespace + "/views/" + view)
        .then()
        .statusCode(anyOf(is(204), is(404)));
  }

  private Map<String, Object> createViewPayload(String namespace, String view) {
    Map<String, Object> schema =
        Map.of(
            "schema-id",
            1,
            "type",
            "struct",
            "fields",
            List.of(Map.of("id", 1, "name", "dummy", "required", true, "type", "string")));
    Map<String, Object> representation =
        Map.of("type", "sql", "sql", "SELECT 1", "dialect", "ansi");
    Map<String, Object> version = new java.util.LinkedHashMap<>();
    version.put("version-id", 1);
    version.put("schema-id", 1);
    version.put("summary", Map.of("operation", "create"));
    version.put("representations", List.of(representation));
    version.put("default-namespace", List.of(namespace));

    Map<String, Object> payload = new java.util.LinkedHashMap<>();
    payload.put("name", view);
    payload.put("schema", schema);
    payload.put("view-version", version);
    payload.put("properties", Map.of("dialect", "spark"));
    return payload;
  }
}
