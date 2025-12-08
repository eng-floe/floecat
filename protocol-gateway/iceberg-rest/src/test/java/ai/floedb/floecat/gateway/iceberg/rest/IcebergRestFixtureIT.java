package ai.floedb.floecat.gateway.iceberg.rest;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import ai.floedb.floecat.gateway.iceberg.rest.support.TestS3Fixtures;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@QuarkusTestResource(value = RealServiceTestResource.class, restrictToAnnotatedClass = true)
class IcebergRestFixtureIT {

  private static final String DEFAULT_ACCOUNT = "5eaa9cd5-7d08-3750-9457-cfe800b0b9d2";
  private static final String NAMESPACE_PREFIX = "fixture_ns_";
  private static final String TABLE_PREFIX = "fixture_tbl_";
  private static final String CATALOG = "sales";
  private static final String METADATA_V1 =
      "metadata/00000-16393a9a-3433-440c-98f4-fe023ed03973.metadata.json";
  private static final String METADATA_V3 =
      "metadata/00002-503f4508-3824-4cb6-bdf1-4bd6bf5a0ade.metadata.json";

  private static long expectedSnapshotId;

  private RequestSpecification spec;

  @BeforeAll
  static void initFixtures() throws IOException {
    TestS3Fixtures.seedFixtures();
    expectedSnapshotId = parseSnapshotId(METADATA_V3);
  }

  private static long parseSnapshotId(String relativeMetadataPath) throws IOException {
    Path metadataPath = TestS3Fixtures.bucketPath().resolve(Path.of(relativeMetadataPath));
    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.readTree(metadataPath.toFile());
    return node.path("current-snapshot-id").asLong();
  }

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
  void registersAndOverwritesTableFromFixtureMetadata() {
    String namespace = NAMESPACE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String table = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");

    given()
        .spec(spec)
        .body(Map.of("namespace", namespace, "description", "Fixture namespace"))
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(201);

    registerTable(namespace, table, METADATA_V1, false);
    verifyMetadataLocation(namespace, table, METADATA_V1);

    registerTable(namespace, table, METADATA_V3, true);
    verifyMetadataLocation(namespace, table, METADATA_V3);

    given()
        .spec(spec)
        .when()
        .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table + "/snapshots")
        .then()
        .statusCode(200)
        .body("entries", hasSize(3))
        .body("entries[2].snapshot-id", equalTo(expectedSnapshotId));
  }

  private void registerTable(
      String namespace, String table, String metadataLocation, boolean overwrite) {
    Map<String, Object> payload =
        Map.of(
            "name",
            table,
            "metadata-location",
            TestS3Fixtures.bucketUri(metadataLocation),
            "overwrite",
            overwrite,
            "properties",
            Map.of(
                "io-impl",
                "ai.floedb.floecat.gateway.iceberg.rest.support.io.InMemoryS3FileIO",
                "fs.floecat.test-root",
                TestS3Fixtures.bucketPath().getParent().toString()));
    System.out.printf(
        "FixtureIT register namespace=%s table=%s metadata=%s overwrite=%s%n",
        namespace, table, metadataLocation, overwrite);
    String responseLocation =
        given()
            .spec(spec)
            .body(payload)
            .when()
            .post("/v1/" + CATALOG + "/namespaces/" + namespace + "/register")
            .then()
            .statusCode(200)
            .extract()
            .path("'metadata-location'");
    System.out.printf(
        "FixtureIT register response namespace=%s table=%s metadataLocation=%s%n",
        namespace, table, responseLocation);
  }

  private void verifyMetadataLocation(String namespace, String table, String relativePath) {
    String actual = fetchMetadataLocation(namespace, table);
    System.out.printf(
        "FixtureIT metadata-location namespace=%s table=%s actual=%s expected=%s%n",
        namespace, table, actual, TestS3Fixtures.bucketUri(relativePath));
    Assertions.assertEquals(TestS3Fixtures.bucketUri(relativePath), actual);
  }

  private String fetchMetadataLocation(String namespace, String table) {
    return given()
        .spec(spec)
        .when()
        .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
        .then()
        .statusCode(200)
        .extract()
        .path("'metadata-location'");
  }
}
