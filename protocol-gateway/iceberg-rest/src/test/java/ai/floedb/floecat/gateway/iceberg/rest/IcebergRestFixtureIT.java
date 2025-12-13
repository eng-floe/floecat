package ai.floedb.floecat.gateway.iceberg.rest;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.connector.rpc.ListConnectorsRequest;
import ai.floedb.floecat.connector.rpc.ListConnectorsResponse;
import ai.floedb.floecat.connector.rpc.SyncCaptureRequest;
import ai.floedb.floecat.gateway.iceberg.rest.common.InMemoryS3FileIO;
import ai.floedb.floecat.gateway.iceberg.rest.common.RealServiceTestResource;
import ai.floedb.floecat.gateway.iceberg.rest.common.TestS3Fixtures;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@QuarkusTestResource(value = RealServiceTestResource.class, restrictToAnnotatedClass = true)
class IcebergRestFixtureIT {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String DEFAULT_ACCOUNT = "5eaa9cd5-7d08-3750-9457-cfe800b0b9d2";
  private static final String NAMESPACE_PREFIX = "fixture_ns_";
  private static final String TABLE_PREFIX = "fixture_tbl_";
  private static final String CATALOG = "sales";
  private static final String FIXTURE_METADATA_PREFIX = TestS3Fixtures.bucketUri("metadata/");
  private static final String METADATA_V1 =
      "metadata/00000-16393a9a-3433-440c-98f4-fe023ed03973.metadata.json";
  private static final String METADATA_V3 =
      "metadata/00002-503f4508-3824-4cb6-bdf1-4bd6bf5a0ade.metadata.json";
  private static final Path TEST_S3_ROOT = TestS3Fixtures.bucketPath().getParent();
  private static final String STAGE_BUCKET = "staged-fixtures";
  private static final String DEFAULT_AUTH_HEADER = "Bearer integration-test";
  private static final String ACCOUNT_HEADER_NAME = "x-tenant-id";
  private static final String AUTH_HEADER_NAME = "authorization";

  private static long expectedSnapshotId;
  private static List<Long> fixtureSnapshotIds;
  private static Map<Long, String> fixtureManifestLists = new java.util.HashMap<>();
  private static String upstreamHost;
  private static int upstreamPort;
  private static int serviceGrpcPort;
  private static boolean connectorIntegrationEnabled;

  private RequestSpecification spec;

  @BeforeAll
  static void initFixtures() throws IOException {
    TestS3Fixtures.seedFixturesOnce();
    fixtureManifestLists.clear();
    fixtureSnapshotIds = parseSnapshotIds(METADATA_V3);
    expectedSnapshotId =
        fixtureSnapshotIds.isEmpty() ? -1L : fixtureSnapshotIds.get(fixtureSnapshotIds.size() - 1);
    parseUpstreamTarget();
    connectorIntegrationEnabled = parseConnectorIntegration();
  }

  @Test
  void snapshotCommitUpdatesMetadata() {
    String namespace = NAMESPACE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String table = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");

    TestS3Fixtures.seedStageTable(namespace, table);

    given()
        .spec(spec)
        .body(Map.of("namespace", namespace, "description", "Snapshot namespace"))
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(201);

    registerTable(namespace, table, METADATA_V3, false);

    Assertions.assertFalse(fixtureSnapshotIds.isEmpty(), "Fixture metadata must contain snapshots");
    List<Long> initialSnapshots = fetchSnapshotIds(namespace, table);
    Assertions.assertTrue(
        initialSnapshots.containsAll(fixtureSnapshotIds),
        "Expected imported snapshots to be present");

    long removedSnapshotId = fixtureSnapshotIds.isEmpty() ? 0L : fixtureSnapshotIds.get(0);
    long parentSnapshotId =
        fixtureSnapshotIds.isEmpty() ? 0L : fixtureSnapshotIds.get(fixtureSnapshotIds.size() - 1);
    long newSnapshotId = System.currentTimeMillis();

    String anyFixtureManifestRel = fixtureManifestLists.values().stream().findFirst().orElseThrow();
    String manifestList = TestS3Fixtures.stageTableUri(namespace, table, anyFixtureManifestRel);

    Map<String, Object> addSnapshotUpdate =
        Map.of(
            "action",
            "add-snapshot",
            "snapshot",
            Map.ofEntries(
                Map.entry("snapshot-id", newSnapshotId),
                Map.entry("timestamp-ms", System.currentTimeMillis()),
                Map.entry("parent-snapshot-id", parentSnapshotId),
                Map.entry("sequence-number", System.currentTimeMillis()),
                Map.entry("manifest-list", manifestList),
                Map.entry("summary", Map.of("operation", "append"))));
    Map<String, Object> removeSnapshotUpdate =
        Map.of("action", "remove-snapshots", "snapshot-ids", List.of(removedSnapshotId));
    Map<String, Object> setRefUpdate =
        Map.of(
            "action",
            "set-snapshot-ref",
            "ref-name",
            "branch-test",
            "type",
            "branch",
            "snapshot-id",
            newSnapshotId,
            "max-ref-age-ms",
            60000);
    Map<String, Object> commitPayload =
        Map.of("updates", List.of(addSnapshotUpdate, removeSnapshotUpdate, setRefUpdate));

    given()
        .spec(spec)
        .body(commitPayload)
        .when()
        .post("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
        .then()
        .statusCode(200);

    List<Long> updatedSnapshots = fetchSnapshotIds(namespace, table);
    Assertions.assertTrue(
        updatedSnapshots.contains(newSnapshotId), "New snapshot should be present after commit");
    Assertions.assertFalse(
        updatedSnapshots.contains(removedSnapshotId),
        "Removed snapshot should no longer be listed");
    Assertions.assertEquals(
        initialSnapshots.size(),
        updatedSnapshots.size(),
        "Snapshot count should remain constant after add/remove");

    var tableJson =
        given()
            .spec(spec)
            .when()
            .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
            .then()
            .statusCode(200)
            .extract()
            .jsonPath();
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> refs = tableJson.getMap("metadata.refs");
    Assertions.assertNotNull(refs, "Refs map should be present in metadata");
    Map<String, Object> branchRef = refs.get("branch-test");
    Assertions.assertNotNull(branchRef, "branch-test ref should be created");
    Object snapshotValue = branchRef.get("snapshot-id");
    Assertions.assertEquals(
        newSnapshotId,
        snapshotValue instanceof Number ? ((Number) snapshotValue).longValue() : snapshotValue);
  }

  @Test
  void transactionCommitUpdatesConnectorMetadata() {
    Assumptions.assumeTrue(
        connectorIntegrationEnabled, "Connector integration disabled for this test profile");
    String namespace = NAMESPACE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String table = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    TestS3Fixtures.seedStageTable(namespace, table);

    given()
        .spec(spec)
        .body(Map.of("namespace", namespace, "description", "Connector namespace"))
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(201);

    String stageId = "stage-" + UUID.randomUUID();
    Map<String, Object> stageRequest = stageCreateRequest(table, namespace);
    given()
        .spec(spec)
        .header("Iceberg-Transaction-Id", stageId)
        .body(stageRequest)
        .when()
        .post("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables")
        .then()
        .statusCode(200)
        .body("stage-id", equalTo(stageId));

    given()
        .spec(spec)
        .body(
            Map.of(
                "table-changes",
                List.of(
                    Map.of(
                        "identifier",
                        Map.of("namespace", List.of(namespace), "name", table),
                        "stage-id",
                        stageId,
                        "updates",
                        List.of(
                            Map.of(
                                "action",
                                "set-properties",
                                "updates",
                                Map.of(
                                    "metadata-location",
                                    fixtureMetadataTarget(namespace, table))))))))
        .when()
        .post("/v1/" + CATALOG + "/transactions/commit")
        .then()
        .statusCode(204);

    String commitMetadataLocation = ensurePromotedMetadata(fetchMetadataLocation(namespace, table));
    Assertions.assertNotNull(commitMetadataLocation, "commit should materialize metadata");

    Connector connector = awaitConnectorForTable(namespace, table, Duration.ofSeconds(10));
    Assertions.assertNotNull(connector, "Connector should be created for registered table");

    withConnectorsClient(
        stub -> {
          stub.syncCapture(
              SyncCaptureRequest.newBuilder().setConnectorId(connector.getResourceId()).build());
          return null;
        });

    Connector refreshed =
        withConnectorsClient(
            stub ->
                stub.getConnector(
                        ai.floedb.floecat.connector.rpc.GetConnectorRequest.newBuilder()
                            .setConnectorId(connector.getResourceId())
                            .build())
                    .getConnector());

    String tableMetadataLocation = ensurePromotedMetadata(fetchMetadataLocation(namespace, table));
    Assertions.assertTrue(
        tableMetadataLocation.startsWith(FIXTURE_METADATA_PREFIX),
        () ->
            "persisted metadata should reside under the floecat fixture bucket: "
                + tableMetadataLocation);
    Assertions.assertTrue(
        Files.exists(localS3Path(tableMetadataLocation)),
        "persisted metadata file should exist on the fake S3 filesystem");
    Assertions.assertEquals(
        tableMetadataLocation,
        refreshed.getPropertiesMap().get("external.metadata-location"),
        "Connector external metadata location should match the persisted metadata");
  }

  @Test
  void deleteTablePurgesMetadataWhenRequested() {
    try {
      String namespace = NAMESPACE_PREFIX + UUID.randomUUID().toString().replace("-", "");
      given()
          .spec(spec)
          .body(Map.of("namespace", namespace, "description", "Purge namespace"))
          .when()
          .post("/v1/" + CATALOG + "/namespaces")
          .then()
          .statusCode(201);

      String keepTable = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
      registerTable(namespace, keepTable, METADATA_V1, false);
      given()
          .spec(spec)
          .when()
          .delete("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + keepTable)
          .then()
          .statusCode(204);

      given()
          .spec(spec)
          .when()
          .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + keepTable)
          .then()
          .statusCode(404);

      String purgeTable = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
      registerTable(namespace, purgeTable, METADATA_V3, false);
      Path metadataPath = localS3Path(TestS3Fixtures.bucketUri(METADATA_V3));
      Assertions.assertTrue(
          Files.exists(metadataPath),
          "Fixture metadata should exist before issuing purge delete request");

      given()
          .spec(spec)
          .when()
          .delete(
              "/v1/"
                  + CATALOG
                  + "/namespaces/"
                  + namespace
                  + "/tables/"
                  + purgeTable
                  + "?purgeRequested=true")
          .then()
          .statusCode(204);

      given()
          .spec(spec)
          .when()
          .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + purgeTable)
          .then()
          .statusCode(404);
      Assertions.assertTrue(
          Files.notExists(metadataPath),
          "metadata file should be deleted when purgeRequested=true");
      fixtureManifestLists
          .values()
          .forEach(
              relative -> {
                Path manifestPath = localS3Path(TestS3Fixtures.bucketUri(relative));
                Assertions.assertTrue(
                    Files.notExists(manifestPath),
                    () ->
                        "manifest file should be deleted when purgeRequested=true: "
                            + manifestPath);
              });
    } finally {
      reseedFixtureBucket();
    }
  }

  @Test
  void registerPersistsFullMetadata() throws IOException {
    String namespace = NAMESPACE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String table = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    given()
        .spec(spec)
        .body(Map.of("namespace", namespace, "description", "Metadata namespace"))
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(201);

    registerTable(namespace, table, METADATA_V3, false);

    JsonNode actualMetadata = fetchPersistedMetadata(namespace, table);
    JsonNode expectedMetadata =
        MAPPER.readTree(TestS3Fixtures.prefixPath().resolve(Path.of(METADATA_V3)).toFile());

    Assertions.assertEquals(
        canonicalizeMetadata(expectedMetadata, TestS3Fixtures.bucketUri(METADATA_V3)),
        canonicalizeMetadata(actualMetadata, fetchMetadataLocation(namespace, table)),
        "Persisted Iceberg metadata should match original fixture contents");
  }

  private static List<Long> parseSnapshotIds(String relativeMetadataPath) throws IOException {
    Path metadataPath = TestS3Fixtures.prefixPath().resolve(Path.of(relativeMetadataPath));
    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.readTree(metadataPath.toFile());
    List<Long> ids = new java.util.ArrayList<>();
    if (node.has("snapshots")) {
      for (JsonNode snapshotNode : node.withArray("snapshots")) {
        long snapshotId = snapshotNode.path("snapshot-id").asLong();
        ids.add(snapshotId);
        String manifestList = snapshotNode.path("manifest-list").asText(null);
        if (manifestList != null && !manifestList.isBlank()) {
          fixtureManifestLists.putIfAbsent(snapshotId, relativeMetadataPath(manifestList));
        }
      }
    }
    long current = node.path("current-snapshot-id").asLong();
    if (!ids.contains(current)) {
      ids.add(current);
    }
    return List.copyOf(ids);
  }

  private static String relativeMetadataPath(String location) {
    if (location == null || location.isBlank()) {
      return location;
    }
    int idx = location.indexOf("/metadata/");
    if (idx >= 0 && idx + 1 < location.length()) {
      return location.substring(idx + 1);
    }
    return location;
  }

  private static void parseUpstreamTarget() {
    String target =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.gateway.upstream-target", String.class)
            .orElseGet(
                () ->
                    Optional.ofNullable(System.getProperty("floecat.gateway.upstream-target"))
                        .orElseGet(
                            () ->
                                Optional.ofNullable(
                                        System.getenv("FLOECAT_GATEWAY_UPSTREAM_TARGET"))
                                    .orElse("localhost:9100")));
    String[] parts = target.split(":");
    upstreamHost = parts[0];
    upstreamPort = parts.length > 1 ? Integer.parseInt(parts[1]) : 9100;
    serviceGrpcPort =
        Optional.ofNullable(System.getProperty("floecat.test.upstream-grpc-port"))
            .map(Integer::parseInt)
            .orElse(upstreamPort);
  }

  private static boolean parseConnectorIntegration() {
    return ConfigProvider.getConfig()
        .getOptionalValue("floecat.gateway.connector-integration-enabled", Boolean.class)
        .orElseGet(
            () -> {
              String raw = System.getProperty("floecat.gateway.connector-integration-enabled");
              if (raw == null || raw.isBlank()) {
                raw = System.getenv("FLOECAT_GATEWAY_CONNECTOR_INTEGRATION_ENABLED");
              }
              return raw != null && !raw.isBlank() && Boolean.parseBoolean(raw);
            });
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
  void stageCommitMaterializesMetadata() {
    String namespace = NAMESPACE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String table = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    TestS3Fixtures.seedStageTable(namespace, table);

    given()
        .spec(spec)
        .body(Map.of("namespace", namespace, "description", "Staged namespace"))
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(201);

    String stageId = "stage-" + UUID.randomUUID();
    String tableLocation = "s3://" + STAGE_BUCKET + "/" + namespace + "/" + table;
    String stageMetadataLocation = TestS3Fixtures.stageTableUri(namespace, table, METADATA_V3);

    Map<String, Object> stageRequest =
        Map.of(
            "name",
            table,
            "schema",
            Map.of(
                "schema-id",
                1,
                "last-column-id",
                1,
                "type",
                "struct",
                "fields",
                List.of(Map.of("id", 1, "name", "id", "required", true, "type", "int"))),
            "partition-spec",
            Map.of(
                "spec-id",
                0,
                "fields",
                List.of(
                    Map.of("name", "id", "field-id", 1, "source-id", 1, "transform", "identity"))),
            "write-order",
            Map.of("order-id", 0, "fields", List.of(Map.of("source-id", 1))),
            "properties",
            Map.ofEntries(
                Map.entry("io-impl", InMemoryS3FileIO.class.getName()),
                Map.entry("fs.floecat.test-root", TEST_S3_ROOT.toAbsolutePath().toString()),
                Map.entry("metadata-location", stageMetadataLocation)),
            "location",
            tableLocation,
            "stage-create",
            true);

    given()
        .spec(spec)
        .header("Iceberg-Transaction-Id", stageId)
        .body(stageRequest)
        .when()
        .post("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables")
        .then()
        .statusCode(200)
        .body("stage-id", equalTo(stageId));

    Map<String, Object> commitRequest =
        Map.of(
            "stage-id",
            stageId,
            "requirements",
            List.of(Map.of("type", "assert-create")),
            "properties",
            Map.of("metadata-location", fixtureMetadataTarget(namespace, table)),
            "updates",
            List.of(
                Map.of(
                    "action",
                    "set-properties",
                    "updates",
                    Map.of("metadata-location", fixtureMetadataTarget(namespace, table)))));

    String commitMetadataLocation =
        given()
            .spec(spec)
            .body(commitRequest)
            .when()
            .post("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
            .then()
            .statusCode(200)
            .extract()
            .path("'metadata-location'");

    Assertions.assertNotNull(commitMetadataLocation, "metadata-location should be populated");
    Assertions.assertTrue(
        commitMetadataLocation.startsWith(FIXTURE_METADATA_PREFIX),
        () ->
            "metadata-location should reside under the floecat fixture bucket: "
                + commitMetadataLocation);
    String fileName = commitMetadataLocation.substring(commitMetadataLocation.lastIndexOf('/') + 1);
    Assertions.assertTrue(
        fileName.contains("-"), "materialized metadata file should include a version prefix");

    String persistedLocation = ensurePromotedMetadata(fetchMetadataLocation(namespace, table));
    Assertions.assertTrue(
        persistedLocation.startsWith(FIXTURE_METADATA_PREFIX),
        () ->
            "persisted metadata should be stored under the floecat fixture bucket: "
                + persistedLocation);
    Assertions.assertTrue(
        persistedLocation.endsWith(".metadata.json"),
        "persisted metadata should be an Iceberg metadata file");

    Path commitPath = localS3Path(commitMetadataLocation);
    Assertions.assertTrue(
        Files.exists(commitPath),
        "materialized metadata file should exist on the fake S3 filesystem");
    Path persistedPath = localS3Path(persistedLocation);
    Assertions.assertTrue(
        Files.exists(persistedPath),
        "persisted metadata file should exist on the fake S3 filesystem");
  }

  private String registerTable(
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
                InMemoryS3FileIO.class.getName(),
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
    return responseLocation;
  }

  private String fixtureMetadataTarget(String namespace, String table) {
    return TestS3Fixtures.bucketUri(
        String.format("metadata/%s/%s/metadata.json", namespace, table));
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

  private List<Long> fetchSnapshotIds(String namespace, String table) {
    return given()
        .spec(spec)
        .when()
        .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table + "?snapshots=all")
        .then()
        .statusCode(200)
        .extract()
        .jsonPath()
        .getList("metadata.snapshots.'snapshot-id'", Long.class);
  }

  private JsonNode fetchPersistedMetadata(String namespace, String table) throws IOException {
    String body =
        given()
            .spec(spec)
            .when()
            .get(
                "/v1/"
                    + CATALOG
                    + "/namespaces/"
                    + namespace
                    + "/tables/"
                    + table
                    + "?snapshots=all")
            .then()
            .statusCode(200)
            .extract()
            .asString();
    JsonNode node = MAPPER.readTree(body);
    JsonNode metadata = node.path("metadata");
    Assertions.assertFalse(metadata.isMissingNode(), "Table response should contain metadata");
    return metadata;
  }

  private static JsonNode canonicalizeMetadata(JsonNode metadata, String metadataLocation)
      throws IOException {
    String location =
        metadataLocation != null && !metadataLocation.isBlank()
            ? metadataLocation
            : metadata
                .path("metadata-location")
                .asText(
                    metadata
                        .path("properties")
                        .path("metadata-location")
                        .asText("s3://mock/metadata"));
    TableMetadata parsed = TableMetadataParser.fromJson(location, metadata);
    return MAPPER.readTree(TableMetadataParser.toJson(parsed));
  }

  private String ensurePromotedMetadata(String location) {
    if (location == null || location.isBlank()) {
      return location;
    }
    if (location.startsWith(FIXTURE_METADATA_PREFIX)) {
      return location;
    }
    return TestS3Fixtures.bucketUri(location);
  }

  private Path localS3Path(String location) {
    URI uri = URI.create(location);
    if (!"s3".equalsIgnoreCase(uri.getScheme())) {
      throw new IllegalArgumentException("Only s3:// URIs are supported: " + location);
    }
    String bucket = uri.getHost();
    if (bucket == null || bucket.isBlank()) {
      throw new IllegalArgumentException("Missing bucket in s3 URI: " + location);
    }
    String key = uri.getPath() == null ? "" : uri.getPath().replaceFirst("^/", "");
    return key.isEmpty() ? TEST_S3_ROOT.resolve(bucket) : TEST_S3_ROOT.resolve(bucket).resolve(key);
  }

  private Map<String, Object> stageCreateRequest(String table, String namespace) {
    String stagedMetadataLocation = TestS3Fixtures.stageTableUri(namespace, table, METADATA_V3);
    return Map.of(
        "name",
        table,
        "schema",
        Map.of(
            "schema-id",
            1,
            "last-column-id",
            1,
            "type",
            "struct",
            "fields",
            List.of(Map.of("id", 1, "name", "id", "required", true, "type", "int"))),
        "partition-spec",
        Map.of(
            "spec-id",
            0,
            "fields",
            List.of(Map.of("name", "id", "field-id", 1, "source-id", 1, "transform", "identity"))),
        "write-order",
        Map.of("sort-order-id", 0, "fields", List.of(Map.of("source-id", 1))),
        "properties",
        Map.of(
            "metadata-location",
            stagedMetadataLocation,
            "io-impl",
            InMemoryS3FileIO.class.getName(),
            "fs.floecat.test-root",
            TEST_S3_ROOT.toAbsolutePath().toString()),
        "location",
        String.format("s3://%s/%s/%s", STAGE_BUCKET, namespace, table),
        "stage-create",
        true);
  }

  private Connector awaitConnectorForTable(String namespace, String table, Duration timeout) {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      Connector connector =
          withConnectorsClient(
              stub -> {
                ListConnectorsResponse response =
                    stub.listConnectors(ListConnectorsRequest.newBuilder().build());
                return response.getConnectorsList().stream()
                    .filter(
                        c ->
                            table.equals(c.getPropertiesMap().get("external.table-name"))
                                && namespace.equals(
                                    c.getPropertiesMap().getOrDefault("external.namespace", "")))
                    .findFirst()
                    .orElse(null);
              });
      if (connector != null) {
        return connector;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    Assertions.fail("Timed out waiting for connector for " + namespace + "." + table);
    return null;
  }

  private <T> T withConnectorsClient(Function<ConnectorsGrpc.ConnectorsBlockingStub, T> fn) {
    parseUpstreamTarget();
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(upstreamHost, serviceGrpcPort).usePlaintext().build();
    try {
      Metadata headers = new Metadata();
      headers.put(
          Metadata.Key.of(ACCOUNT_HEADER_NAME, Metadata.ASCII_STRING_MARSHALLER), DEFAULT_ACCOUNT);
      headers.put(
          Metadata.Key.of(AUTH_HEADER_NAME, Metadata.ASCII_STRING_MARSHALLER), DEFAULT_AUTH_HEADER);
      ConnectorsGrpc.ConnectorsBlockingStub stub =
          ConnectorsGrpc.newBlockingStub(channel)
              .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
      return fn.apply(stub);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.UNAVAILABLE) {
        Assumptions.assumeTrue(
            false, "Connectors gRPC endpoint unavailable (" + e.getMessage() + ")");
        return null;
      }
      throw e;
    } finally {
      channel.shutdownNow();
    }
  }

  private static void reseedFixtureBucket() {
    try {
      TestS3Fixtures.seedFixtures();
    } catch (RuntimeException e) {
      System.err.printf("Failed to reseed fixture bucket after purge test: %s%n", e.getMessage());
    }
  }
}
