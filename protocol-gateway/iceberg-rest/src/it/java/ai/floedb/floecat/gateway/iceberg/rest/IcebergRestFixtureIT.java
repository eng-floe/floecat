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

package ai.floedb.floecat.gateway.iceberg.rest;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.account.rpc.AccountServiceGrpc;
import ai.floedb.floecat.account.rpc.ListAccountsRequest;
import ai.floedb.floecat.account.rpc.ListAccountsResponse;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.GetTableStatsRequest;
import ai.floedb.floecat.catalog.rpc.GetTableStatsResponse;
import ai.floedb.floecat.catalog.rpc.ListColumnStatsRequest;
import ai.floedb.floecat.catalog.rpc.ListColumnStatsResponse;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.ResolveTableRequest;
import ai.floedb.floecat.catalog.rpc.ResolveTableResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.connector.rpc.GetConnectorRequest;
import ai.floedb.floecat.connector.rpc.ListConnectorsRequest;
import ai.floedb.floecat.connector.rpc.ListConnectorsResponse;
import ai.floedb.floecat.connector.rpc.SyncCaptureRequest;
import ai.floedb.floecat.connector.rpc.TriggerReconcileRequest;
import ai.floedb.floecat.gateway.iceberg.rest.common.RealServiceTestResource;
import ai.floedb.floecat.gateway.iceberg.rest.common.TestS3Fixtures;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractBlockingStub;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.SchemaParser;
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
  private static final String DEFAULT_ACCOUNT_NAME = "t-0001";
  private static volatile String seedAccountId;
  private static final String NAMESPACE_PREFIX = "fixture_ns_";
  private static final String TABLE_PREFIX = "fixture_tbl_";
  private static final String CATALOG = "examples";
  private static final String FIXTURE_METADATA_PREFIX = TestS3Fixtures.bucketUri("metadata/");
  private static final String METADATA_V1 =
      "metadata/00000-16393a9a-3433-440c-98f4-fe023ed03973.metadata.json";
  private static final String METADATA_V3 =
      "metadata/00002-503f4508-3824-4cb6-bdf1-4bd6bf5a0ade.metadata.json";
  private static final Path TEST_S3_ROOT = TestS3Fixtures.bucketPath().getParent();
  private static final String STAGE_BUCKET = "staged-fixtures";
  private static final String DEFAULT_AUTH_HEADER = "Bearer integration-test";
  private static final String AUTH_HEADER_NAME = "authorization";
  private static final boolean USE_AWS_FIXTURES = TestS3Fixtures.useAwsFixtures();

  private static long expectedSnapshotId;
  private static List<Long> fixtureSnapshotIds;
  private static Map<Long, String> fixtureManifestLists = new HashMap<>();
  private static Integer fixtureSchemaId;
  private static String fixtureSchemaJson;
  private static Integer fixtureLastColumnId;
  private static Integer fixtureDefaultSpecId;
  private static Integer fixtureDefaultSortOrderId;
  private static TableMetadata fixtureMetadata;
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
    fixtureMetadata = loadFixtureMetadata(METADATA_V3);
    fixtureSchemaId = fixtureMetadata.currentSchemaId();
    fixtureLastColumnId = fixtureMetadata.lastColumnId();
    fixtureDefaultSpecId = fixtureMetadata.defaultSpecId();
    fixtureDefaultSortOrderId = fixtureMetadata.defaultSortOrderId();
    fixtureSchemaJson = SchemaParser.toJson(fixtureMetadata.schema());
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
        .body(Map.of("namespace", List.of(namespace)))
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(200);

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
    Assertions.assertNotNull(fixtureSchemaId, "Fixture schema id should be available");
    Assertions.assertNotNull(fixtureSchemaJson, "Fixture schema JSON should be available");

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
                Map.entry("schema-id", fixtureSchemaId),
                Map.entry("schema-json", fixtureSchemaJson),
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
    Map<String, Object> setMainRefUpdate =
        Map.of(
            "action",
            "set-snapshot-ref",
            "ref-name",
            "main",
            "type",
            "branch",
            "snapshot-id",
            newSnapshotId);
    Map<String, Object> commitPayload =
        Map.of(
            "requirements",
            List.of(),
            "updates",
            List.of(addSnapshotUpdate, removeSnapshotUpdate, setRefUpdate, setMainRefUpdate));

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
        .body(Map.of("namespace", List.of(namespace)))
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(200);

    String stageId = "stage-" + UUID.randomUUID();
    Map<String, Object> stageRequest = stageCreateRequest(table, namespace);
    given()
        .spec(spec)
        .header("Iceberg-Transaction-Id", stageId)
        .body(stageRequest)
        .when()
        .post("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables")
        .then()
        .log()
        .ifValidationFails()
        .statusCode(200);

    given()
        .spec(spec)
        .header("Iceberg-Transaction-Id", stageId)
        .body(
            Map.of(
                "table-changes",
                List.of(
                    Map.of(
                        "identifier",
                        Map.of("namespace", List.of(namespace), "name", table),
                        "requirements",
                        List.of(),
                        "updates",
                        List.of()))))
        .when()
        .post("/v1/" + CATALOG + "/transactions/commit")
        .then()
        .statusCode(204);

    String commitMetadataLocation =
        ensurePromotedMetadata(fetchTablePropertyMetadataLocation(namespace, table));
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
                        GetConnectorRequest.newBuilder()
                            .setConnectorId(connector.getResourceId())
                            .build())
                    .getConnector());

    String stagePrefix = TestS3Fixtures.stageTableUri(namespace, table, "metadata/");
    Assertions.assertTrue(
        commitMetadataLocation.startsWith(stagePrefix),
        () ->
            "persisted metadata should reside under the stage bucket: "
                + commitMetadataLocation);
    assertFixtureObjectExists(
        commitMetadataLocation, "persisted metadata file should exist in fixture storage");
    Assertions.assertEquals(
        commitMetadataLocation,
        refreshed.getPropertiesMap().get("external.metadata-location"),
        "Connector external metadata location should match the persisted metadata");
  }

  @Test
  void transactionCommitSupportsAddSnapshot() {
    String namespace = NAMESPACE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String table = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    TestS3Fixtures.seedStageTable(namespace, table);

    given()
        .spec(spec)
        .body(Map.of("namespace", namespace))
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(200);

    String stageId = "stage-" + UUID.randomUUID();
    Map<String, Object> stageRequest = stageCreateRequest(table, namespace);
    given()
        .spec(spec)
        .header("Iceberg-Transaction-Id", stageId)
        .body(stageRequest)
        .when()
        .post("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables")
        .then()
        .log()
        .ifValidationFails()
        .statusCode(200);

    Assertions.assertNotNull(fixtureSchemaId, "Fixture schema id should be available");
    Assertions.assertNotNull(fixtureSchemaJson, "Fixture schema JSON should be available");
    String anyFixtureManifestRel = fixtureManifestLists.values().stream().findFirst().orElseThrow();
    String manifestList = TestS3Fixtures.stageTableUri(namespace, table, anyFixtureManifestRel);
    long snapshotId = System.currentTimeMillis();

    Map<String, Object> addSnapshotUpdate =
        Map.of(
            "action",
            "add-snapshot",
            "snapshot",
            Map.ofEntries(
                Map.entry("snapshot-id", snapshotId),
                Map.entry("timestamp-ms", System.currentTimeMillis()),
                Map.entry("sequence-number", 1L),
                Map.entry("manifest-list", manifestList),
                Map.entry("schema-id", fixtureSchemaId),
                Map.entry("schema-json", fixtureSchemaJson),
                Map.entry("summary", Map.of("operation", "append"))));
    Map<String, Object> setRefUpdate =
        Map.of(
            "action",
            "set-snapshot-ref",
            "ref-name",
            "main",
            "snapshot-id",
            snapshotId,
            "type",
            "branch");

    given()
        .spec(spec)
        .header("Iceberg-Transaction-Id", stageId)
        .body(
            Map.of(
                "table-changes",
                List.of(
                    Map.of(
                        "identifier",
                        Map.of("namespace", List.of(namespace), "name", table),
                        "requirements",
                        List.of(Map.of("type", "assert-create")),
                        "updates",
                        List.of(addSnapshotUpdate, setRefUpdate)))))
        .when()
        .post("/v1/" + CATALOG + "/transactions/commit")
        .then()
        .statusCode(204);

    var response =
        given()
            .spec(spec)
            .when()
            .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
            .then()
            .statusCode(200)
            .extract()
            .jsonPath();
    long currentSnapshot =
        ((Number) response.getLong("metadata.current-snapshot-id")).longValue();
    Assertions.assertEquals(snapshotId, currentSnapshot);
    Object mainSnapshot = response.get("metadata.refs.main.snapshot-id");
    Assertions.assertEquals(
        snapshotId,
        mainSnapshot instanceof Number ? ((Number) mainSnapshot).longValue() : mainSnapshot);
  }

  @Test
  void transactionCommitEnforcesAssertRefSnapshotId() {
    String namespace = NAMESPACE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String table = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    TestS3Fixtures.seedStageTable(namespace, table);

    given()
        .spec(spec)
        .body(Map.of("namespace", namespace))
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(200);

    registerTable(namespace, table, METADATA_V3, false);

    Number actualSnapshot =
        given()
            .spec(spec)
            .when()
            .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
            .then()
            .statusCode(200)
            .extract()
            .jsonPath()
            .getLong("metadata.current-snapshot-id");

    long wrongSnapshot = actualSnapshot == null ? 1L : actualSnapshot.longValue() + 1;
    Map<String, Object> assertRef =
        Map.of(
            "type",
            "assert-ref-snapshot-id",
            "ref",
            "main",
            "snapshot-id",
            wrongSnapshot);

    given()
        .spec(spec)
        .body(
            Map.of(
                "table-changes",
                List.of(
                    Map.of(
                        "identifier",
                        Map.of("namespace", List.of(namespace), "name", table),
                        "requirements",
                        List.of(assertRef),
                        "updates",
                        List.of()))))
        .when()
        .post("/v1/" + CATALOG + "/transactions/commit")
        .then()
        .statusCode(409);
  }

  @Test
  void transactionCommitAssertCreateFailsWhenTableAlreadyExists() {
    String namespace = NAMESPACE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String table = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    TestS3Fixtures.seedStageTable(namespace, table);

    given()
        .spec(spec)
        .body(Map.of("namespace", namespace))
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(200);

    registerTable(namespace, table, METADATA_V3, false);

    given()
        .spec(spec)
        .body(
            Map.of(
                "table-changes",
                List.of(
                    Map.of(
                        "identifier",
                        Map.of("namespace", List.of(namespace), "name", table),
                        "requirements",
                        List.of(Map.of("type", "assert-create")),
                        "updates",
                        List.of()))))
        .when()
        .post("/v1/" + CATALOG + "/transactions/commit")
        .then()
        .statusCode(409)
        .body("error.type", equalTo("CommitFailedException"));
  }

  @Test
  void transactionCommitAssertRefSnapshotIdNullFailsWhenRefExists() {
    String namespace = NAMESPACE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String table = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    TestS3Fixtures.seedStageTable(namespace, table);

    given()
        .spec(spec)
        .body(Map.of("namespace", namespace))
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(200);

    registerTable(namespace, table, METADATA_V3, false);

    given()
        .spec(spec)
        .body(
            Map.of(
                "table-changes",
                List.of(
                    Map.of(
                        "identifier",
                        Map.of("namespace", List.of(namespace), "name", table),
                        "requirements",
                        List.of(assertRefSnapshotIdRequirement("main", null)),
                        "updates",
                        List.of()))))
        .when()
        .post("/v1/" + CATALOG + "/transactions/commit")
        .then()
        .statusCode(409)
        .body("error.type", equalTo("CommitFailedException"));
  }

  @Test
  void transactionCommitAssertRefSnapshotIdNullPassesWhenRefMissing() {
    String namespace = NAMESPACE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String table = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    TestS3Fixtures.seedStageTable(namespace, table);

    given()
        .spec(spec)
        .body(Map.of("namespace", namespace))
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(200);

    registerTable(namespace, table, METADATA_V3, false);

    given()
        .spec(spec)
        .body(
            Map.of(
                "table-changes",
                List.of(
                    Map.of(
                        "identifier",
                        Map.of("namespace", List.of(namespace), "name", table),
                        "requirements",
                        List.of(assertRefSnapshotIdRequirement("dev", null)),
                        "updates",
                        List.of()))))
        .when()
        .post("/v1/" + CATALOG + "/transactions/commit")
        .then()
        .statusCode(204);
  }

  @Test
  void transactionCommitRollsBackSnapshotChangesOnFailure() {
    String namespace = NAMESPACE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String table = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    TestS3Fixtures.seedStageTable(namespace, table);

    given()
        .spec(spec)
        .body(Map.of("namespace", namespace))
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(200);

    registerTable(namespace, table, METADATA_V3, false);

    List<Long> initialSnapshots = fetchSnapshotIds(namespace, table);
    Number initialMainRef =
        given()
            .spec(spec)
            .when()
            .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
            .then()
            .statusCode(200)
            .extract()
            .jsonPath()
            .getLong("metadata.refs.main.snapshot-id");
    if (initialMainRef == null) {
      initialMainRef =
          given()
              .spec(spec)
              .when()
              .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
              .then()
              .statusCode(200)
              .extract()
              .jsonPath()
              .getLong("metadata.current-snapshot-id");
    }

    Assertions.assertNotNull(fixtureSchemaId, "Fixture schema id should be available");
    Assertions.assertNotNull(fixtureSchemaJson, "Fixture schema JSON should be available");
    String anyFixtureManifestRel = fixtureManifestLists.values().stream().findFirst().orElseThrow();
    String manifestList = TestS3Fixtures.stageTableUri(namespace, table, anyFixtureManifestRel);
    long newSnapshotId = System.currentTimeMillis();

    Map<String, Object> addSnapshotUpdate =
        Map.of(
            "action",
            "add-snapshot",
            "snapshot",
            Map.ofEntries(
                Map.entry("snapshot-id", newSnapshotId),
                Map.entry("timestamp-ms", System.currentTimeMillis()),
                Map.entry("sequence-number", 1L),
                Map.entry("manifest-list", manifestList),
                Map.entry("schema-id", fixtureSchemaId),
                Map.entry("schema-json", fixtureSchemaJson),
                Map.entry("summary", Map.of("operation", "append"))));
    Map<String, Object> setRefUpdate =
        Map.of(
            "action",
            "set-snapshot-ref",
            "ref-name",
            "main",
            "snapshot-id",
            newSnapshotId,
            "type",
            "branch");
    Map<String, Object> badRemoval =
        Map.of("action", "remove-snapshots", "snapshot-ids", List.of());

    given()
        .spec(spec)
        .body(
            Map.of(
                "table-changes",
                List.of(
                    Map.of(
                        "identifier",
                        Map.of("namespace", List.of(namespace), "name", table),
                        "requirements",
                        List.of(),
                        "updates",
                        List.of(addSnapshotUpdate, setRefUpdate, badRemoval)))))
        .when()
        .post("/v1/" + CATALOG + "/transactions/commit")
        .then()
        .statusCode(400);

    List<Long> finalSnapshots = fetchSnapshotIds(namespace, table);
    Assertions.assertEquals(
        initialSnapshots.size(),
        finalSnapshots.size(),
        "Snapshot count should match after rollback");
    Assertions.assertTrue(
        finalSnapshots.containsAll(initialSnapshots),
        "Snapshots should be restored after rollback");

    Number finalMainRef =
        given()
            .spec(spec)
            .when()
            .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
            .then()
            .statusCode(200)
            .extract()
            .jsonPath()
            .getLong("metadata.refs.main.snapshot-id");
    if (finalMainRef == null) {
      finalMainRef =
          given()
              .spec(spec)
              .when()
              .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
              .then()
              .statusCode(200)
              .extract()
              .jsonPath()
              .getLong("metadata.current-snapshot-id");
    }
    Assertions.assertEquals(
        initialMainRef == null ? null : initialMainRef.longValue(),
        finalMainRef == null ? null : finalMainRef.longValue(),
        "Main ref should be restored after rollback");
  }

  @Test
  void transactionCommitIsIdempotent() {
    String namespace = NAMESPACE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String table = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    TestS3Fixtures.seedStageTable(namespace, table);

    given()
        .spec(spec)
        .body(Map.of("namespace", namespace))
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(200);

    registerTable(namespace, table, METADATA_V3, false);

    Assertions.assertNotNull(fixtureSchemaId, "Fixture schema id should be available");
    Assertions.assertNotNull(fixtureSchemaJson, "Fixture schema JSON should be available");
    String anyFixtureManifestRel = fixtureManifestLists.values().stream().findFirst().orElseThrow();
    String manifestList = TestS3Fixtures.stageTableUri(namespace, table, anyFixtureManifestRel);
    long snapshotId = System.currentTimeMillis();

    Map<String, Object> addSnapshotUpdate =
        Map.of(
            "action",
            "add-snapshot",
            "snapshot",
            Map.ofEntries(
                Map.entry("snapshot-id", snapshotId),
                Map.entry("timestamp-ms", System.currentTimeMillis()),
                Map.entry("sequence-number", 1L),
                Map.entry("manifest-list", manifestList),
                Map.entry("schema-id", fixtureSchemaId),
                Map.entry("schema-json", fixtureSchemaJson),
                Map.entry("summary", Map.of("operation", "append"))));
    Map<String, Object> setRefUpdate =
        Map.of(
            "action",
            "set-snapshot-ref",
            "ref-name",
            "main",
            "snapshot-id",
            snapshotId,
            "type",
            "branch");

    Map<String, Object> commitBody =
        Map.of(
            "table-changes",
            List.of(
                Map.of(
                    "identifier",
                    Map.of("namespace", List.of(namespace), "name", table),
                    "requirements",
                    List.of(),
                    "updates",
                    List.of(addSnapshotUpdate, setRefUpdate))));

    String idemKey = "idem-" + UUID.randomUUID();
    given()
        .spec(spec)
        .header("Idempotency-Key", idemKey)
        .body(commitBody)
        .when()
        .post("/v1/" + CATALOG + "/transactions/commit")
        .then()
        .statusCode(204);

    List<Long> snapshotIdsAfterFirst = fetchSnapshotIds(namespace, table);

    given()
        .spec(spec)
        .header("Idempotency-Key", idemKey)
        .body(commitBody)
        .when()
        .post("/v1/" + CATALOG + "/transactions/commit")
        .then()
        .statusCode(204);

    List<Long> snapshotIdsAfterSecond = fetchSnapshotIds(namespace, table);
    Assertions.assertEquals(
        snapshotIdsAfterFirst.size(),
        snapshotIdsAfterSecond.size(),
        "Idempotent retry should not create extra snapshots");
    Assertions.assertTrue(
        snapshotIdsAfterSecond.containsAll(snapshotIdsAfterFirst),
        "Snapshot set should be unchanged after retry");
  }

  @Test
  void transactionCommitRetryAfterFailureIsSafe() {
    String namespace = NAMESPACE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String table = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    TestS3Fixtures.seedStageTable(namespace, table);

    given()
        .spec(spec)
        .body(Map.of("namespace", namespace))
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(200);

    registerTable(namespace, table, METADATA_V3, false);

    List<Long> initialSnapshots = fetchSnapshotIds(namespace, table);
    Number initialMainRef =
        given()
            .spec(spec)
            .when()
            .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
            .then()
            .statusCode(200)
            .extract()
            .jsonPath()
            .getLong("metadata.refs.main.snapshot-id");
    if (initialMainRef == null) {
      initialMainRef =
          given()
              .spec(spec)
              .when()
              .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
              .then()
              .statusCode(200)
              .extract()
              .jsonPath()
              .getLong("metadata.current-snapshot-id");
    }

    Assertions.assertNotNull(fixtureSchemaId, "Fixture schema id should be available");
    Assertions.assertNotNull(fixtureSchemaJson, "Fixture schema JSON should be available");
    String anyFixtureManifestRel = fixtureManifestLists.values().stream().findFirst().orElseThrow();
    String manifestList = TestS3Fixtures.stageTableUri(namespace, table, anyFixtureManifestRel);
    long newSnapshotId = System.currentTimeMillis();

    Map<String, Object> addSnapshotUpdate =
        Map.of(
            "action",
            "add-snapshot",
            "snapshot",
            Map.ofEntries(
                Map.entry("snapshot-id", newSnapshotId),
                Map.entry("timestamp-ms", System.currentTimeMillis()),
                Map.entry("sequence-number", 1L),
                Map.entry("manifest-list", manifestList),
                Map.entry("schema-id", fixtureSchemaId),
                Map.entry("schema-json", fixtureSchemaJson),
                Map.entry("summary", Map.of("operation", "append"))));
    Map<String, Object> setRefUpdate =
        Map.of(
            "action",
            "set-snapshot-ref",
            "ref-name",
            "main",
            "snapshot-id",
            newSnapshotId,
            "type",
            "branch");
    Map<String, Object> badRemoval =
        Map.of("action", "remove-snapshots", "snapshot-ids", List.of());

    Map<String, Object> commitBody =
        Map.of(
            "table-changes",
            List.of(
                Map.of(
                    "identifier",
                    Map.of("namespace", List.of(namespace), "name", table),
                    "requirements",
                    List.of(),
                    "updates",
                    List.of(addSnapshotUpdate, setRefUpdate, badRemoval))));

    String idemKey = "idem-" + UUID.randomUUID();
    given()
        .spec(spec)
        .header("Idempotency-Key", idemKey)
        .body(commitBody)
        .when()
        .post("/v1/" + CATALOG + "/transactions/commit")
        .then()
        .statusCode(400);

    List<Long> afterFirst = fetchSnapshotIds(namespace, table);
    Assertions.assertEquals(
        initialSnapshots.size(),
        afterFirst.size(),
        "Snapshots should be rolled back after failure");

    given()
        .spec(spec)
        .header("Idempotency-Key", idemKey)
        .body(commitBody)
        .when()
        .post("/v1/" + CATALOG + "/transactions/commit")
        .then()
        .statusCode(400);

    List<Long> afterSecond = fetchSnapshotIds(namespace, table);
    Assertions.assertEquals(
        initialSnapshots.size(),
        afterSecond.size(),
        "Retry after failure should not create snapshots");

    Number finalMainRef =
        given()
            .spec(spec)
            .when()
            .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
            .then()
            .statusCode(200)
            .extract()
            .jsonPath()
            .getLong("metadata.refs.main.snapshot-id");
    if (finalMainRef == null) {
      finalMainRef =
          given()
              .spec(spec)
              .when()
              .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
              .then()
              .statusCode(200)
              .extract()
              .jsonPath()
              .getLong("metadata.current-snapshot-id");
    }
    Assertions.assertEquals(
        initialMainRef == null ? null : initialMainRef.longValue(),
        finalMainRef == null ? null : finalMainRef.longValue(),
        "Main ref should remain unchanged after retry");
  }

  @Test
  void transactionCommitFailsWhenAnyRequirementFails() {
    String namespace = NAMESPACE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String tableA = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String tableB = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    TestS3Fixtures.seedStageTable(namespace, tableA);
    TestS3Fixtures.seedStageTable(namespace, tableB);

    given()
        .spec(spec)
        .body(Map.of("namespace", namespace))
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(200);

    registerTable(namespace, tableA, METADATA_V3, false);
    registerTable(namespace, tableB, METADATA_V3, false);

    List<Long> initialSnapshotsA = fetchSnapshotIds(namespace, tableA);

    Number currentSnapshotB =
        given()
            .spec(spec)
            .when()
            .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + tableB)
            .then()
            .statusCode(200)
            .extract()
            .jsonPath()
            .getLong("metadata.current-snapshot-id");
    long wrongSnapshotB = currentSnapshotB == null ? 1L : currentSnapshotB.longValue() + 1;

    Assertions.assertNotNull(fixtureSchemaId, "Fixture schema id should be available");
    Assertions.assertNotNull(fixtureSchemaJson, "Fixture schema JSON should be available");
    String anyFixtureManifestRel = fixtureManifestLists.values().stream().findFirst().orElseThrow();
    String manifestList = TestS3Fixtures.stageTableUri(namespace, tableA, anyFixtureManifestRel);
    long newSnapshotId = System.currentTimeMillis();

    Map<String, Object> addSnapshotUpdate =
        Map.of(
            "action",
            "add-snapshot",
            "snapshot",
            Map.ofEntries(
                Map.entry("snapshot-id", newSnapshotId),
                Map.entry("timestamp-ms", System.currentTimeMillis()),
                Map.entry("sequence-number", 1L),
                Map.entry("manifest-list", manifestList),
                Map.entry("schema-id", fixtureSchemaId),
                Map.entry("schema-json", fixtureSchemaJson),
                Map.entry("summary", Map.of("operation", "append"))));

    Map<String, Object> badRequirement =
        Map.of(
            "type",
            "assert-ref-snapshot-id",
            "ref",
            "main",
            "snapshot-id",
            wrongSnapshotB);

    Map<String, Object> commitBody =
        Map.of(
            "table-changes",
            List.of(
                Map.of(
                    "identifier",
                    Map.of("namespace", List.of(namespace), "name", tableA),
                    "requirements",
                    List.of(),
                    "updates",
                    List.of(addSnapshotUpdate)),
                Map.of(
                    "identifier",
                    Map.of("namespace", List.of(namespace), "name", tableB),
                    "requirements",
                    List.of(badRequirement),
                    "updates",
                    List.of())));

    given()
        .spec(spec)
        .body(commitBody)
        .when()
        .post("/v1/" + CATALOG + "/transactions/commit")
        .then()
        .statusCode(409);

    List<Long> finalSnapshotsA = fetchSnapshotIds(namespace, tableA);
    Assertions.assertEquals(
        initialSnapshotsA.size(),
        finalSnapshotsA.size(),
        "No snapshots should be added when any requirement fails");
    Assertions.assertTrue(
        finalSnapshotsA.containsAll(initialSnapshotsA),
        "Snapshot set should remain unchanged after failure");
  }

  @Test
  void transactionCommitAppliesAllTableChangesAtomically() {
    String namespace = NAMESPACE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String tableA = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String tableB = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    TestS3Fixtures.seedStageTable(namespace, tableA);
    TestS3Fixtures.seedStageTable(namespace, tableB);

    given()
        .spec(spec)
        .body(Map.of("namespace", namespace))
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(200);

    registerTable(namespace, tableA, METADATA_V3, false);
    registerTable(namespace, tableB, METADATA_V3, false);

    String ownerA = "txn-owner-a-" + UUID.randomUUID();
    String ownerB = "txn-owner-b-" + UUID.randomUUID();
    Map<String, Object> setOwnerA =
        Map.of("action", "set-properties", "updates", Map.of("owner", ownerA));
    Map<String, Object> setOwnerB =
        Map.of("action", "set-properties", "updates", Map.of("owner", ownerB));

    given()
        .spec(spec)
        .body(
            Map.of(
                "table-changes",
                List.of(
                    Map.of(
                        "identifier",
                        Map.of("namespace", List.of(namespace), "name", tableA),
                        "requirements",
                        List.of(),
                        "updates",
                        List.of(setOwnerA)),
                    Map.of(
                        "identifier",
                        Map.of("namespace", List.of(namespace), "name", tableB),
                        "requirements",
                        List.of(),
                        "updates",
                        List.of(setOwnerB)))))
        .when()
        .post("/v1/" + CATALOG + "/transactions/commit")
        .then()
        .statusCode(204);

    Assertions.assertEquals(ownerA, fetchTableMetadataProperty(namespace, tableA, "owner"));
    Assertions.assertEquals(ownerB, fetchTableMetadataProperty(namespace, tableB, "owner"));
  }

  @Test
  void transactionCommitIsIdempotentForMultipleTables() {
    String namespace = NAMESPACE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String tableA = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String tableB = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    TestS3Fixtures.seedStageTable(namespace, tableA);
    TestS3Fixtures.seedStageTable(namespace, tableB);

    given()
        .spec(spec)
        .body(Map.of("namespace", namespace))
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(200);

    registerTable(namespace, tableA, METADATA_V3, false);
    registerTable(namespace, tableB, METADATA_V3, false);

    String ownerA = "txn-idem-owner-a-" + UUID.randomUUID();
    String ownerB = "txn-idem-owner-b-" + UUID.randomUUID();
    Map<String, Object> setOwnerA =
        Map.of("action", "set-properties", "updates", Map.of("owner", ownerA));
    Map<String, Object> setOwnerB =
        Map.of("action", "set-properties", "updates", Map.of("owner", ownerB));
    Map<String, Object> commitBody =
        Map.of(
            "table-changes",
            List.of(
                Map.of(
                    "identifier",
                    Map.of("namespace", List.of(namespace), "name", tableA),
                    "requirements",
                    List.of(),
                    "updates",
                    List.of(setOwnerA)),
                Map.of(
                    "identifier",
                    Map.of("namespace", List.of(namespace), "name", tableB),
                    "requirements",
                    List.of(),
                    "updates",
                    List.of(setOwnerB))));

    String idemKey = "idem-" + UUID.randomUUID();
    given()
        .spec(spec)
        .header("Idempotency-Key", idemKey)
        .body(commitBody)
        .when()
        .post("/v1/" + CATALOG + "/transactions/commit")
        .then()
        .statusCode(204);

    String tableAMetadataAfterFirst = fetchTablePropertyMetadataLocation(namespace, tableA);
    String tableBMetadataAfterFirst = fetchTablePropertyMetadataLocation(namespace, tableB);

    given()
        .spec(spec)
        .header("Idempotency-Key", idemKey)
        .body(commitBody)
        .when()
        .post("/v1/" + CATALOG + "/transactions/commit")
        .then()
        .statusCode(204);

    String tableAMetadataAfterSecond = fetchTablePropertyMetadataLocation(namespace, tableA);
    String tableBMetadataAfterSecond = fetchTablePropertyMetadataLocation(namespace, tableB);
    Assertions.assertEquals(tableAMetadataAfterFirst, tableAMetadataAfterSecond);
    Assertions.assertEquals(tableBMetadataAfterFirst, tableBMetadataAfterSecond);
    Assertions.assertEquals(ownerA, fetchTableMetadataProperty(namespace, tableA, "owner"));
    Assertions.assertEquals(ownerB, fetchTableMetadataProperty(namespace, tableB, "owner"));
  }

  @Test
  void transactionCommitFailureDoesNotPartiallyApplyOtherTableChanges() {
    String namespace = NAMESPACE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String tableA = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String tableB = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    TestS3Fixtures.seedStageTable(namespace, tableA);
    TestS3Fixtures.seedStageTable(namespace, tableB);

    given()
        .spec(spec)
        .body(Map.of("namespace", namespace))
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(200);

    registerTable(namespace, tableA, METADATA_V3, false);
    registerTable(namespace, tableB, METADATA_V3, false);

    String initialOwnerA = fetchTableMetadataProperty(namespace, tableA, "owner");
    String initialOwnerB = fetchTableMetadataProperty(namespace, tableB, "owner");
    String metadataBeforeA = fetchTablePropertyMetadataLocation(namespace, tableA);

    Map<String, Object> setOwnerA =
        Map.of(
            "action",
            "set-properties",
            "updates",
            Map.of("owner", "should-not-apply-" + UUID.randomUUID()));
    Map<String, Object> badRemoval =
        Map.of("action", "remove-snapshots", "snapshot-ids", List.of());

    given()
        .spec(spec)
        .body(
            Map.of(
                "table-changes",
                List.of(
                    Map.of(
                        "identifier",
                        Map.of("namespace", List.of(namespace), "name", tableA),
                        "requirements",
                        List.of(),
                        "updates",
                        List.of(setOwnerA)),
                    Map.of(
                        "identifier",
                        Map.of("namespace", List.of(namespace), "name", tableB),
                        "requirements",
                        List.of(),
                        "updates",
                        List.of(badRemoval)))))
        .when()
        .post("/v1/" + CATALOG + "/transactions/commit")
        .then()
        .statusCode(400);

    Assertions.assertEquals(initialOwnerA, fetchTableMetadataProperty(namespace, tableA, "owner"));
    Assertions.assertEquals(initialOwnerB, fetchTableMetadataProperty(namespace, tableB, "owner"));
    Assertions.assertEquals(metadataBeforeA, fetchTablePropertyMetadataLocation(namespace, tableA));
  }

  @Test
  void deleteTablePurgesMetadataWhenRequested() {
    try {
      String namespace = NAMESPACE_PREFIX + UUID.randomUUID().toString().replace("-", "");
      given()
          .spec(spec)
          .body(Map.of("namespace", List.of(namespace)))
          .when()
          .post("/v1/" + CATALOG + "/namespaces")
          .then()
          .statusCode(200);

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
      String metadataLocation = TestS3Fixtures.bucketUri(METADATA_V3);
      assertFixtureObjectExists(
          metadataLocation, "Fixture metadata should exist before issuing purge delete request");

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
      assertFixtureObjectMissing(
          metadataLocation, "metadata file should be deleted when purgeRequested=true");
      fixtureManifestLists
          .values()
          .forEach(
              relative -> {
                String manifestLocation = TestS3Fixtures.bucketUri(relative);
                assertFixtureObjectMissing(
                    manifestLocation,
                    "manifest file should be deleted when purgeRequested=true: "
                        + manifestLocation);
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
        .body(Map.of("namespace", List.of(namespace)))
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(200);

    registerTable(namespace, table, METADATA_V3, false);

    JsonNode actualMetadata = fetchPersistedMetadata(namespace, table);
    Path expectedMetadataPath = resolveFixtureMetadata(METADATA_V3);
    JsonNode expectedMetadata = MAPPER.readTree(expectedMetadataPath.toFile());

    Assertions.assertEquals(
        canonicalizeMetadata(expectedMetadata, TestS3Fixtures.bucketUri(METADATA_V3)),
        canonicalizeMetadata(actualMetadata, fetchMetadataLocation(namespace, table)),
        "Persisted Iceberg metadata should match original fixture contents");
  }

  private static List<Long> parseSnapshotIds(String relativeMetadataPath) throws IOException {
    Path metadataPath = resolveFixtureMetadata(relativeMetadataPath);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.readTree(metadataPath.toFile());
    List<Long> ids = new ArrayList<>();
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

  private static TableMetadata loadFixtureMetadata(String relativeMetadataPath) throws IOException {
    Path metadataPath = resolveFixtureMetadata(relativeMetadataPath);
    JsonNode node = MAPPER.readTree(metadataPath.toFile());
    return TableMetadataParser.fromJson(TestS3Fixtures.bucketUri(relativeMetadataPath), node);
  }

  private static Path resolveFixtureMetadata(String relativeMetadataPath) throws IOException {
    Path seededPath = TestS3Fixtures.prefixPath().resolve(Path.of(relativeMetadataPath));
    if (Files.isRegularFile(seededPath)) {
      return seededPath;
    }
    Path simplePath = TestS3Fixtures.fixturePath("simple").resolve(Path.of(relativeMetadataPath));
    if (Files.isRegularFile(simplePath)) {
      return simplePath;
    }
    Path complexPath = TestS3Fixtures.fixturePath("complex").resolve(Path.of(relativeMetadataPath));
    if (Files.isRegularFile(complexPath)) {
      return complexPath;
    }
    throw new IOException("Fixture metadata not found for " + relativeMetadataPath);
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

  private static Map<String, Object> assertRefSnapshotIdRequirement(String ref, Long snapshotId) {
    Map<String, Object> requirement = new LinkedHashMap<>();
    requirement.put("type", "assert-ref-snapshot-id");
    requirement.put("ref", ref);
    requirement.put("snapshot-id", snapshotId);
    return requirement;
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
    String accountId = resolveSeedAccountId();
    spec =
        new RequestSpecBuilder()
            .addHeader("authorization", "Bearer integration-test")
            .setContentType(ContentType.JSON)
            .build();
  }

  @Test
  void listsNamespacesFromSeededService() {
    given()
        .spec(spec)
        .when()
        .get("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(200)
        .body("namespaces*.get(0)", hasItem("iceberg"));
  }

  @Test
  void createsAndDeletesNamespaceViaGateway() {
    String ns = uniqueName("it_ns_");
    Map<String, Object> payload = Map.of("namespace", List.of(ns));

    given()
        .spec(spec)
        .body(payload)
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(200)
        .body("namespace[0]", equalTo(ns));

    given()
        .spec(spec)
        .when()
        .get("/v1/" + CATALOG + "/namespaces/" + ns)
        .then()
        .statusCode(200)
        .body("namespace[0]", equalTo(ns));

    deleteNamespaceResource(ns);
  }

  @Test
  void trinoCreateTableSequenceMaterializesMetadata() throws Exception {
    String namespace = uniqueName("it_ns_");
    String table = uniqueName("it_tbl_");
    createNamespace(namespace);

    try {
      given()
          .spec(spec)
          .when()
          .get("/v1/" + CATALOG + "/namespaces/" + namespace)
          .then()
          .statusCode(200);

      given()
          .spec(spec)
          .when()
          .get("/v1/" + CATALOG + "/namespaces/" + namespace)
          .then()
          .statusCode(200);

      Map<String, Object> createPayload = new LinkedHashMap<>();
      createPayload.put("name", table);
      createPayload.put("location", null);
      createPayload.put(
          "schema",
          Map.of(
              "type",
              "struct",
              "schema-id",
              0,
              "fields",
              List.of(Map.of("id", 1, "name", "i", "required", false, "type", "int"))));
      createPayload.put("partition-spec", Map.of("spec-id", 0, "fields", List.of()));
      createPayload.put("write-order", Map.of("order-id", 0, "fields", List.of()));
      createPayload.put(
          "properties",
          Map.of(
              "write.format.default",
              "PARQUET",
              "format-version",
              "2",
              "write.parquet.compression-codec",
              ""));
      createPayload.put("stage-create", false);

      given()
          .spec(spec)
          .body(createPayload)
          .when()
          .post("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables")
          .then()
          .statusCode(200);

      String tableBody =
          given()
              .spec(spec)
              .when()
              .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
              .then()
              .statusCode(200)
              .extract()
              .asString();
      JsonNode tableNode = MAPPER.readTree(tableBody);
      JsonNode metadataNode = tableNode.path("metadata");
      String tableUuid = metadataNode.path("table-uuid").asText(null);
      if (tableUuid == null || tableUuid.isBlank()) {
        tableUuid = metadataNode.path("properties").path("table-uuid").asText(null);
      }
      Assertions.assertNotNull(tableUuid, "table-uuid should be available");

      Map.Entry<Long, String> fixtureManifest =
          fixtureManifestLists.entrySet().stream().findFirst().orElseThrow();
      long snapshotId = fixtureManifest.getKey();
      String manifestList = TestS3Fixtures.bucketUri(fixtureManifest.getValue());
      Map<String, Object> metricsPayload =
          Map.of(
              "report-type",
              "commit-report",
              "table-name",
              CATALOG + "_rest." + namespace + "." + table,
              "snapshot-id",
              snapshotId,
              "sequence-number",
              1,
              "operation",
              "append",
              "metrics",
              Map.of(
                  "total-duration",
                  Map.of("count", 1, "time-unit", "nanoseconds", "total-duration", 78018250),
                  "attempts",
                  Map.of("unit", "count", "value", 1),
                  "total-data-files",
                  Map.of("unit", "count", "value", 0),
                  "total-delete-files",
                  Map.of("unit", "count", "value", 0),
                  "total-records",
                  Map.of("unit", "count", "value", 0),
                  "total-files-size-bytes",
                  Map.of("unit", "bytes", "value", 0),
                  "total-positional-deletes",
                  Map.of("unit", "count", "value", 0),
                  "total-equality-deletes",
                  Map.of("unit", "count", "value", 0)),
              "metadata",
              Map.of("engine-version", "478", "engine-name", "trino", "iceberg-version", "1.10.0"));

      given()
          .spec(spec)
          .body(metricsPayload)
          .when()
          .post("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table + "/metrics")
          .then()
          .statusCode(204);

      given()
          .spec(spec)
          .when()
          .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
          .then()
          .statusCode(200);

      Map<String, Object> commitPayload =
          new LinkedHashMap<>();
      Map<String, Object> assertTableUuid = new LinkedHashMap<>();
      assertTableUuid.put("type", "assert-table-uuid");
      assertTableUuid.put("uuid", tableUuid);
      Map<String, Object> assertRefSnapshot = new LinkedHashMap<>();
      assertRefSnapshot.put("type", "assert-ref-snapshot-id");
      assertRefSnapshot.put("ref", "main");
      assertRefSnapshot.put("snapshot-id", null);
      commitPayload.put("requirements", List.of(assertTableUuid, assertRefSnapshot));
      commitPayload.put(
          "updates",
          List.of(
              Map.of(
                  "action",
                  "remove-properties",
                  "removals",
                  List.of("write.parquet.compression-codec", "format-version")),
              Map.of(
                  "action",
                  "add-snapshot",
                  "snapshot",
                  Map.of(
                      "sequence-number",
                      1,
                      "snapshot-id",
                      snapshotId,
                      "timestamp-ms",
                      System.currentTimeMillis(),
                      "summary",
                      Map.of("operation", "append", "trino_user", "trino"),
                      "manifest-list",
                      manifestList,
                      "schema-id",
                      0)),
              Map.of(
                  "action",
                  "set-snapshot-ref",
                  "ref-name",
                  "main",
                  "snapshot-id",
                  snapshotId,
                  "type",
                  "branch")));

      String commitBody =
          given()
              .spec(spec)
              .body(commitPayload)
              .when()
              .post("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
              .then()
              .statusCode(200)
              .extract()
              .asString();

      JsonNode responseNode = MAPPER.readTree(commitBody);
      JsonNode commitNode = responseNode.path("metadata");
      Assertions.assertEquals(snapshotId, commitNode.path("current-snapshot-id").asLong());
      Assertions.assertTrue(
          commitNode.path("last-sequence-number").asLong() >= 1L,
          "last-sequence-number should be >= 1");
      Assertions.assertEquals(
          snapshotId,
          commitNode.path("refs").path("main").path("snapshot-id").asLong());
      Assertions.assertTrue(
          responseNode.path("metadata-location").asText("").contains("/metadata/"),
          "metadata-location should be populated");
    } finally {
      deleteTableResource(namespace, table);
      deleteNamespaceResource(namespace);
    }
  }

  @Test
  void duckdbCreateTableSequenceMaterializesMetadata() throws Exception {
    String namespace = uniqueName("it_ns_");
    String table = uniqueName("it_tbl_");
    createNamespace(namespace);

    try {
      given()
          .spec(spec)
          .when()
          .get("/v1/" + CATALOG + "/namespaces/" + namespace)
          .then()
          .statusCode(200);

      given()
          .spec(spec)
          .when()
          .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
          .then()
          .statusCode(404);

      Map<String, Object> createPayload = new LinkedHashMap<>();
      createPayload.put("stage-create", true);
      createPayload.put("name", table);
      createPayload.put(
          "schema",
          Map.of(
              "type",
              "struct",
              "fields",
              List.of(Map.of("name", "event_id", "id", 1, "type", "int", "required", false)),
              "schema-id",
              0));
      createPayload.put("partition-spec", Map.of("spec-id", 0, "fields", List.of()));
      createPayload.put("write-order", Map.of("order-id", 0, "fields", List.of()));
      createPayload.put("properties", Map.of());

      String stageId = "stage-" + UUID.randomUUID();
      given()
          .spec(spec)
          .header("Iceberg-Transaction-Id", stageId)
          .body(createPayload)
          .when()
          .post("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables")
          .then()
          .statusCode(200);

      Map<String, Object> commitPayload = new LinkedHashMap<>();
      commitPayload.put("requirements", List.of(Map.of("type", "assert-create")));
      commitPayload.put(
          "updates",
          List.of(
              Map.of("action", "assign-uuid", "uuid", table),
              Map.of("action", "upgrade-format-version", "format-version", 1),
              Map.of(
                  "action",
                  "add-schema",
                  "last-column-id",
                  1,
                  "schema",
                  Map.of(
                      "type",
                      "struct",
                      "fields",
                      List.of(
                          Map.of("name", "event_id", "id", 1, "type", "int", "required", false)),
                      "schema-id",
                      0,
                      "identifier-field-ids",
                      List.of())),
              Map.of("action", "set-current-schema", "schema-id", 0),
              Map.of("action", "add-spec", "spec", Map.of("spec-id", 0, "fields", List.of())),
              Map.of("action", "set-default-spec", "spec-id", 0),
              Map.of(
                  "action",
                  "add-sort-order",
                  "sort-order",
                  Map.of("order-id", 0, "fields", List.of())),
              Map.of("action", "set-default-sort-order", "sort-order-id", 0),
              Map.of(
                  "action",
                  "set-location",
                  "location",
                  String.format("s3://floecat/%s/%s", namespace, table))));
      commitPayload.put(
          "identifier", Map.of("name", table, "namespace", List.of(namespace)));

      io.restassured.response.Response commitResponse =
          given()
              .spec(spec)
              .header("Iceberg-Transaction-Id", stageId)
              .body(commitPayload)
              .when()
              .post("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
              .then()
              .extract()
              .response();
      String commitBody = commitResponse.asString();
      Assertions.assertEquals(
          200,
          commitResponse.statusCode(),
          () ->
              "DuckDB commit failed: status="
                  + commitResponse.statusCode()
                  + " body="
                  + commitBody);

      JsonNode responseNode = MAPPER.readTree(commitBody);
      JsonNode commitNode = responseNode.path("metadata");
      Assertions.assertTrue(
          responseNode.path("metadata-location").asText("").contains("/metadata/"),
          "metadata-location should be populated");
      Assertions.assertEquals(1, commitNode.path("format-version").asInt());
      Assertions.assertTrue(
          commitNode.path("current-snapshot-id").isMissingNode()
              || commitNode.path("current-snapshot-id").isNull()
              || commitNode.path("current-snapshot-id").asLong() <= 0,
          "current-snapshot-id should be empty on create");
    } finally {
      deleteTableResource(namespace, table);
      deleteNamespaceResource(namespace);
    }
  }

  @Test
  void listsTablesInSeededNamespace() {
    registerTable(
        "iceberg",
        "trino_test",
        "metadata/00002-503f4508-3824-4cb6-bdf1-4bd6bf5a0ade.metadata.json",
        false);

    given()
        .spec(spec)
        .when()
        .get("/v1/" + CATALOG + "/namespaces/iceberg/tables")
        .then()
        .statusCode(200)
        .body("identifiers.name", hasItems("trino_test"));

    given()
        .spec(spec)
        .when()
        .get("/v1/" + CATALOG + "/namespaces/iceberg/tables/trino_test")
        .then()
        .statusCode(200)
        .body("metadata.location", equalTo("s3://yb-iceberg-tpcds/trino_test"))
        .body("metadata.'table-uuid'", notNullValue())
        .body("metadata.'format-version'", equalTo(2));

    if (connectorIntegrationEnabled) {
      Connector connector =
          awaitConnectorForTable("iceberg", "trino_test", Duration.ofSeconds(20));
      Assertions.assertNotNull(connector, "Connector should be created for trino_test");

      withConnectorsClient(
          stub -> {
            stub.syncCapture(
                SyncCaptureRequest.newBuilder()
                    .setConnectorId(connector.getResourceId())
                    .setIncludeStatistics(true)
                    .build());
            stub.triggerReconcile(
                TriggerReconcileRequest.newBuilder()
                    .setConnectorId(connector.getResourceId())
                    .setFullRescan(true)
                    .build());
            return null;
          });

      ResourceId tableId = resolveTableId("iceberg", "trino_test");
      Table tableRecord =
          withTableClient(
              stub ->
                  stub.getTable(GetTableRequest.newBuilder().setTableId(tableId).build())
                      .getTable());
      Assertions.assertNotNull(tableRecord, "Table should be retrievable via table service");
      String metadataLocation = tableRecord.getPropertiesMap().get("metadata-location");
      Assertions.assertNotNull(metadataLocation, "metadata-location property should exist");
      Assertions.assertTrue(
          metadataLocation.startsWith(FIXTURE_METADATA_PREFIX),
          () -> "metadata-location should reside under fixture bucket: " + metadataLocation);
      Assertions.assertTrue(
          tableRecord.hasUpstream() && tableRecord.getUpstream().hasConnectorId(),
          "Upstream connector identifier must be populated");

      ListSnapshotsResponse snapshots =
          withSnapshotClient(
              stub ->
                  stub.listSnapshots(
                      ListSnapshotsRequest.newBuilder().setTableId(tableId).build()));
      List<Long> importedSnapshotIds =
          snapshots.getSnapshotsList().stream()
              .map(Snapshot::getSnapshotId)
              .collect(Collectors.toList());
      Assertions.assertTrue(
          importedSnapshotIds.containsAll(fixtureSnapshotIds),
          "Snapshot catalog should contain fixture snapshot ids");

      SnapshotRef current = SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build();
      TableStats stats = awaitTableStats(tableId, current, Duration.ofSeconds(30));
      Assertions.assertTrue(
          stats.getRowCount() > 0, "Fixture stats should report a positive row count");
      Assertions.assertTrue(
          stats.getTotalSizeBytes() > 0, "Fixture stats should track total bytes");

      ListColumnStatsResponse columnStats =
          awaitColumnStats(tableId, current, Duration.ofSeconds(30));
      Assertions.assertFalse(
          columnStats.getColumnsList().isEmpty(), "Column NDV statistics must be available");
      columnStats
          .getColumnsList()
          .forEach(
              col -> {
                Assertions.assertNotNull(col.getColumnName(), "Column name should be set");
                Assertions.assertFalse(col.getColumnName().isBlank(), "Column name should be set");
                Assertions.assertFalse(
                    col.getLogicalType().isBlank(),
                    () -> "Logical type should be populated for " + col.getColumnName());
                Assertions.assertTrue(
                    col.hasNdv(), "NDV must be present for " + col.getColumnName());
                Assertions.assertTrue(
                    col.getNdv().hasApprox() || col.getNdv().hasExact(),
                    () -> "NDV should contain exact or approx estimate for " + col.getColumnName());
                if (col.getNdv().hasApprox()) {
                  var approx = col.getNdv().getApprox();
                  Assertions.assertTrue(
                      approx.getEstimate() > 0.0d,
                      () -> "Approximate NDV must be positive for " + col.getColumnName());
                  Assertions.assertTrue(
                      approx.getRowsSeen() > 0,
                      () -> "rows-seen must be positive for " + col.getColumnName());
                  Assertions.assertTrue(
                      approx.getRowsTotal() >= approx.getRowsSeen(),
                      () -> "rows-total must be >= rows-seen for " + col.getColumnName());
                  Assertions.assertFalse(
                      approx.getMethod().isBlank(),
                      () -> "NDV method must be set for " + col.getColumnName());
                }
                Assertions.assertFalse(
                    col.getNdv().getSketchesList().isEmpty(),
                    () -> "Sketch metadata must be present for " + col.getColumnName());
                col.getNdv()
                    .getSketchesList()
                    .forEach(
                        sketch ->
                            Assertions.assertFalse(
                                sketch.getType().isBlank(),
                                () -> "Sketch type must be set for column " + col.getColumnName()));
              });
    }
  }

  @Test
  void registersTableAndCommitsPropertiesViaGateway() {
    String namespace = uniqueName("it_ns_");
    String table = uniqueName("it_tbl_");
    createNamespace(namespace);
    try {
      registerTable(namespace, table, METADATA_V3, false);

      Map<String, Object> commitPayload =
          Map.of(
              "requirements",
              List.of(),
              "updates",
              List.of(
                  Map.of("action", "set-properties", "updates", Map.of("owner", "integration"))));

      given()
          .spec(spec)
          .body(commitPayload)
          .when()
          .post("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
          .then()
          .statusCode(200)
          .body("metadata.properties.owner", equalTo("integration"));

      given()
          .spec(spec)
          .when()
          .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
          .then()
          .statusCode(200)
          .body("metadata.properties.owner", equalTo("integration"));
    } finally {
      deleteTableResource(namespace, table);
      deleteNamespaceResource(namespace);
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
          .post("/v1/" + CATALOG + "/namespaces/" + namespace + "/views")
          .then()
          .statusCode(200)
          .body(
              "metadata.location",
              equalTo("floecat://views/" + namespace + "/" + view + "/metadata.json"));

      given()
          .spec(spec)
          .when()
          .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/views/" + view)
          .then()
          .statusCode(200)
          .body("metadata.versions[0].representations[0].sql", equalTo("SELECT 1"))
          .body("metadata.properties.dialect", equalTo("spark"));
    } finally {
      deleteViewResource(namespace, view);
      deleteNamespaceResource(namespace);
    }
  }

  @Test
  void stageCommitMaterializesMetadata() {
    String namespace = NAMESPACE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    String table = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
    TestS3Fixtures.seedStageTable(namespace, table);

    given()
        .spec(spec)
        .body(Map.of("namespace", List.of(namespace)))
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(200);

    String stageId = "stage-" + UUID.randomUUID();
    String tableLocation = "s3://" + STAGE_BUCKET + "/" + namespace + "/" + table;
    String stageMetadataLocation = TestS3Fixtures.stageTableUri(namespace, table, METADATA_V3);

    Map<String, Object> stageRequest = new LinkedHashMap<>();
    stageRequest.put("name", table);
    stageRequest.put("schema", fixtureSchema());
    stageRequest.put("partition-spec", fixturePartitionSpec());
    stageRequest.put("write-order", fixtureWriteOrder());
    Map<String, Object> stageProps = fixtureIoProperties();
    stageProps.put("metadata-location", stageMetadataLocation);
    stageRequest.put("properties", stageProps);
    stageRequest.put("location", tableLocation);
    stageRequest.put("stage-create", true);

    given()
        .spec(spec)
        .header("Iceberg-Transaction-Id", stageId)
        .body(stageRequest)
        .when()
        .post("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables")
        .then()
        .statusCode(200);

    Map<String, Object> commitRequest =
        Map.of(
            "requirements",
            List.of(Map.of("type", "assert-create")),
            "updates",
            List.of());

    String commitMetadataLocation =
        given()
            .spec(spec)
            .header("Iceberg-Transaction-Id", stageId)
            .body(commitRequest)
            .when()
            .post("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
            .then()
            .log()
            .ifValidationFails()
            .statusCode(200)
            .extract()
            .path("'metadata-location'");

    Assertions.assertNotNull(commitMetadataLocation, "metadata-location should be populated");
    String stagePrefix = TestS3Fixtures.stageTableUri(namespace, table, "metadata/");
    Assertions.assertTrue(
        commitMetadataLocation.startsWith(stagePrefix),
        () ->
            "metadata-location should reside under the stage bucket: " + commitMetadataLocation);
    String fileName = commitMetadataLocation.substring(commitMetadataLocation.lastIndexOf('/') + 1);
    Assertions.assertTrue(
        fileName.contains("-"), "materialized metadata file should include a version prefix");

    String persistedLocation =
        ensurePromotedMetadata(fetchTablePropertyMetadataLocation(namespace, table));
    Assertions.assertTrue(
        persistedLocation.startsWith(stagePrefix),
        () ->
            "persisted metadata should be stored under the stage bucket: " + persistedLocation);
    Assertions.assertTrue(
        persistedLocation.endsWith(".metadata.json"),
        "persisted metadata should be an Iceberg metadata file");

    assertFixtureObjectExists(
        commitMetadataLocation,
        "materialized metadata file should exist in the fixture bucket storage");
    assertFixtureObjectExists(
        persistedLocation, "persisted metadata file should exist in the fixture bucket storage");
  }

  private String registerTable(
      String namespace, String table, String metadataLocation, boolean overwrite) {
    Map<String, Object> properties = fixtureIoProperties();
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("name", table);
    payload.put("metadata-location", TestS3Fixtures.bucketUri(metadataLocation));
    payload.put("overwrite", overwrite);
    payload.put("properties", properties);
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
            .log()
            .ifValidationFails()
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
        String.format(
            "metadata/%s/%s/00000-00000000-0000-0000-0000-000000000000.metadata.json",
            namespace, table));
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

  private String fetchTablePropertyMetadataLocation(String namespace, String table) {
    ResourceId tableId = resolveTableId(namespace, table);
    Table tableRecord =
        withTableClient(
            stub ->
                stub.getTable(GetTableRequest.newBuilder().setTableId(tableId).build())
                    .getTable());
    if (tableRecord == null) {
      return null;
    }
    return tableRecord.getPropertiesMap().get("metadata-location");
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

  private String fetchTableMetadataProperty(String namespace, String table, String key) {
    return given()
        .spec(spec)
        .when()
        .get("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
        .then()
        .statusCode(200)
        .extract()
        .path("metadata.properties.'" + key + "'");
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
    JsonNode normalized = MAPPER.readTree(TableMetadataParser.toJson(parsed));
    if (normalized instanceof ObjectNode objectNode) {
      JsonNode propsNode = objectNode.get("properties");
      if (propsNode instanceof ObjectNode props) {
        props.remove("format-version");
        props.remove("metadata-location");
        props.remove("metadata_location");
        String propLocation = props.path("location").asText(null);
        if (propLocation != null && propLocation.endsWith("/")) {
          props.put("location", propLocation.substring(0, propLocation.length() - 1));
        }
      }
    }
    return normalized;
  }

  private String ensurePromotedMetadata(String location) {
    if (location == null || location.isBlank()) {
      return location;
    }
    if (location.startsWith("s3://")) {
      return location;
    }
    if (location.startsWith(FIXTURE_METADATA_PREFIX)) {
      return location;
    }
    return TestS3Fixtures.bucketUri(location);
  }

  private static String uniqueName(String prefix) {
    return prefix + UUID.randomUUID().toString().replace("-", "");
  }

  private void createNamespace(String namespace) {
    Map<String, Object> payload = Map.of("namespace", List.of(namespace));
    given()
        .spec(spec)
        .body(payload)
        .when()
        .post("/v1/" + CATALOG + "/namespaces")
        .then()
        .statusCode(200)
        .body("namespace[0]", equalTo(namespace));
  }

  private void deleteNamespaceResource(String namespace) {
    given()
        .spec(spec)
        .when()
        .delete("/v1/" + CATALOG + "/namespaces/" + namespace)
        .then()
        .statusCode(anyOf(is(204), is(404)));
  }

  private void deleteTableResource(String namespace, String table) {
    given()
        .spec(spec)
        .when()
        .delete("/v1/" + CATALOG + "/namespaces/" + namespace + "/tables/" + table)
        .then()
        .statusCode(anyOf(is(204), is(404)));
  }

  private void deleteViewResource(String namespace, String view) {
    given()
        .spec(spec)
        .when()
        .delete("/v1/" + CATALOG + "/namespaces/" + namespace + "/views/" + view)
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
    Map<String, Object> version = new LinkedHashMap<>();
    version.put("version-id", 1);
    version.put("schema-id", 1);
    version.put("summary", Map.of("operation", "create"));
    version.put("representations", List.of(representation));
    version.put("default-namespace", List.of(namespace));

    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("name", view);
    payload.put("schema", schema);
    payload.put("view-version", version);
    payload.put("properties", Map.of("dialect", "spark"));
    return payload;
  }

  private Map<String, Object> fixtureIoProperties() {
    Map<String, Object> props = new LinkedHashMap<>();
    TestS3Fixtures.fileIoProperties(TEST_S3_ROOT.toAbsolutePath().toString()).forEach(props::put);
    int formatVersion = fixtureMetadata == null ? 2 : fixtureMetadata.formatVersion();
    props.put("format-version", Integer.toString(formatVersion));
    if (fixtureMetadata != null && fixtureMetadata.lastUpdatedMillis() > 0) {
      props.put("last-updated-ms", Long.toString(fixtureMetadata.lastUpdatedMillis()));
    }
    return props;
  }

  private void assertFixtureObjectExists(String location, String message) {
    if (USE_AWS_FIXTURES) {
      Assertions.assertTrue(
          TestS3Fixtures.objectExistsInS3(location), message + " (" + location + ")");
    } else {
      Path local = localS3Path(location);
      Assertions.assertTrue(Files.exists(local), message + " (" + local + ")");
    }
  }

  private void assertFixtureObjectMissing(String location, String message) {
    if (USE_AWS_FIXTURES) {
      Assertions.assertFalse(
          TestS3Fixtures.objectExistsInS3(location), message + " (" + location + ")");
    } else {
      Path local = localS3Path(location);
      Assertions.assertTrue(Files.notExists(local), message + " (" + local + ")");
    }
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
    Map<String, Object> request = new LinkedHashMap<>();
    request.put("name", table);
    request.put("schema", fixtureSchema());
    request.put("partition-spec", fixturePartitionSpec());
    request.put("write-order", fixtureWriteOrder());
    Map<String, Object> props = fixtureIoProperties();
    props.put("metadata-location", stagedMetadataLocation);
    request.put("properties", props);
    request.put("location", String.format("s3://%s/%s/%s", STAGE_BUCKET, namespace, table));
    request.put("stage-create", true);
    return request;
  }

  private Map<String, Object> fixtureSchema() {
    if (fixtureSchemaJson == null || fixtureSchemaJson.isBlank()) {
      throw new IllegalStateException("Fixture schema JSON is required");
    }
    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> schema =
          MAPPER.readValue(fixtureSchemaJson, Map.class);
      if (fixtureSchemaId != null) {
        schema.putIfAbsent("schema-id", fixtureSchemaId);
      }
      if (fixtureLastColumnId != null) {
        schema.putIfAbsent("last-column-id", fixtureLastColumnId);
      }
      return schema;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to parse fixture schema JSON", e);
    }
  }

  private Map<String, Object> fixturePartitionSpec() {
    if (fixtureMetadata == null) {
      throw new IllegalStateException("Fixture metadata is required");
    }
    int targetId = fixtureDefaultSpecId == null ? fixtureMetadata.defaultSpecId() : fixtureDefaultSpecId;
    var spec =
        fixtureMetadata.specs().stream()
            .filter(s -> s.specId() == targetId)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Default partition spec not found"));
    List<Map<String, Object>> fields =
        spec.fields().stream()
            .map(
                field -> {
                  Map<String, Object> entry = new LinkedHashMap<>();
                  entry.put("name", field.name());
                  entry.put("field-id", field.fieldId());
                  entry.put("source-id", field.sourceId());
                  entry.put("transform", field.transform().toString());
                  return entry;
                })
            .collect(Collectors.toList());
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("spec-id", spec.specId());
    payload.put("fields", fields);
    return payload;
  }

  private Map<String, Object> fixtureWriteOrder() {
    if (fixtureMetadata == null) {
      throw new IllegalStateException("Fixture metadata is required");
    }
    int targetId =
        fixtureDefaultSortOrderId == null
            ? fixtureMetadata.defaultSortOrderId()
            : fixtureDefaultSortOrderId;
    var order =
        fixtureMetadata.sortOrders().stream()
            .filter(o -> o.orderId() == targetId)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Default sort order not found"));
    List<Map<String, Object>> fields =
        order.fields().stream()
            .map(
                field -> {
                  Map<String, Object> entry = new LinkedHashMap<>();
                  entry.put("source-id", field.sourceId());
                  entry.put("transform", field.transform().toString());
                  entry.put("direction", field.direction().name().toLowerCase(Locale.ROOT));
                  entry.put(
                      "null-order",
                      field.nullOrder().name().toLowerCase(Locale.ROOT).replace('_', '-'));
                  return entry;
                })
            .collect(Collectors.toList());
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("order-id", order.orderId());
    payload.put("fields", fields);
    return payload;
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

  private <S extends AbstractBlockingStub<S>, T> T withServiceClient(
      String serviceName, Function<ManagedChannel, S> stubFactory, Function<S, T> fn) {
    parseUpstreamTarget();
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(upstreamHost, serviceGrpcPort).usePlaintext().build();
    try {
      Metadata headers = new Metadata();
      headers.put(
          Metadata.Key.of(AUTH_HEADER_NAME, Metadata.ASCII_STRING_MARSHALLER), DEFAULT_AUTH_HEADER);
      S stub =
          stubFactory
              .apply(channel)
              .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
      return fn.apply(stub);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.UNAVAILABLE) {
        Assumptions.assumeTrue(
            false, serviceName + " gRPC endpoint unavailable (" + e.getMessage() + ")");
        return null;
      }
      throw e;
    } finally {
      channel.shutdownNow();
    }
  }

  private <T> T withConnectorsClient(Function<ConnectorsGrpc.ConnectorsBlockingStub, T> fn) {
    return withServiceClient("Connectors", ConnectorsGrpc::newBlockingStub, fn);
  }

  private <T> T withDirectoryClient(
      Function<DirectoryServiceGrpc.DirectoryServiceBlockingStub, T> fn) {
    return withServiceClient("Directory", DirectoryServiceGrpc::newBlockingStub, fn);
  }

  private static String resolveSeedAccountId() {
    if (seedAccountId != null && !seedAccountId.isBlank()) {
      return seedAccountId;
    }
    parseUpstreamTarget();
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(upstreamHost, serviceGrpcPort).usePlaintext().build();
    try {
      AccountServiceGrpc.AccountServiceBlockingStub accounts =
          AccountServiceGrpc.newBlockingStub(channel);
      ListAccountsResponse response = accounts.listAccounts(ListAccountsRequest.getDefaultInstance());
      List<Account> accountsList = response.getAccountsList();
      if (accountsList.isEmpty()) {
        throw new IllegalStateException("No accounts returned by service");
      }
      Optional<Account> named =
          accountsList.stream()
              .filter(account -> DEFAULT_ACCOUNT_NAME.equals(account.getDisplayName()))
              .findFirst();
      Account account = named.orElseGet(() -> accountsList.get(0));
      seedAccountId = account.getResourceId().getId();
      return seedAccountId;
    } finally {
      channel.shutdownNow();
    }
  }

  private <T> T withTableClient(Function<TableServiceGrpc.TableServiceBlockingStub, T> fn) {
    return withServiceClient("Table", TableServiceGrpc::newBlockingStub, fn);
  }

  private <T> T withSnapshotClient(
      Function<SnapshotServiceGrpc.SnapshotServiceBlockingStub, T> fn) {
    return withServiceClient("Snapshot", SnapshotServiceGrpc::newBlockingStub, fn);
  }

  private <T> T withStatisticsClient(
      Function<TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub, T> fn) {
    return withServiceClient("Statistics", TableStatisticsServiceGrpc::newBlockingStub, fn);
  }

  private ResourceId resolveTableId(String namespace, String table) {
    NameRef ref =
        NameRef.newBuilder()
            .setCatalog(CATALOG)
            .addAllPath(namespaceSegments(namespace))
            .setName(table)
            .build();
    ResolveTableResponse response =
        withDirectoryClient(
            stub -> stub.resolveTable(ResolveTableRequest.newBuilder().setRef(ref).build()));
    return response.getResourceId();
  }

  private static List<String> namespaceSegments(String namespace) {
    if (namespace == null || namespace.isBlank()) {
      return List.of();
    }
    return Arrays.stream(namespace.split("\\."))
        .filter(part -> part != null && !part.isBlank())
        .collect(Collectors.toList());
  }

  private TableStats awaitTableStats(
      ResourceId tableId, SnapshotRef snapshotRef, Duration timeout) {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      GetTableStatsResponse response =
          withStatisticsClient(
              stub ->
                  stub.getTableStats(
                      GetTableStatsRequest.newBuilder()
                          .setTableId(tableId)
                          .setSnapshot(snapshotRef)
                          .build()));
      if (response.hasStats() && response.getStats().getRowCount() > 0) {
        return response.getStats();
      }
      try {
        TimeUnit.MILLISECONDS.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    Assertions.fail("Timed out waiting for table stats for " + tableId.getId());
    return TableStats.getDefaultInstance();
  }

  private ListColumnStatsResponse awaitColumnStats(
      ResourceId tableId, SnapshotRef snapshotRef, Duration timeout) {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      ListColumnStatsResponse response =
          withStatisticsClient(
              stub ->
                  stub.listColumnStats(
                      ListColumnStatsRequest.newBuilder()
                          .setTableId(tableId)
                          .setSnapshot(snapshotRef)
                          .setPage(PageRequest.newBuilder().setPageSize(200).build())
                          .build()));
      if (!response.getColumnsList().isEmpty()) {
        return response;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    Assertions.fail("Timed out waiting for column stats for " + tableId.getId());
    return ListColumnStatsResponse.getDefaultInstance();
  }

  private static void reseedFixtureBucket() {
    try {
      TestS3Fixtures.seedFixtures();
    } catch (RuntimeException e) {
      System.err.printf("Failed to reseed fixture bucket after purge test: %s%n", e.getMessage());
    }
  }
}
