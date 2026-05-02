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

package ai.floedb.floecat.connector.iceberg.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.connector.spi.FloecatConnector.FileGroupCaptureResult;
import ai.floedb.floecat.gateway.iceberg.rest.common.TestS3Fixtures;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class IcebergConnectorIssuesTest {

  @Test
  void resolvesIncludedFieldIdsFromSnapshotSchemaRatherThanCurrentTableSchema() {
    Schema snapshotSchema =
        new Schema(
            10,
            List.of(
                Types.NestedField.optional(1, "old_col", Types.IntegerType.get()),
                Types.NestedField.optional(2, "shared_col", Types.StringType.get())));
    Schema currentSchema =
        new Schema(
            20,
            List.of(
                Types.NestedField.optional(3, "new_col", Types.IntegerType.get()),
                Types.NestedField.optional(2, "shared_col", Types.StringType.get())));

    Table table =
        (Table)
            Proxy.newProxyInstance(
                Table.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, args) ->
                    switch (method.getName()) {
                      case "schema" -> currentSchema;
                      case "schemas" -> Map.of(10, snapshotSchema, 20, currentSchema);
                      default -> throw new UnsupportedOperationException(method.getName());
                    });
    Snapshot snapshot =
        (Snapshot)
            Proxy.newProxyInstance(
                Snapshot.class.getClassLoader(),
                new Class<?>[] {Snapshot.class},
                (proxy, method, args) ->
                    switch (method.getName()) {
                      case "schemaId" -> 10;
                      default -> throw new UnsupportedOperationException(method.getName());
                    });

    Schema resolved = IcebergConnector.schemaForSnapshot(table, snapshot);

    assertEquals(snapshotSchema, resolved);
    assertEquals(Set.of(1, 2), IcebergConnector.resolveIncludedFieldIds(resolved, Set.of()));
    assertEquals(
        Set.of(1, 2),
        IcebergConnector.resolveIncludedFieldIds(resolved, Set.of("old_col", "shared_col")));
  }

  @Test
  void skipsMalformedBoundsFromTpcdsSfoneFixture() {
    TestS3Fixtures.seedTpcdsSfoneFixtureOnce();

    var props = new HashMap<String, String>();
    props.putAll(
        TestS3Fixtures.fileIoProperties(
            TestS3Fixtures.bucketPath().getParent().toAbsolutePath().toString()));
    props.put("external.namespace", "tpcds_sfone");
    props.put("external.table-name", "catalog_returns");
    props.put("stats.ndv.enabled", "false");
    props.put("iceberg.source", "filesystem");
    String metadataLocation =
        TestS3Fixtures.tpcdsSfoneUri(
            "floe_test.db/tpcds_sfone/catalog_returns/metadata/00002-fb845f92-5b90-4bd1-8670-3cab11eb68b1.metadata.json");

    try (FloecatConnector connector =
        IcebergConnectorFactory.create(
            metadataLocation, props, "none", new HashMap<>(), new HashMap<>())) {
      List<FloecatConnector.SnapshotBundle> snapshots =
          assertDoesNotThrow(
              () ->
                  connector.enumerateSnapshots(
                      "tpcds_sfone",
                      "catalog_returns",
                      ResourceId.newBuilder()
                          .setAccountId("test-account")
                          .setId("test-table")
                          .setKind(ResourceKind.RK_TABLE)
                          .build(),
                      FloecatConnector.SnapshotEnumerationOptions.full(true)));

      assertNotNull(snapshots);
      assertFalse(snapshots.isEmpty(), "expected snapshots from tpcds_sfone fixture");

      long snapshotId = snapshots.get(0).snapshotId();
      var targetStats =
          assertDoesNotThrow(
              () ->
                  connector.captureSnapshotTargetStats(
                      "tpcds_sfone",
                      "catalog_returns",
                      ResourceId.newBuilder()
                          .setAccountId("test-account")
                          .setId("test-table")
                          .setKind(ResourceKind.RK_TABLE)
                          .build(),
                      snapshotId,
                      Set.of()));
      assertTrue(
          targetStats.stream().anyMatch(r -> r.hasTable()),
          "expected table stats to still be produced");
      assertTrue(
          targetStats.stream()
              .filter(r -> r.hasFile())
              .anyMatch(r -> !r.getFile().getColumnsList().isEmpty()),
          "expected file-level stats to still be produced");
    }
  }

  @Test
  void filesystemConnectorInfersLatestMetadataFromBaseLocation() {
    TestS3Fixtures.seedTpcdsSfoneFixtureOnce();

    var props = new HashMap<String, String>();
    props.putAll(
        TestS3Fixtures.fileIoProperties(
            TestS3Fixtures.bucketPath().getParent().toAbsolutePath().toString()));
    props.put("external.namespace", "tpcds_sfone");
    props.put("external.table-name", "catalog_returns");
    props.put("stats.ndv.enabled", "false");
    props.put("iceberg.source", "filesystem");
    String tableLocation = TestS3Fixtures.tpcdsSfoneUri("floe_test.db/tpcds_sfone/catalog_returns");

    try (FloecatConnector connector =
        IcebergConnectorFactory.create(
            tableLocation, props, "none", new HashMap<>(), new HashMap<>())) {
      List<FloecatConnector.SnapshotBundle> snapshots =
          assertDoesNotThrow(
              () ->
                  connector.enumerateSnapshots(
                      "tpcds_sfone",
                      "catalog_returns",
                      ResourceId.newBuilder()
                          .setAccountId("test-account")
                          .setId("test-table")
                          .setKind(ResourceKind.RK_TABLE)
                          .build(),
                      FloecatConnector.SnapshotEnumerationOptions.full(true)));

      assertNotNull(snapshots);
      assertFalse(snapshots.isEmpty(), "expected snapshots from base table location");
    }
  }

  @Test
  void simpleFixtureFileGroupCaptureRetainsColumnMetadata() {
    TestS3Fixtures.seedFixturesOnce();

    var props = new HashMap<String, String>();
    props.putAll(
        TestS3Fixtures.fileIoProperties(
            TestS3Fixtures.bucketPath().getParent().toAbsolutePath().toString()));
    props.put("external.namespace", "iceberg");
    props.put("external.table-name", "trino_test");
    props.put("stats.ndv.enabled", "false");
    props.put("iceberg.source", "filesystem");
    String metadataLocation =
        TestS3Fixtures.bucketUri(
            "metadata/00002-503f4508-3824-4cb6-bdf1-4bd6bf5a0ade.metadata.json");
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("test-account")
            .setId("test-table")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    try (FloecatConnector connector =
        IcebergConnectorFactory.create(
            metadataLocation, props, "none", new HashMap<>(), new HashMap<>())) {
      long snapshotId =
          connector
              .enumerateSnapshots(
                  "iceberg",
                  "trino_test",
                  tableId,
                  FloecatConnector.SnapshotEnumerationOptions.full(true))
              .stream()
              .max(
                  java.util.Comparator.comparingLong(
                      FloecatConnector.SnapshotBundle::sequenceNumber))
              .orElseThrow()
              .snapshotId();

      var plan =
          connector.planSnapshotFiles("iceberg", "trino_test", tableId, snapshotId).orElseThrow();
      assertFalse(
          plan.deleteFiles().isEmpty(),
          "expected current fixture snapshot to include delete files");
      Set<String> plannedFilePaths =
          java.util.stream.Stream.concat(plan.dataFiles().stream(), plan.deleteFiles().stream())
              .map(FloecatConnector.SnapshotFileEntry::filePath)
              .collect(java.util.stream.Collectors.toSet());

      FileGroupCaptureResult captured =
          connector.capturePlannedFileGroup(
              "iceberg",
              "trino_test",
              tableId,
              snapshotId,
              plannedFilePaths,
              Set.of(),
              Set.of(
                  FloecatConnector.StatsTargetKind.TABLE,
                  FloecatConnector.StatsTargetKind.COLUMN,
                  FloecatConnector.StatsTargetKind.FILE),
              false);

      assertTrue(
          captured.statsRecords().stream()
              .filter(TargetStatsRecord::hasScalar)
              .allMatch(
                  record ->
                      !record.getScalar().getDisplayName().isBlank()
                          && !record.getScalar().getLogicalType().isBlank()),
          () -> "scalar target stats should preserve name/type: " + captured.statsRecords());

      assertTrue(
          captured.statsRecords().stream()
              .filter(TargetStatsRecord::hasFile)
              .flatMap(record -> record.getFile().getColumnsList().stream())
              .allMatch(
                  fileColumn ->
                      !fileColumn.getScalar().getDisplayName().isBlank()
                          && !fileColumn.getScalar().getLogicalType().isBlank()),
          () -> "file-column stats should preserve name/type: " + captured.statsRecords());
    }
  }
}
