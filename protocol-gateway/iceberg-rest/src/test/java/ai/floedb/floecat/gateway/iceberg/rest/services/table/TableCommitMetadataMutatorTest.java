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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TableCommitMetadataMutatorTest {

  private final TableCommitMetadataMutator mutator = new TableCommitMetadataMutator();

  @Test
  void applyUpsertsDefinitionEntriesById() {
    TableMetadataView metadata =
        new TableMetadataView(
            2,
            "tbl-uuid",
            "s3://floecat/iceberg/duckdb_mutation_smoke",
            "s3://floecat/iceberg/duckdb_mutation_smoke/metadata/00000-old.metadata.json",
            1L,
            Map.of("format-version", "2"),
            2,
            0,
            0,
            0,
            0,
            null,
            0L,
            List.of(Map.of("schema-id", 0, "type", "struct", "fields", List.of())),
            List.of(Map.of("spec-id", 0, "fields", List.of())),
            List.of(Map.of("order-id", 0, "fields", List.of())),
            Map.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    TableRequests.Commit request =
        new TableRequests.Commit(
            List.of(),
            List.of(
                Map.of(
                    "action",
                    "add-schema",
                    "last-column-id",
                    2,
                    "schema",
                    Map.of(
                        "schema-id",
                        0,
                        "type",
                        "struct",
                        "fields",
                        List.of(Map.of("id", 1, "name", "id", "required", false, "type", "int")))),
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
                    "s3://floecat/iceberg/duckdb_mutation_smoke")));

    TableMetadataView merged = mutator.apply(metadata, request);

    assertEquals(1, merged.schemas().size(), "schema-id should be unique");
    assertEquals(1, merged.partitionSpecs().size(), "spec-id should be unique");
    assertEquals(1, merged.sortOrders().size(), "order-id should be unique");
    assertEquals(
        "s3://floecat/iceberg/duckdb_mutation_smoke",
        merged.location(),
        "set-location update should populate metadata location field");
    assertEquals("0", merged.properties().get("current-schema-id"));
  }

  @Test
  void applyPrefersCommittedCurrentSnapshotOverStaleMetadata() {
    long staleSnapshotId = 7105637631842704244L;
    long newSnapshotId = 1772624629860L;
    TableMetadataView metadata =
        new TableMetadataView(
            2,
            "tbl-uuid",
            "s3://floecat/iceberg/orders",
            "s3://floecat/iceberg/orders/metadata/00000-old.metadata.json",
            1L,
            Map.of("current-snapshot-id", Long.toString(staleSnapshotId), "format-version", "2"),
            2,
            0,
            0,
            0,
            0,
            staleSnapshotId,
            0L,
            List.of(Map.of("schema-id", 0, "type", "struct", "fields", List.of())),
            List.of(Map.of("spec-id", 0, "fields", List.of())),
            List.of(Map.of("order-id", 0, "fields", List.of())),
            Map.of("main", Map.of("snapshot-id", staleSnapshotId, "type", "branch")),
            List.of(),
            List.of(),
            List.of(Map.of("snapshot-id", staleSnapshotId, "timestamp-ms", 1L)),
            List.of(),
            List.of(Map.of("snapshot-id", staleSnapshotId, "timestamp-ms", 1L)));

    TableRequests.Commit request =
        new TableRequests.Commit(
            List.of(),
            List.of(
                Map.of(
                    "action",
                    "add-snapshot",
                    "snapshot",
                    Map.of(
                        "snapshot-id",
                        newSnapshotId,
                        "timestamp-ms",
                        1772624629860L,
                        "sequence-number",
                        1L,
                        "summary",
                        Map.of("operation", "append"))),
                Map.of(
                    "action",
                    "set-snapshot-ref",
                    "ref-name",
                    "main",
                    "snapshot-id",
                    newSnapshotId,
                    "type",
                    "branch")));

    TableMetadataView merged = mutator.apply(metadata, request);

    assertEquals(newSnapshotId, merged.currentSnapshotId());
    assertEquals(Long.toString(newSnapshotId), merged.properties().get("current-snapshot-id"));
    assertEquals(
        newSnapshotId,
        ((Number) ((Map<?, ?>) merged.refs().get("main")).get("snapshot-id")).longValue());
  }

  @Test
  void applyResolvesSetLastSentinelIds() {
    TableMetadataView metadata =
        new TableMetadataView(
            2,
            "tbl-uuid",
            "s3://floecat/iceberg/orders",
            "s3://floecat/iceberg/orders/metadata/00000-old.metadata.json",
            1L,
            Map.of("format-version", "2"),
            0,
            0,
            0,
            0,
            0,
            null,
            0L,
            List.of(Map.of("schema-id", 0, "type", "struct", "fields", List.of())),
            List.of(Map.of("spec-id", 0, "fields", List.of())),
            List.of(Map.of("order-id", 0, "fields", List.of())),
            Map.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    TableRequests.Commit request =
        new TableRequests.Commit(
            List.of(),
            List.of(
                Map.of(
                    "action",
                    "add-schema",
                    "schema",
                    Map.of(
                        "schema-id",
                        1,
                        "type",
                        "struct",
                        "fields",
                        List.of(
                            Map.of("id", 1, "name", "id", "required", false, "type", "int"),
                            Map.of("id", 2, "name", "name", "required", false, "type", "string")))),
                Map.of("action", "set-current-schema", "schema-id", -1),
                Map.of("action", "add-spec", "spec", Map.of("spec-id", 3, "fields", List.of())),
                Map.of("action", "set-default-spec", "spec-id", -1),
                Map.of(
                    "action",
                    "add-sort-order",
                    "sort-order",
                    Map.of("order-id", 5, "fields", List.of())),
                Map.of("action", "set-default-sort-order", "sort-order-id", -1)));

    TableMetadataView merged = mutator.apply(metadata, request);

    assertEquals(1, merged.currentSchemaId());
    assertEquals(3, merged.defaultSpecId());
    assertEquals(5, merged.defaultSortOrderId());
    assertEquals("1", merged.properties().get("current-schema-id"));
    assertEquals("2", merged.properties().get("last-column-id"));
  }

  @Test
  void applyDoesNotPreserveStaleHigherLastSequenceNumberFromExistingMetadata() {
    long newSnapshotId = 1772624629860L;
    TableMetadataView metadata =
        new TableMetadataView(
            2,
            "tbl-uuid",
            "s3://floecat/iceberg/orders",
            "s3://floecat/iceberg/orders/metadata/00000-old.metadata.json",
            1L,
            Map.of("last-sequence-number", "999", "format-version", "2"),
            2,
            0,
            0,
            0,
            0,
            null,
            999L,
            List.of(Map.of("schema-id", 0, "type", "struct", "fields", List.of())),
            List.of(Map.of("spec-id", 0, "fields", List.of())),
            List.of(Map.of("order-id", 0, "fields", List.of())),
            Map.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    TableRequests.Commit request =
        new TableRequests.Commit(
            List.of(),
            List.of(
                Map.of(
                    "action",
                    "add-snapshot",
                    "snapshot",
                    Map.of(
                        "snapshot-id",
                        newSnapshotId,
                        "timestamp-ms",
                        1772624629860L,
                        "sequence-number",
                        7L,
                        "summary",
                        Map.of("operation", "append"))),
                Map.of(
                    "action",
                    "set-snapshot-ref",
                    "ref-name",
                    "main",
                    "snapshot-id",
                    newSnapshotId,
                    "type",
                    "branch")));

    TableMetadataView merged = mutator.apply(metadata, request);

    assertEquals(7L, merged.lastSequenceNumber());
    assertEquals("7", merged.properties().get("last-sequence-number"));
  }

  @Test
  void applyRemovesSnapshotsListedInRemoveSnapshotsUpdate() {
    long keepSnapshotId = 1001L;
    long removedSnapshotId = 1002L;
    TableMetadataView metadata =
        new TableMetadataView(
            2,
            "tbl-uuid",
            "s3://floecat/iceberg/orders",
            "s3://floecat/iceberg/orders/metadata/00000-old.metadata.json",
            1L,
            Map.of("format-version", "2", "current-snapshot-id", Long.toString(keepSnapshotId)),
            0,
            0,
            0,
            0,
            0,
            keepSnapshotId,
            0L,
            List.of(Map.of("schema-id", 0, "type", "struct", "fields", List.of())),
            List.of(Map.of("spec-id", 0, "fields", List.of())),
            List.of(Map.of("order-id", 0, "fields", List.of())),
            Map.of("main", Map.of("snapshot-id", keepSnapshotId, "type", "branch")),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(
                Map.of("snapshot-id", keepSnapshotId, "timestamp-ms", 1L),
                Map.of("snapshot-id", removedSnapshotId, "timestamp-ms", 2L)));

    TableRequests.Commit request =
        new TableRequests.Commit(
            List.of(),
            List.of(
                Map.of("action", "remove-snapshots", "snapshot-ids", List.of(removedSnapshotId))));

    TableMetadataView merged = mutator.apply(metadata, request);

    assertEquals(
        List.of(Map.of("snapshot-id", keepSnapshotId, "timestamp-ms", 1L)), merged.snapshots());
  }
}
