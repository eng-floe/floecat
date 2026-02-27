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

import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CommitResponseBuilderTest {

  private final CommitResponseBuilder builder = new CommitResponseBuilder();

  @Test
  void mergeTableDefinitionUpdatesUpsertsById() {
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

    CommitTableResponseDto response =
        new CommitTableResponseDto(metadata.metadataLocation(), metadata);
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

    CommitTableResponseDto merged = builder.mergeTableDefinitionUpdates(response, request);

    assertEquals(1, merged.metadata().schemas().size(), "schema-id should be unique");
    assertEquals(1, merged.metadata().partitionSpecs().size(), "spec-id should be unique");
    assertEquals(1, merged.metadata().sortOrders().size(), "order-id should be unique");
    assertEquals(
        "s3://floecat/iceberg/duckdb_mutation_smoke",
        merged.metadata().location(),
        "set-location update should populate metadata location field");
    assertEquals("0", merged.metadata().properties().get("current-schema-id"));
  }
}
