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

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.CanonicalTableMetadataService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.junit.jupiter.api.Test;

class CanonicalCommitMetadataServiceMutationTest {

  private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
  private final CanonicalTableMetadataService canonicalTableMetadataService =
      new CanonicalTableMetadataService();
  private final CanonicalCommitMetadataService service = new CanonicalCommitMetadataService();

  CanonicalCommitMetadataServiceMutationTest() {
    canonicalTableMetadataService.setMapper(mapper);
    service.mapper = mapper;
  }

  @Test
  void applyCommitMetadataUpdatesUpsertsDefinitionEntriesById() {
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
                        1,
                        "type",
                        "struct",
                        "fields",
                        List.of(Map.of("id", 1, "name", "id", "required", false, "type", "int")))),
                Map.of("action", "set-current-schema", "schema-id", 1),
                Map.of(
                    "action",
                    "set-location",
                    "location",
                    "s3://floecat/iceberg/duckdb_mutation_smoke")));

    TableMetadataView merged = apply(metadata, request);

    assertEquals(2, merged.schemas().size(), "new schema should be appended canonically");
    assertEquals(
        "s3://floecat/iceberg/duckdb_mutation_smoke",
        merged.location(),
        "set-location update should populate metadata location field");
    assertEquals(1, merged.currentSchemaId());
  }

  @Test
  void applyCommitMetadataUpdatesResolvesSetLastSentinelIds() {
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
                Map.of("action", "set-current-schema", "schema-id", -1)));

    TableMetadataView merged = apply(metadata, request);

    assertEquals(1, merged.currentSchemaId());
  }

  private TableMetadataView apply(TableMetadataView metadata, TableRequests.Commit request) {
    TableMetadata canonical =
        canonicalTableMetadataService.toTableMetadata(metadata, metadata.metadataLocation());
    TableMetadata updated =
        service.applyCommitUpdates(canonical, Table.getDefaultInstance(), request);
    return canonicalTableMetadataService.toTableMetadataView(updated, metadata.metadataLocation());
  }
}
