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

package ai.floedb.floecat.gateway.iceberg.rest.table.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.table.SnapshotUpdateService;
import ai.floedb.floecat.gateway.iceberg.rest.table.TablePropertyService;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableUpdatePlannerTest {
  private final CommitUpdateCompiler compiler = new CommitUpdateCompiler();
  private final TablePropertyService propertyService = new TablePropertyService();
  private final SnapshotUpdateService snapshotUpdateService = mock(SnapshotUpdateService.class);
  private final CommitRequestValidationHelper validationHelper =
      new CommitRequestValidationHelper();

  @BeforeEach
  void setUp() {
    compiler.tablePropertyService = propertyService;
    compiler.snapshotUpdateService = snapshotUpdateService;
    compiler.validationHelper = validationHelper;
    compiler.mapper = new ObjectMapper();
    when(snapshotUpdateService.validateSnapshotUpdates(any())).thenReturn(null);
  }

  @Test
  void compileMergesLocationPropertiesAndSchema() {
    Table existing =
        Table.newBuilder()
            .setUpstream(UpstreamRef.newBuilder().setUri("s3://warehouse/old").build())
            .putProperties("existing", "true")
            .build();
    TableRequests.Commit request =
        new TableRequests.Commit(
            List.of(),
            List.of(
                Map.of("action", "set-location", "location", "s3://warehouse/new"),
                Map.of("action", "set-properties", "updates", Map.of("retention", "7d")),
                Map.of(
                    "action",
                    "add-schema",
                    "schema",
                    Map.of("schema-id", 0, "type", "struct", "fields", List.of()))));

    CommitUpdateCompiler.CompileResult result = compiler.compile(request, () -> existing, true);

    assertFalse(result.hasError());
    assertEquals("7d", result.patch().spec().getPropertiesOrThrow("retention"));
    assertEquals("s3://warehouse/new", result.patch().spec().getUpstream().getUri());
    assertTrue(result.patch().mask().getPathsList().contains("properties"));
    assertTrue(result.patch().mask().getPathsList().contains("upstream.uri"));
    assertTrue(result.patch().mask().getPathsList().contains("schema_json"));
  }

  @Test
  void compileReturnsValidationErrorForUnsupportedAction() {
    CommitUpdateCompiler.CompileResult result =
        compiler.compile(
            new TableRequests.Commit(List.of(), List.of(Map.of("action", "drop"))),
            Table::getDefaultInstance,
            true);

    assertTrue(result.hasError());
    assertNotNull(result.error());
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), result.error().getStatus());
  }

  @Test
  void compileReturnsValidationErrorForMissingAction() {
    CommitUpdateCompiler.CompileResult result =
        compiler.compile(
            new TableRequests.Commit(List.of(), List.of(Map.of("value", "x"))),
            Table::getDefaultInstance,
            true);

    assertTrue(result.hasError());
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), result.error().getStatus());
  }

  @Test
  void compileReturnsSnapshotValidationErrors() {
    Response snapshotError = Response.status(Response.Status.CONFLICT).build();
    when(snapshotUpdateService.validateSnapshotUpdates(any())).thenReturn(snapshotError);

    CommitUpdateCompiler.CompileResult result =
        compiler.compile(
            new TableRequests.Commit(
                List.of(), List.of(Map.of("action", "add-snapshot", "snapshot", Map.of()))),
            Table::getDefaultInstance,
            true);

    assertTrue(result.hasError());
    assertEquals(Response.Status.CONFLICT.getStatusCode(), result.error().getStatus());
  }

  @Test
  void compileStripsFileIoPropertiesFromMergedProperties() {
    CommitUpdateCompiler.CompileResult result =
        compiler.compile(
            new TableRequests.Commit(
                List.of(),
                List.of(
                    Map.of(
                        "action",
                        "set-properties",
                        "updates",
                        Map.of("owner", "alice", "io-impl", "custom.FileIO")))),
            Table::getDefaultInstance,
            true);

    assertFalse(result.hasError());
    assertEquals("alice", result.patch().spec().getPropertiesOrThrow("owner"));
    assertTrue(!result.patch().spec().containsProperties("io-impl"));
  }

  @Test
  void parsedCommitBuildsTypedUpdateEntriesOnce() {
    ParsedCommit commit =
        ParsedCommit.from(
            new TableRequests.Commit(
                List.of(),
                List.of(
                    Map.of("action", "set-properties", "updates", Map.of("owner", "alice")),
                    Map.of("action", "add-snapshot", "snapshot", Map.of("snapshot-id", 7L)))));

    assertEquals(2, commit.updateEntries().size());
    assertEquals(
        ai.floedb.floecat.gateway.iceberg.rest.support.CommitUpdateInspector.UpdateAction
            .SET_PROPERTIES,
        commit.updateEntries().get(0).action());
    assertEquals(
        ai.floedb.floecat.gateway.iceberg.rest.support.CommitUpdateInspector.UpdateAction
            .ADD_SNAPSHOT,
        commit.updateEntries().get(1).action());
    assertEquals(1, commit.addedSnapshots().size());
  }
}
