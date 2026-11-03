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

package ai.floedb.floecat.gateway.iceberg.rest.services.view;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.common.resolver.IcebergTypeMappings;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.ViewMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.ViewRequests;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.types.LogicalTypeProtoAdapter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ViewMetadataServiceTest {
  private final ObjectMapper json = new ObjectMapper();
  private final ViewMetadataService service = new ViewMetadataService();

  @BeforeEach
  void setUp() {
    service.mapper = json;
  }

  @Test
  void fromCreateRejectsMissingRepresentations() throws Exception {
    ViewRequests.ViewVersion version =
        new ViewRequests.ViewVersion(
            1, 123L, 7, Map.of("operation", "create"), List.of(), null, null);
    ViewRequests.Create request =
        new ViewRequests.Create(
            "reports",
            null,
            json.readTree("{\"schema-id\":7,\"type\":\"struct\",\"fields\":[]}"),
            version,
            Map.of("owner", "team"));

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> service.fromCreate(List.of("db"), "reports", request));
    assertEquals("view-version.representations is required", ex.getMessage());
  }

  @Test
  void fromViewFallsBackToSynthesizedMetadataWhenStoredMetadataIsInvalid() {
    View view =
        View.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:reports").build())
            .setDisplayName("reports")
            .addSqlDefinitions(
                ai.floedb.floecat.catalog.rpc.ViewSqlDefinition.newBuilder().setSql("").build())
            .putProperties(ViewMetadataService.METADATA_PROPERTY_KEY, "{not-json")
            .putProperties(
                ViewMetadataService.METADATA_LOCATION_PROPERTY_KEY, "s3://warehouse/reports")
            .putProperties("comment", "demo")
            .build();

    ViewMetadataService.MetadataContext context = service.fromView(List.of("db"), "reports", view);

    assertEquals("s3://warehouse/reports", context.metadata().location());
    assertEquals("select 1", context.sql());
    assertEquals("cat:db:reports", context.metadata().viewUuid());
    assertEquals("demo", context.userProperties().get("comment"));
  }

  @Test
  void fromViewUsesMetadataLocationFallbackWhenSerializedMetadataLocationBlank() throws Exception {
    ViewMetadataView stored =
        new ViewMetadataView("uuid-1", 1, "", 3, List.of(), List.of(), List.of(), Map.of("k", "v"));
    View view =
        View.newBuilder()
            .setDisplayName("reports")
            .addSqlDefinitions(
                ai.floedb.floecat.catalog.rpc.ViewSqlDefinition.newBuilder()
                    .setSql("select 7")
                    .build())
            .putProperties(
                ViewMetadataService.METADATA_PROPERTY_KEY, json.writeValueAsString(stored))
            .putProperties(ViewMetadataService.METADATA_LOCATION_PROPERTY_KEY, "s3://loc/from-prop")
            .build();

    ViewMetadataService.MetadataContext context = service.fromView(List.of("db"), "reports", view);

    assertEquals("s3://loc/from-prop", context.metadata().location());
  }

  @Test
  void applyCommitRejectsUnsupportedRequirementType() throws Exception {
    ViewMetadataService.MetadataContext current = contextWithSingleVersion();
    ViewRequests.Commit commit =
        new ViewRequests.Commit(
            List.of(json.readTree("{\"type\":\"assert-last-assigned-field-id\",\"id\":3}")),
            List.of(json.readTree("{\"action\":\"set-location\",\"location\":\"s3://loc\"}")));

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> service.applyCommit(List.of("db"), current, commit));
    assertEquals("Unsupported view requirement: assert-last-assigned-field-id", ex.getMessage());
  }

  @Test
  void applyCommitRejectsSetPropertiesWithReservedKey() throws Exception {
    ViewMetadataService.MetadataContext current = contextWithSingleVersion();
    ViewRequests.Commit commit =
        new ViewRequests.Commit(
            List.of(),
            List.of(
                json.readTree(
                    """
                    {
                      "action":"set-properties",
                      "updates":{"metadata-location":"s3://blocked"}
                    }
                    """)));

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> service.applyCommit(List.of("db"), current, commit));
    assertTrue(ex.getMessage().contains("managed internally"));
  }

  @Test
  void applyCommitRejectsSetCurrentVersionWhenMissing() throws Exception {
    ViewMetadataService.MetadataContext current = contextWithSingleVersion();
    ViewRequests.Commit commit =
        new ViewRequests.Commit(
            List.of(),
            List.of(
                json.readTree("{\"action\":\"set-current-view-version\",\"view-version-id\":99}")));

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> service.applyCommit(List.of("db"), current, commit));
    assertEquals("view-version-id 99 does not exist", ex.getMessage());
  }

  @Test
  void applyCommitCanSynthesizeVersionAndHandleRemoveProperties() throws Exception {
    ViewMetadataView metadata =
        new ViewMetadataView(
            "uuid-1", 1, null, 0, List.of(), List.of(), List.of(), Map.of("owner", "a"));
    ViewMetadataService.MetadataContext current =
        new ViewMetadataService.MetadataContext(
            metadata, Map.of("owner", "a", "drop", "me"), "select 3");
    List<JsonNode> updates = new ArrayList<>();
    updates.add(json.readTree("{\"action\":\"remove-properties\",\"removals\":[\"drop\"]}"));
    updates.add(json.readTree("{\"action\":\"set-current-view-version\"}"));
    ViewRequests.Commit commit = new ViewRequests.Commit(List.of(), updates);

    ViewMetadataService.MetadataContext updated =
        service.applyCommit(List.of("db"), current, commit);

    assertEquals("select 3", updated.sql());
    assertEquals("a", updated.userProperties().get("owner"));
    assertTrue(!updated.userProperties().containsKey("drop"));
    assertEquals(1, updated.metadata().versions().size());
    assertEquals("floecat://views/db//metadata.json", updated.metadata().location());
  }

  @Test
  void extractDialectPicksFirstSqlRepresentation() {
    // contextWithSingleVersion() has dialect="ansi" in its first SQL representation
    ViewMetadataService.MetadataContext ctx = contextWithSingleVersion();
    assertEquals("ansi", service.extractDialect(ctx));
  }

  @Test
  void extractCreationSearchPathUsesDefaultNamespace() {
    // contextWithSingleVersion() has defaultNamespace=["db"]
    ViewMetadataService.MetadataContext ctx = contextWithSingleVersion();
    assertEquals(List.of("db"), service.extractCreationSearchPath(ctx));
  }

  @Test
  void extractOutputColumnsMapsTheViewSchemaAsTableSchemasAre() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(42, "id", Types.IntegerType.get()),
            Types.NestedField.optional(7, "at", Types.TimestampNanoType.withZone()),
            Types.NestedField.optional(3, "tag", Types.FixedType.ofLength(16)),
            Types.NestedField.optional(
                9,
                "point",
                Types.StructType.of(Types.NestedField.optional(10, "x", Types.LongType.get()))));

    List<SchemaColumn> columns = service.extractOutputColumns(contextWithSchema(schema));

    // The top-level columns alone, at their positions, typed as table ingestion types them.
    assertEquals(
        List.of("id:1:42", "at:2:7", "tag:3:3", "point:4:9"),
        columns.stream()
            .map(c -> c.getName() + ":" + c.getOrdinal() + ":" + c.getFieldId())
            .toList());
    for (int i = 0; i < columns.size(); i++) {
      var source = schema.columns().get(i);
      assertEquals(source.isOptional(), columns.get(i).getNullable());
      assertEquals(
          LogicalTypeProtoAdapter.toProto(IcebergTypeMappings.toLogical(source.type())),
          columns.get(i).getType());
    }
    assertEquals(9, columns.get(1).getType().getTemporalPrecision());
  }

  @Test
  void extractOutputColumnsRefusesATypeNestedPastFloecatsDepth() throws Exception {
    Type type = Types.StringType.get();
    for (int id = 100; id < 170; id++) {
      type = Types.ListType.ofOptional(id, type);
    }
    Schema schema = new Schema(Types.NestedField.optional(1, "deep", type));

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> service.extractOutputColumns(contextWithSchema(schema)));
    assertTrue(ex.getMessage().contains("nesting depth"), ex.getMessage());
  }

  /** A context whose current version's schema is `schema`. */
  private ViewMetadataService.MetadataContext contextWithSchema(Schema schema) throws Exception {
    var summary = json.readValue(SchemaParser.toJson(schema), ViewMetadataView.SchemaSummary.class);
    var current = contextWithSingleVersion();
    var metadata = current.metadata();
    return new ViewMetadataService.MetadataContext(
        new ViewMetadataView(
            metadata.viewUuid(),
            metadata.formatVersion(),
            metadata.location(),
            metadata.currentVersionId(),
            metadata.versions(),
            metadata.versionLog(),
            List.of(summary),
            metadata.properties()),
        current.userProperties(),
        current.sql());
  }

  private ViewMetadataService.MetadataContext contextWithSingleVersion() {
    ViewMetadataView.ViewVersion version =
        new ViewMetadataView.ViewVersion(
            1,
            123L,
            0,
            Map.of("operation", "create"),
            List.of(new ViewMetadataView.ViewRepresentation("sql", "select 1", "ansi")),
            List.of("db"),
            null);
    ViewMetadataView metadata =
        new ViewMetadataView(
            "uuid-1",
            1,
            "s3://warehouse/reports",
            1,
            List.of(version),
            List.of(new ViewMetadataView.ViewHistoryEntry(1, 123L)),
            List.of(new ViewMetadataView.SchemaSummary(0, "struct", List.of(), List.of())),
            Map.of("owner", "team"));
    return new ViewMetadataService.MetadataContext(metadata, Map.of("owner", "team"), "select 1");
  }
}
