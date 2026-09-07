/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.catalog.unity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.catalog.access.CatalogCapability;
import ai.floedb.floecat.catalog.access.CatalogObjectName;
import ai.floedb.floecat.catalog.access.NamespacePath;
import ai.floedb.floecat.catalog.access.VendedStorageCredentials;
import ai.floedb.floecat.client.unity.TemporaryTableCredentials;
import ai.floedb.floecat.client.unity.UnityCatalogClient;
import ai.floedb.floecat.client.unity.UnityCatalogException;
import ai.floedb.floecat.client.unity.UnityCatalogTable;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class UnityCatalogAccessClientTest {
  private static final NamespacePath SALES = NamespacePath.of("main", "sales");
  private static final CatalogObjectName ORDERS = new CatalogObjectName(SALES, "orders");

  private final UnityCatalogClient unity = mock(UnityCatalogClient.class);
  private final UnityStorageAccessValidator storageValidator =
      mock(UnityStorageAccessValidator.class);
  private final UnityCatalogAccessClient client =
      new UnityCatalogAccessClient(unity, null, storageValidator, Map.of("s3.region", "us-east-1"));

  @Test
  void exposesTheIntegrationCapabilities() {
    assertThat(client.capabilities().supports(CatalogCapability.VALIDATE)).isTrue();
    assertThat(client.capabilities().supports(CatalogCapability.STABLE_OBJECT_IDS)).isTrue();
    assertThat(client.capabilities().supports(CatalogCapability.VEND_STORAGE_CREDENTIALS)).isTrue();
  }

  @Test
  void mapsCatalogsAndSchemasOntoHierarchicalNamespaces() {
    when(unity.listCatalogs()).thenReturn(List.of("system", "main"));
    when(unity.listSchemas("main")).thenReturn(List.of("sales", "default"));

    assertThat(client.listNamespaces(NamespacePath.root()))
        .containsExactly(NamespacePath.of("main"), NamespacePath.of("system"));
    assertThat(client.listNamespaces(NamespacePath.of("main")))
        .containsExactly(NamespacePath.of("main", "default"), SALES);
    assertThat(client.listNamespaces(SALES)).isEmpty();
  }

  @Test
  void listsOnlyDeltaTablesAndListsViewsSeparately() {
    when(unity.listTables("main", "sales"))
        .thenReturn(
            List.of(
                table("orders", "MANAGED", "DELTA"),
                table("customers", "EXTERNAL", "ICEBERG"),
                table("orders_view", "VIEW", null),
                table("", "EXTERNAL", "DELTA"),
                table("", "VIEW", null)));

    assertThat(client.listTables(SALES)).containsExactly(ORDERS);
    assertThat(client.listViews(SALES))
        .containsExactly(new CatalogObjectName(SALES, "orders_view"));
  }

  @Test
  void mapsDeltaTableMetadataAndStableIdentity() {
    UnityCatalogTable table =
        new UnityCatalogTable(
            "orders",
            "table-id",
            "EXTERNAL",
            "DELTA",
            "s3://warehouse/orders",
            null,
            List.of(
                new UnityCatalogTable.Column(
                    "order_id", "LONG", "bigint", "{\"type\":\"long\"}", false)),
            Map.of("owner", "analytics"));
    when(unity.getTable("main.sales.orders")).thenReturn(Optional.of(table));

    var mapped = client.loadTable(ORDERS);

    assertThat(mapped.identity().value()).isEqualTo("table-id");
    assertThat(mapped.identity().stable()).isTrue();
    assertThat(mapped.format()).isEqualTo("DELTA");
    assertThat(mapped.storageLocation()).contains("s3://warehouse/orders");
    assertThat(mapped.properties()).containsEntry("owner", "analytics");
    assertThat(mapped.schemaJson())
        .isEqualTo(
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"order_id\","
                + "\"type\":\"long\",\"nullable\":false}]}");
  }

  @Test
  void mapsSparkViewDefinition() {
    UnityCatalogTable view =
        new UnityCatalogTable(
            "orders_view",
            "view-id",
            "VIEW",
            null,
            null,
            "select * from orders",
            List.of(ONE_COLUMN),
            Map.of());
    CatalogObjectName name = new CatalogObjectName(SALES, "orders_view");
    when(unity.getTable("main.sales.orders_view")).thenReturn(Optional.of(view));

    var mapped = client.loadView(name);

    assertThat(mapped.definitions()).hasSize(1);
    assertThat(mapped.definitions().getFirst().sql()).isEqualTo("select * from orders");
    assertThat(mapped.definitions().getFirst().dialect()).isEqualTo("spark");
    assertThat(mapped.defaultNamespace()).isEqualTo(SALES);
  }

  @Test
  void mapsAndValidatesVendedAwsCredentials() {
    UnityCatalogTable table = deltaTable("s3://warehouse/orders");
    when(unity.getTableWithLenientColumns("main.sales.orders")).thenReturn(Optional.of(table));
    when(unity.generateTemporaryTableCredentials(
            "table-id", UnityCatalogClient.TableOperation.READ))
        .thenReturn(
            new TemporaryTableCredentials(
                new TemporaryTableCredentials.AwsCredentials("access", "secret", "session", null),
                false,
                "1770000000000",
                "s3://warehouse/orders"));

    VendedStorageCredentials credentials = client.vendStorageCredentials(ORDERS).orElseThrow();
    client.validateStorageAccess(ORDERS, credentials);

    assertThat(credentials.properties())
        .containsEntry("s3.access-key-id", "access")
        .containsEntry("s3.secret-access-key", "secret")
        .containsEntry("s3.session-token", "session")
        .containsEntry("s3.region", "us-east-1");
    assertThat(credentials.scopePrefix()).isEqualTo("s3://warehouse/orders");
    assertThat(credentials.expiresAt()).contains(Instant.ofEpochMilli(1770000000000L));
    verify(storageValidator).validate("s3://warehouse/orders", credentials);
  }

  /**
   * An expiration_time in microseconds is a unit mismatch, not a date in year 62178. Read as millis
   * it looks far in the future, so it would survive every downstream expiry check and then throw
   * inside Timestamps.fromMillis, deep in a gRPC handler where the failure is retried rather than
   * reported.
   *
   * <p>The bound makes such a value absent, and an absent expiry now refuses the vend outright: a
   * delegated credential floecat cannot bound is one the runtime vend path terminally rejects, so
   * accepting it here would let validation pass a tuple that no read can use.
   */
  @Test
  void refusesAVendWhoseExpiryIsUnusable() {
    record Case(String name, String raw) {}
    for (var c :
        java.util.List.of(
            new Case("microseconds", "1770000000000000"),
            new Case("one past the ceiling", "253402300800000"),
            new Case("zero", "0"),
            new Case("negative", "-1"),
            new Case("not a number", "soon"),
            new Case("absent", null))) {
      when(unity.getTableWithLenientColumns("main.sales.orders"))
          .thenReturn(Optional.of(deltaTable("s3://warehouse/orders")));
      when(unity.generateTemporaryTableCredentials(
              "table-id", UnityCatalogClient.TableOperation.READ))
          .thenReturn(
              new TemporaryTableCredentials(
                  new TemporaryTableCredentials.AwsCredentials("access", "secret", "session", null),
                  false,
                  c.raw(),
                  "s3://warehouse/orders"));

      assertThatThrownBy(() -> client.vendStorageCredentials(ORDERS), c.name())
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure -> {
                assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
                assertThat(failure.getMessage()).contains("expiration_time");
              });
    }
  }

  /**
   * And the other half of the same contract: a vend with no session token cannot be published
   * either, however well-formed its expiry. Validation accepted this tuple while the runtime vend
   * path terminally refused it, so an integration could report valid and then fail every read.
   */
  @Test
  void refusesAVendWithNoSessionToken() {
    when(unity.getTableWithLenientColumns("main.sales.orders"))
        .thenReturn(Optional.of(deltaTable("s3://warehouse/orders")));
    when(unity.generateTemporaryTableCredentials(
            "table-id", UnityCatalogClient.TableOperation.READ))
        .thenReturn(
            new TemporaryTableCredentials(
                new TemporaryTableCredentials.AwsCredentials("access", "secret", null, null),
                false,
                "1770000000000",
                "s3://warehouse/orders"));

    assertThatThrownBy(() -> client.vendStorageCredentials(ORDERS))
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure -> {
              assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
              assertThat(failure.getMessage()).contains("s3.session-token");
            });
  }

  /** The last instant a proto Timestamp can carry is a date, not a mismatch, so it survives. */
  @Test
  void keepsAnExpiryAtTheTimestampCeiling() {
    when(unity.getTableWithLenientColumns("main.sales.orders"))
        .thenReturn(Optional.of(deltaTable("s3://warehouse/orders")));
    when(unity.generateTemporaryTableCredentials(
            "table-id", UnityCatalogClient.TableOperation.READ))
        .thenReturn(
            new TemporaryTableCredentials(
                new TemporaryTableCredentials.AwsCredentials("access", "secret", "session", null),
                false,
                "253402300799999",
                "s3://warehouse/orders"));

    assertThat(client.vendStorageCredentials(ORDERS).orElseThrow().expiresAt())
        .contains(Instant.ofEpochMilli(253402300799999L));
  }

  /**
   * Listing a namespace that cannot hold tables is a question with a true answer -- none -- not a
   * misconfiguration. listNamespaces hands out one-segment catalog paths by design, and an overlay
   * with no include filters (the documented default) has the reconciler list tables for every one
   * of them, so throwing here failed every unfiltered overlay against a Unity workspace on the
   * first catalog it reached.
   */
  @Test
  void listingANamespaceThatCannotHoldTablesIsEmptyRatherThanAFailure() {
    for (NamespacePath namespace :
        java.util.List.of(
            NamespacePath.root(),
            NamespacePath.of("main"),
            NamespacePath.of("main", "sales", "x"))) {
      assertThat(client.listTables(namespace)).as("tables %s", namespace).isEmpty();
      assertThat(client.listViews(namespace)).as("views %s", namespace).isEmpty();
    }
    // And it never reached the upstream to find that out.
    org.mockito.Mockito.verifyNoInteractions(unity);
  }

  /** Addressing an object is not listing: a namespace of the wrong depth cannot name one. */
  @Test
  void loadingThroughANamespaceThatCannotHoldTablesStillFails() {
    CatalogObjectName shallow = new CatalogObjectName(NamespacePath.of("main"), "orders");
    assertThatThrownBy(() -> client.loadTable(shallow))
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure ->
                assertThat(failure.code())
                    .isEqualTo(CatalogAccessException.Code.INVALID_CONFIGURATION));
    assertThatThrownBy(() -> client.vendStorageCredentials(shallow))
        .isInstanceOf(CatalogAccessException.class);
  }

  /**
   * Neither a vended storage_url nor a table storage_location means this provider cannot say what
   * the credential reaches. A blank scope is not a narrow one: covers("") is true everywhere and
   * the vendor would stamp it as an unrestricted response prefix. Refused by name, so an operator
   * sees which of the two locations was missing rather than a generic "vended no credentials".
   */
  @Test
  void aVendWithNoKnownScopeIsRefusedRatherThanVendedUnscoped() {
    UnityCatalogTable noLocation = deltaTable(null);
    when(unity.getTableWithLenientColumns("main.sales.orders")).thenReturn(Optional.of(noLocation));
    when(unity.generateTemporaryTableCredentials(
            "table-id", UnityCatalogClient.TableOperation.READ))
        .thenReturn(
            new TemporaryTableCredentials(
                new TemporaryTableCredentials.AwsCredentials("access", "secret", "session", null),
                false,
                "1770000000000",
                null));

    assertThatThrownBy(() -> client.vendStorageCredentials(ORDERS))
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure -> {
              assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
              assertThat(failure.getMessage()).contains("states a location");
            });
  }

  /**
   * Vending and validation read no column, and getTable's javadoc sends every such caller to the
   * lenient variant: a columns shape one Unity deployment renders differently would otherwise fail
   * the table for both, where neither ever looks at the schema.
   */
  @Test
  void vendingAndValidationDoNotDependOnTheSchemaDecoding() {
    UnityCatalogTable table = deltaTable("s3://warehouse/orders");
    when(unity.getTableWithLenientColumns("main.sales.orders")).thenReturn(Optional.of(table));
    when(unity.getTable("main.sales.orders"))
        .thenThrow(new AssertionError("vending must not take the strict path"));
    when(unity.generateTemporaryTableCredentials(
            "table-id", UnityCatalogClient.TableOperation.READ))
        .thenReturn(
            new TemporaryTableCredentials(
                new TemporaryTableCredentials.AwsCredentials("access", "secret", "session", null),
                false,
                "1770000000000",
                "s3://warehouse/orders"));

    VendedStorageCredentials credentials = client.vendStorageCredentials(ORDERS).orElseThrow();
    client.validateStorageAccess(ORDERS, credentials);

    verify(storageValidator).validate("s3://warehouse/orders", credentials);
  }

  /**
   * The view schema must be Iceberg form, not Delta form. CatalogOverlayReconciler parses it with
   * SchemaParser, which requires an id and required on every field, and a parse failure there
   * aborts the whole overlay -- every table in it, not just the view.
   */
  @Test
  void viewSchemaIsIcebergFormWithPositionalIds() throws Exception {
    UnityCatalogTable view =
        viewWith(
            column("id", "LONG", "bigint", false),
            column("name", "STRING", "string", true),
            column("amount", "DECIMAL", "decimal(10,2)", true));
    when(unity.getTable("main.sales.orders_view")).thenReturn(Optional.of(view));

    var schema =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .readTree(
                client.loadView(new CatalogObjectName(SALES, "orders_view")).outputSchemaJson());

    assertThat(schema.get("type").asText()).isEqualTo("struct");
    var fields = schema.get("fields");
    assertThat(fields).hasSize(3);
    // Positional ids from 1: Unity issues none for view columns, and position is the identity a
    // reader resolving output columns needs.
    assertThat(fields.get(0).get("id").asInt()).isEqualTo(1);
    assertThat(fields.get(2).get("id").asInt()).isEqualTo(3);
    // required is the inverse of nullable, and must be present or SchemaParser refuses the field.
    assertThat(fields.get(0).get("required").asBoolean()).isTrue();
    assertThat(fields.get(1).get("required").asBoolean()).isFalse();
    assertThat(fields.get(0).get("type").asText()).isEqualTo("long");
    assertThat(fields.get(1).get("type").asText()).isEqualTo("string");
    assertThat(fields.get(2).get("type").asText()).isEqualTo("decimal(10, 2)");
  }

  /** Delta and Iceberg disagree on two things it would be quiet to get wrong. */
  @Test
  void icebergTypeMappingWidensSafelyAndKeepsTimestampSemantics() {
    record Case(String typeName, String typeText, String expected) {}
    for (Case c :
        java.util.List.of(
            new Case("BOOLEAN", "boolean", "boolean"),
            // Iceberg has no byte or short; widening to int cannot lose a value.
            new Case("BYTE", "tinyint", "int"),
            new Case("SHORT", "smallint", "int"),
            new Case("INT", "int", "int"),
            new Case("LONG", "bigint", "long"),
            new Case("FLOAT", "float", "float"),
            new Case("DOUBLE", "double", "double"),
            new Case("DATE", "date", "date"),
            // An instant, not a local time. Swapping these two shifts every value silently.
            new Case("TIMESTAMP", "timestamp", "timestamptz"),
            new Case("TIMESTAMP_NTZ", "timestamp_ntz", "timestamp"),
            new Case("STRING", "string", "string"),
            new Case("BINARY", "binary", "binary"),
            new Case("DECIMAL", "decimal(38,9)", "decimal(38, 9)"))) {
      assertThat(
              UnityCatalogAccessClient.icebergType(column("c", c.typeName(), c.typeText(), true)))
          .as(c.typeName())
          .isEqualTo(c.expected());
    }
  }

  /**
   * A type with no Iceberg primitive is named, not guessed. Widening an unmapped type to string
   * would produce a schema that parses and lies about the column.
   */
  @Test
  void viewWithAnUnrepresentableColumnTypeIsRefusedByName() {
    UnityCatalogTable view = viewWith(column("payload", "STRUCT", "struct<a:int>", true));
    when(unity.getTable("main.sales.orders_view")).thenReturn(Optional.of(view));

    assertThatThrownBy(() -> client.loadView(new CatalogObjectName(SALES, "orders_view")))
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure -> {
              assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
              assertThat(failure.getMessage()).contains("payload");
            });
    // Decimal without a stated precision and scale is unmapped for the same reason.
    assertThat(UnityCatalogAccessClient.icebergType(column("d", "DECIMAL", null, true))).isNull();
  }

  /**
   * A nested output column is ordinary in Databricks, and type_json carries its shape. Iceberg
   * gives every member of a container its own id, so the translation has to allocate them from the
   * same counter as the top-level fields -- ids must be unique across the schema, not per level.
   */
  @Test
  void viewSchemaTranslatesNestedTypesWithUniqueIds() throws Exception {
    UnityCatalogTable view =
        viewWith(
            jsonColumn("id", "\"long\"", false),
            jsonColumn(
                "tags",
                "{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true}",
                true),
            jsonColumn(
                "props",
                "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"long\","
                    + "\"valueContainsNull\":false}",
                true),
            jsonColumn(
                "addr",
                "{\"type\":\"struct\",\"fields\":["
                    + "{\"name\":\"city\",\"type\":\"string\",\"nullable\":true},"
                    + "{\"name\":\"zip\",\"type\":\"integer\",\"nullable\":false}]}",
                true));
    when(unity.getTable("main.sales.orders_view")).thenReturn(Optional.of(view));

    var schema =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .readTree(
                client.loadView(new CatalogObjectName(SALES, "orders_view")).outputSchemaJson());
    var fields = schema.get("fields");

    assertThat(fields).hasSize(4);
    assertThat(fields.get(1).get("type").get("type").asText()).isEqualTo("list");
    assertThat(fields.get(1).get("type").get("element").asText()).isEqualTo("string");
    assertThat(fields.get(1).get("type").get("element-required").asBoolean()).isFalse();
    var map = fields.get(2).get("type");
    assertThat(map.get("type").asText()).isEqualTo("map");
    assertThat(map.get("key").asText()).isEqualTo("string");
    assertThat(map.get("value").asText()).isEqualTo("long");
    // valueContainsNull false means the value is required.
    assertThat(map.get("value-required").asBoolean()).isTrue();
    var struct = fields.get(3).get("type");
    assertThat(struct.get("type").asText()).isEqualTo("struct");
    assertThat(struct.get("fields")).hasSize(2);
    assertThat(struct.get("fields").get(1).get("required").asBoolean()).isTrue();

    // Every id in the schema, at any depth, is distinct.
    var ids = new java.util.ArrayList<Integer>();
    collectIds(schema, ids);
    assertThat(ids).doesNotHaveDuplicates().isNotEmpty();
  }

  /**
   * An untranslatable view is advertised and refused at load, not dropped from the listing.
   *
   * <p>One path, deliberately. Filtering it out of the listing hid it from the reconciler entirely,
   * and the reconciler reads a relation missing from discovery as deleted upstream -- so a view
   * that was already materialized got hard-deleted, while the same view in a deployment that omits
   * columns from the listing went down the loadView path and was correctly left alone. Which
   * happened came down to deployment variance. Refusing at load reaches the reconciler's per-object
   * guard, which records the view as unobserved and leaves the local copy in place.
   */
  @Test
  void anUntranslatableViewIsListedAndRefusedAtLoad() {
    UnityCatalogTable ok = viewWith(jsonColumn("id", "\"long\"", false));
    UnityCatalogTable unmappable =
        new UnityCatalogTable(
            "variant_view",
            "view-id-2",
            "VIEW",
            null,
            null,
            "select 1",
            List.of(column("payload", "VARIANT", "variant", true)),
            Map.of());
    when(unity.listTables("main", "sales")).thenReturn(List.of(ok, unmappable));
    when(unity.getTable("main.sales.variant_view")).thenReturn(Optional.of(unmappable));

    assertThat(client.listViews(SALES))
        .containsExactly(
            new CatalogObjectName(SALES, "orders_view"),
            new CatalogObjectName(SALES, "variant_view"));

    assertThatThrownBy(() -> client.loadView(new CatalogObjectName(SALES, "variant_view")))
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure -> {
              assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
              assertThat(failure.getMessage()).contains("payload");
            });
  }

  /** Tables and views come from one paginated walk of /tables, not one each. */
  @Test
  void bothListingsShareASingleUpstreamWalk() {
    UnityCatalogTable table = deltaTable("s3://warehouse/orders");
    UnityCatalogTable view = viewWith(jsonColumn("id", "\"long\"", false));
    when(unity.listTables("main", "sales")).thenReturn(List.of(table, view));

    assertThat(client.listTables(SALES)).containsExactly(ORDERS);
    assertThat(client.listViews(SALES))
        .containsExactly(new CatalogObjectName(SALES, "orders_view"));

    // Twice would pay for two paginated walks to learn the same thing, and let the two listings
    // disagree if the upstream changed in between.
    verify(unity, org.mockito.Mockito.times(1)).listTables("main", "sales");
  }

  private static void collectIds(com.fasterxml.jackson.databind.JsonNode node, List<Integer> ids) {
    if (node.isObject()) {
      node.fields()
          .forEachRemaining(
              entry -> {
                String key = entry.getKey();
                if (key.equals("id") || key.endsWith("-id")) {
                  ids.add(entry.getValue().asInt());
                } else {
                  collectIds(entry.getValue(), ids);
                }
              });
    } else if (node.isArray()) {
      node.forEach(child -> collectIds(child, ids));
    }
  }

  private static UnityCatalogTable.Column jsonColumn(
      String name, String typeJson, boolean nullable) {
    return new UnityCatalogTable.Column(
        name, null, null, "{\"name\":\"" + name + "\",\"type\":" + typeJson + "}", nullable);
  }

  /**
   * Partition columns come back in partition order, and index 0 is the first of them rather than a
   * default meaning "not partitioned". An empty list here records a partitioned Delta table as
   * unpartitioned and gives up pruning for every query against it.
   */
  @Test
  void loadTableReportsPartitionColumnsInPartitionOrder() {
    UnityCatalogTable table =
        new UnityCatalogTable(
            "orders",
            "table-id",
            "EXTERNAL",
            "DELTA",
            "s3://warehouse/orders",
            null,
            List.of(
                partitioned("region", 1),
                column("id", "LONG", "bigint", false),
                partitioned("day", 0)),
            Map.of());
    when(unity.getTable("main.sales.orders")).thenReturn(Optional.of(table));

    assertThat(client.loadTable(ORDERS).partitionKeys()).containsExactly("day", "region");
  }

  /**
   * The format filter and the loader have to normalize identically. Comparing the raw value dropped
   * a table whose format came back padded from every listing, while loadTable would have taken it
   * -- so the table never appeared in an overlay and nothing said why.
   */
  @Test
  void aPaddedDataSourceFormatIsStillDelta() {
    UnityCatalogTable padded =
        new UnityCatalogTable(
            "orders",
            "table-id",
            "EXTERNAL",
            " delta ",
            "s3://warehouse/orders",
            null,
            List.of(ONE_COLUMN),
            Map.of());
    when(unity.listTables("main", "sales")).thenReturn(List.of(padded));
    when(unity.getTable("main.sales.orders")).thenReturn(Optional.of(padded));

    assertThat(client.listTables(SALES)).containsExactly(ORDERS);
    assertThat(client.loadTable(ORDERS).format()).isEqualTo("DELTA");
  }

  /**
   * The same normalization, on the other side of the split. A padded {@code "VIEW "} failed the
   * view test, then failed the table test on its data source format, and was listed as neither --
   * gone from the overlay with no log line and no skip counter, which is the one outcome the two
   * predicates exist to prevent.
   */
  @Test
  void aPaddedViewTypeIsStillAView() {
    UnityCatalogTable padded =
        new UnityCatalogTable(
            "orders_view",
            "view-id",
            " view ",
            null,
            null,
            "select * from orders",
            List.of(),
            Map.of());
    when(unity.listTables("main", "sales")).thenReturn(List.of(padded));
    when(unity.getTable("main.sales.orders_view")).thenReturn(Optional.of(padded));

    assertThat(client.listViews(SALES))
        .containsExactly(new CatalogObjectName(SALES, "orders_view"));
    assertThat(client.listTables(SALES)).isEmpty();
  }

  /**
   * A view with nothing to plan against is refused rather than published. The reconciler writes the
   * CatalogView straight to the repository, bypassing the invariant ViewServiceImpl enforces on the
   * gRPC path, and a view persisted with no definitions answers "" from ViewNode.sql() -- it
   * resolves like any other relation and then cannot be planned or queried.
   */
  @Test
  void refusesAViewWithNoUsableDefinition() {
    record Case(String name, String definition) {}
    for (Case c :
        java.util.List.of(
            new Case("absent", null), new Case("blank", "   "), new Case("empty", ""))) {
      UnityCatalogTable view =
          new UnityCatalogTable(
              "orders_view", "view-id", "VIEW", null, null, c.definition(), List.of(), Map.of());
      when(unity.getTable("main.sales.orders_view")).thenReturn(Optional.of(view));

      assertThatThrownBy(
              () -> client.loadView(new CatalogObjectName(SALES, "orders_view")), c.name())
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure -> {
                assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
                assertThat(failure.getMessage()).contains("view_definition");
              });
    }
  }

  /**
   * The transport separates "Unity answered with a cloud this client does not map" from "Unity
   * answered with no credential field at all", and collapsing both into an empty Optional bought
   * nothing: a Catalog Integration refuses an empty vend outright, so the only thing empty cost was
   * the reason. An Azure-backed workspace reported "vended no storage credentials" with nothing
   * naming the cloud.
   */
  @Test
  void namesTheReasonWhenNoAwsCredentialsCanBeAssembled() {
    record Case(String name, boolean unsupportedCloud, String expected) {}
    for (Case c :
        java.util.List.of(
            new Case("azure, gcp or r2", true, "cloud this provider does not support"),
            new Case("no credential field at all", false, "no recognized storage credentials"))) {
      when(unity.getTableWithLenientColumns("main.sales.orders"))
          .thenReturn(Optional.of(deltaTable("s3://warehouse/orders")));
      when(unity.generateTemporaryTableCredentials(
              "table-id", UnityCatalogClient.TableOperation.READ))
          .thenReturn(
              new TemporaryTableCredentials(
                  null, c.unsupportedCloud(), "1770000000000", "s3://warehouse/orders"));

      assertThatThrownBy(() -> client.vendStorageCredentials(ORDERS), c.name())
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure -> {
                assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
                assertThat(failure.getMessage()).contains(c.expected());
              });
    }
  }

  /**
   * Unity answers a table it cannot mint credentials for with a 4xx carrying the workspace error
   * envelope, which the transport reads as INVALID_REQUEST -- and UnityCatalogErrors maps that to
   * INVALID_CONFIGURATION, which a validation walk treats as terminal because it is meant to
   * describe the Integration. It does not: a table with no storage credential or on a non-cloud
   * location answers this way while its neighbours vend, so one such table ended the whole search.
   */
  @Test
  void aCredentialsRouteRefusalIsPerTableRatherThanTerminal() {
    when(unity.getTableWithLenientColumns("main.sales.orders"))
        .thenReturn(Optional.of(deltaTable("s3://warehouse/orders")));
    when(unity.generateTemporaryTableCredentials(
            "table-id", UnityCatalogClient.TableOperation.READ))
        .thenThrow(
            new UnityCatalogException(
                UnityCatalogException.Failure.INVALID_REQUEST,
                400,
                "no storage credential for this table",
                null));

    assertThatThrownBy(() -> client.vendStorageCredentials(ORDERS))
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure -> {
              assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
              assertThat(failure.getMessage()).contains("main.sales.orders");
            });
  }

  /**
   * Scoped to that one call. An INVALID_REQUEST from listing or loading really does describe the
   * Integration -- a base URI a proxy will not route answers this way for everything -- so it stays
   * terminal.
   */
  @Test
  void aRefusalFromAnotherRouteKeepsItsTerminalClassification() {
    when(unity.listTables("main", "sales"))
        .thenThrow(
            new UnityCatalogException(
                UnityCatalogException.Failure.INVALID_REQUEST, 400, "ENDPOINT_NOT_FOUND", null));

    assertThatThrownBy(() -> client.listTables(SALES))
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure ->
                assertThat(failure.code())
                    .isEqualTo(CatalogAccessException.Code.INVALID_CONFIGURATION));
  }

  /**
   * A relation with no columns is refused per object rather than published as an empty schema.
   * parseColumns treats an absent or JSON-null "columns" as an empty list even in strict mode, and
   * nothing downstream catches it: IcebergSchemaMapper answers with a descriptor holding zero
   * columns rather than throwing, so the reconciler persisted a relation with no output columns and
   * reported it created.
   */
  @Test
  void refusesARelationThatExposesNoColumns() {
    UnityCatalogTable table =
        new UnityCatalogTable(
            "orders",
            "table-id",
            "EXTERNAL",
            "DELTA",
            "s3://warehouse/orders",
            null,
            List.of(),
            Map.of());
    when(unity.getTable("main.sales.orders")).thenReturn(Optional.of(table));

    assertThatThrownBy(() -> client.loadTable(ORDERS))
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure -> {
              assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
              assertThat(failure.getMessage()).contains("no columns", "main.sales.orders");
            });

    UnityCatalogTable view =
        new UnityCatalogTable(
            "orders_view",
            "view-id",
            "VIEW",
            null,
            null,
            "select * from orders",
            List.of(),
            Map.of());
    when(unity.getTable("main.sales.orders_view")).thenReturn(Optional.of(view));

    assertThatThrownBy(() -> client.loadView(new CatalogObjectName(SALES, "orders_view")))
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure ->
                assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED));
  }

  /**
   * The probe that follows a vend reuses the table the vend loaded. Both needed the storage
   * location and both loaded it, so every validation attempt paid three catalog round trips where
   * the attempt cap is sized for two.
   */
  @Test
  void theStorageProbeReusesTheTableTheVendJustLoaded() {
    var table = deltaTable("s3://warehouse/orders");
    when(unity.getTableWithLenientColumns("main.sales.orders")).thenReturn(Optional.of(table));
    when(unity.generateTemporaryTableCredentials(
            "table-id", UnityCatalogClient.TableOperation.READ))
        .thenReturn(
            new TemporaryTableCredentials(
                new TemporaryTableCredentials.AwsCredentials("access", "secret", "session", null),
                false,
                "1770000000000",
                "s3://warehouse/orders"));

    var vended = client.vendStorageCredentials(ORDERS).orElseThrow();
    client.validateStorageAccess(ORDERS, vended);

    // One load for the vend, none for the probe.
    verify(unity, times(1)).getTableWithLenientColumns("main.sales.orders");
  }

  /**
   * Consumed rather than cached: a standalone probe long after some unrelated vend has to load the
   * table itself, or it would check a location that may have moved since.
   */
  @Test
  void aStandaloneStorageProbeLoadsTheTableItself() {
    var table = deltaTable("s3://warehouse/orders");
    when(unity.getTableWithLenientColumns("main.sales.orders")).thenReturn(Optional.of(table));
    var credentials =
        new VendedStorageCredentials(
            Map.of("s3.access-key-id", "AKIA", "s3.secret-access-key", "secret"),
            "s3://warehouse/orders",
            Optional.empty());

    client.validateStorageAccess(ORDERS, credentials);

    verify(unity, times(1)).getTableWithLenientColumns("main.sales.orders");
  }

  /**
   * A column-mapped Delta table keeps its mapping. Unity carries {@code delta.columnMapping.id} and
   * {@code delta.columnMapping.physicalName} in each field's {@code metadata}, and both are read
   * downstream -- DeltaSchemaMapper takes the id as the field id, DeltaSchemaNormalizer builds the
   * logical-to-physical Parquet name mapping from the physical name. Rebuilding each field from
   * name, type and nullable dropped the metadata, so a renamed or column-mapped column resolved
   * against the wrong Parquet column or not at all. Name mapping is the ordinary shape for a
   * managed Unity table.
   */
  @Test
  void schemaJsonKeepsColumnMappingMetadata() throws Exception {
    String typeJson =
        "{\"name\":\"amount\",\"type\":\"long\",\"nullable\":true,\"metadata\":"
            + "{\"delta.columnMapping.id\":7,\"delta.columnMapping.physicalName\":\"col-7\"}}";
    UnityCatalogTable table =
        new UnityCatalogTable(
            "orders",
            "table-id",
            "EXTERNAL",
            "DELTA",
            "s3://warehouse/orders",
            null,
            List.of(new UnityCatalogTable.Column("amount", "LONG", "long", typeJson, true, null)),
            Map.of());
    when(unity.getTable("main.sales.orders")).thenReturn(Optional.of(table));

    var mapped =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .readTree(client.loadTable(ORDERS).schemaJson());
    var field = mapped.get("fields").get(0);

    assertThat(field.get("name").asText()).isEqualTo("amount");
    assertThat(field.get("type").asText()).isEqualTo("long");
    assertThat(field.get("nullable").asBoolean()).isTrue();
    assertThat(field.get("metadata").get("delta.columnMapping.id").asInt()).isEqualTo(7);
    assertThat(field.get("metadata").get("delta.columnMapping.physicalName").asText())
        .isEqualTo("col-7");
  }

  /**
   * And the column record still decides name and nullability. A type_json that disagrees with the
   * listing is not the authority on those, and a column with no usable type_json still produces a
   * field from its declared type.
   */
  @Test
  void theColumnRecordDecidesNameAndNullabilityRegardlessOfTypeJson() throws Exception {
    UnityCatalogTable table =
        new UnityCatalogTable(
            "orders",
            "table-id",
            "EXTERNAL",
            "DELTA",
            "s3://warehouse/orders",
            null,
            List.of(
                new UnityCatalogTable.Column(
                    "amount",
                    "LONG",
                    "long",
                    "{\"name\":\"stale\",\"type\":\"long\",\"nullable\":true}",
                    false,
                    null),
                new UnityCatalogTable.Column("note", "STRING", "string", null, true, null)),
            Map.of());
    when(unity.getTable("main.sales.orders")).thenReturn(Optional.of(table));

    var mapped =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .readTree(client.loadTable(ORDERS).schemaJson());

    assertThat(mapped.get("fields").get(0).get("name").asText()).isEqualTo("amount");
    assertThat(mapped.get("fields").get(0).get("nullable").asBoolean()).isFalse();
    assertThat(mapped.get("fields").get(1).get("name").asText()).isEqualTo("note");
    assertThat(mapped.get("fields").get(1).get("type").asText()).isEqualTo("string");
  }

  /**
   * A column with no type at all is refused, not published with an empty one. Same policy as
   * viewSchemaJson: a schema that parses and lies fails later, in DeltaSchemaMapper or at scan
   * time, with nothing pointing back at the upstream column.
   */
  @Test
  void refusesAColumnThatStatesNoType() {
    UnityCatalogTable table =
        new UnityCatalogTable(
            "orders",
            "table-id",
            "EXTERNAL",
            "DELTA",
            "s3://warehouse/orders",
            null,
            List.of(new UnityCatalogTable.Column("amount", null, null, null, true, null)),
            Map.of());
    when(unity.getTable("main.sales.orders")).thenReturn(Optional.of(table));

    assertThatThrownBy(() -> client.loadTable(ORDERS))
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure -> {
              assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
              assertThat(failure.getMessage()).contains("states no type", "amount");
            });
  }

  /** A column whose type_json is unusable still publishes from its declared type. */
  @Test
  void fallsBackToTheDeclaredTypeWhenTypeJsonIsUnusable() throws Exception {
    UnityCatalogTable table =
        new UnityCatalogTable(
            "orders",
            "table-id",
            "EXTERNAL",
            "DELTA",
            "s3://warehouse/orders",
            null,
            List.of(new UnityCatalogTable.Column("amount", "LONG", null, "not json", true, null)),
            Map.of());
    when(unity.getTable("main.sales.orders")).thenReturn(Optional.of(table));

    var mapped =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .readTree(client.loadTable(ORDERS).schemaJson());

    assertThat(mapped.get("fields").get(0).get("type").asText()).isEqualTo("LONG");
  }

  private static UnityCatalogTable.Column partitioned(String name, int partitionIndex) {
    return new UnityCatalogTable.Column(name, "STRING", "string", null, true, partitionIndex);
  }

  /**
   * Unity answers more relation kinds than VIEW and table. MATERIALIZED_VIEW, STREAMING_TABLE and
   * FOREIGN are all DELTA, and only MANAGED and EXTERNAL are promised a storage_location -- so
   * treating "not a VIEW" as a table advertised a materialized view, an Overlay persisted it as
   * TF_DELTA, and vending later picked an object with no addressable storage.
   */
  @Test
  void listTablesAdvertisesOnlyKindsTheExternalDeltaPathCanRead() {
    when(unity.listTables("main", "sales"))
        .thenReturn(
            List.of(
                relation("managed", "MANAGED", "DELTA"),
                relation("external", "EXTERNAL", "DELTA"),
                relation("mat_view", "MATERIALIZED_VIEW", "DELTA"),
                relation("stream", "STREAMING_TABLE", "DELTA"),
                relation("foreign_tbl", "FOREIGN", "DELTA"),
                relation("future_kind", "SOMETHING_NEW", "DELTA"),
                relation("iceberg", "EXTERNAL", "ICEBERG")));

    assertThat(client.listTables(SALES))
        .containsExactly(
            new CatalogObjectName(SALES, "external"), new CatalogObjectName(SALES, "managed"));
    // Nor are the view-like kinds offered as views: loadView needs a definition and a
    // representable output schema, so promoting them is a feature, not a classification fix.
    assertThat(client.listViews(SALES)).isEmpty();
  }

  private static UnityCatalogTable relation(String name, String tableType, String format) {
    return new UnityCatalogTable(
        name, name + "-id", tableType, format, "s3://warehouse/" + name, null, List.of(), Map.of());
  }

  private static UnityCatalogTable.Column column(
      String name, String typeName, String typeText, boolean nullable) {
    return new UnityCatalogTable.Column(name, typeName, typeText, null, nullable);
  }

  private static UnityCatalogTable viewWith(UnityCatalogTable.Column... columns) {
    return new UnityCatalogTable(
        "orders_view",
        "view-id",
        "VIEW",
        null,
        null,
        "select * from orders",
        List.of(columns),
        Map.of());
  }

  @Test
  void rejectsCredentialsWhoseScopeDoesNotCoverTheTable() {
    when(unity.getTableWithLenientColumns("main.sales.orders"))
        .thenReturn(Optional.of(deltaTable("s3://warehouse/orders")));
    VendedStorageCredentials credentials =
        new VendedStorageCredentials(
            Map.of("s3.access-key-id", "access"), "s3://another/table", Optional.empty());

    assertThatThrownBy(() -> client.validateStorageAccess(ORDERS, credentials))
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure ->
                assertThat(failure.code())
                    .isEqualTo(CatalogAccessException.Code.CREDENTIAL_SCOPE_INVALID));
  }

  @Test
  void rejectsVendedCredentialsWhoseScopeDoesNotCoverTheTable() {
    when(unity.getTableWithLenientColumns("main.sales.orders"))
        .thenReturn(Optional.of(deltaTable("s3://warehouse/orders")));
    when(unity.generateTemporaryTableCredentials(
            "table-id", UnityCatalogClient.TableOperation.READ))
        .thenReturn(
            new TemporaryTableCredentials(
                new TemporaryTableCredentials.AwsCredentials("access", "secret", "session", null),
                false,
                "1770000000000",
                "s3://another/table"));

    assertThatThrownBy(() -> client.vendStorageCredentials(ORDERS))
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure ->
                assertThat(failure.code())
                    .isEqualTo(CatalogAccessException.Code.CREDENTIAL_SCOPE_INVALID));
  }

  @Test
  void preservesCatalogAuthenticationFailureWrappedByTheHttpClient() {
    CatalogAccessException authenticationFailure =
        new CatalogAccessException(
            CatalogAccessException.Code.UNAUTHENTICATED,
            "Unity Catalog OAuth token request failed with HTTP 401");
    when(unity.listCatalogs())
        .thenThrow(
            new UnityCatalogException(
                UnityCatalogException.Failure.TRANSPORT,
                -1,
                "Unity Catalog request failed",
                authenticationFailure));

    assertThatThrownBy(client::validate).isSameAs(authenticationFailure);
  }

  @Test
  void closesTransportAndAuthenticationOwnerOnce() throws Exception {
    AutoCloseable authenticationOwner = mock(AutoCloseable.class);
    UnityCatalogAccessClient owned =
        new UnityCatalogAccessClient(unity, authenticationOwner, storageValidator, Map.of());

    owned.close();
    owned.close();

    verify(unity, times(1)).close();
    verify(authenticationOwner, times(1)).close();
  }

  private static UnityCatalogTable table(String name, String tableType, String format) {
    return new UnityCatalogTable(
        name, name + "-id", tableType, format, null, null, List.of(), Map.of());
  }

  private static UnityCatalogTable deltaTable(String location) {
    return new UnityCatalogTable(
        "orders", "table-id", "EXTERNAL", "DELTA", location, null, List.of(ONE_COLUMN), Map.of());
  }

  /**
   * Enough of a schema to load. A relation with no columns is refused per object now, so a fixture
   * that carries none is asserting on a shape the provider does not accept.
   */
  private static final UnityCatalogTable.Column ONE_COLUMN =
      new UnityCatalogTable.Column("id", "LONG", "long", null, false, null);
}
