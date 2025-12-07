package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.*;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.DescribeInputsRequest;
import ai.floedb.floecat.query.rpc.QueryInput;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests schema resolution for specific snapshots using DescribeInputs().
 *
 * <p>BeginQuery no longer returns schema; DescribeInputs now does.
 */
@QuarkusTest
class QueryServiceSchemaIT {

  @GrpcClient("floecat")
  QueryServiceGrpc.QueryServiceBlockingStub queries;

  @GrpcClient("floecat")
  QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub schemaSvc;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @GrpcClient("floecat")
  ViewServiceGrpc.ViewServiceBlockingStub view;

  String catalogPrefix = this.getClass().getSimpleName() + "_";

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void describeInputsReturnsSchemaPerSnapshot() {

    // ------------------------------
    // Create catalog, namespace, table
    // ------------------------------
    var cat = TestSupport.createCatalog(catalog, catalogPrefix + "cat1", "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "sch", List.of("db"), "desc");

    Schema schemaV1 = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    Schema schemaV2 =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "qty", Types.IntegerType.get()));

    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders",
            "s3://bucket/orders",
            SchemaParser.toJson(schemaV1),
            "desc");

    // snapshot 1 → schemaV1
    var snap1 =
        TestSupport.createSnapshot(
            snapshot, tbl.getResourceId(), 1L, System.currentTimeMillis() - 10_000L);

    // update table schema → schemaV2
    table.updateTable(
        UpdateTableRequest.newBuilder()
            .setTableId(tbl.getResourceId())
            .setSpec(
                TableSpec.newBuilder()
                    .setSchemaJson(SchemaParser.toJson(schemaV2))
                    .setUpstream(tbl.getUpstream()))
            .setUpdateMask(FieldMask.newBuilder().addPaths("schema_json").build())
            .build());

    // snapshot 2 → schemaV2
    var snap2 =
        TestSupport.createSnapshot(
            snapshot, tbl.getResourceId(), 2L, System.currentTimeMillis() - 5_000L);

    // Fully qualified name
    var name =
        NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addPath("db")
            .addPath("sch")
            .setName("orders")
            .build();

    // ------------------------------
    // BeginQuery (no inputs)
    // ------------------------------
    var begin = queries.beginQuery(BeginQueryRequest.newBuilder().setTtlSeconds(10).build());

    String qid = begin.getQuery().getQueryId();
    assertFalse(qid.isBlank(), "BeginQuery must return queryId");

    // ------------------------------
    // DescribeInputs with snapshot1
    // ------------------------------
    var resp1 =
        schemaSvc.describeInputs(
            DescribeInputsRequest.newBuilder()
                .setQueryId(qid)
                .addInputs(
                    QueryInput.newBuilder()
                        .setName(name)
                        .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snap1.getSnapshotId())))
                .build());

    assertEquals(1, resp1.getSchemasCount());
    assertEquals(1, resp1.getSchemas(0).getColumnsCount(), "snapshot1 schema should have 1 column");

    // ------------------------------
    // DescribeInputs with snapshot2
    // ------------------------------
    var resp2 =
        schemaSvc.describeInputs(
            DescribeInputsRequest.newBuilder()
                .setQueryId(qid)
                .addInputs(
                    QueryInput.newBuilder()
                        .setName(name)
                        .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snap2.getSnapshotId())))
                .build());

    assertEquals(1, resp2.getSchemasCount());
    assertEquals(
        2,
        resp2.getSchemas(0).getColumnsCount(),
        "snapshot2 schema should expose v2 with 2 columns");

    assertTrue(
        resp2.getSchemas(0).getColumnsList().stream().anyMatch(c -> c.getName().equals("qty")),
        "schemaV2 must include column 'qty'");
  }

  @Test
  void describeInputsHandlesViewInputs() {
    var cat = TestSupport.createCatalog(catalog, catalogPrefix + "cat_view", "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "sch", List.of("db"), "desc");
    var createdView =
        TestSupport.createView(
            view, cat.getResourceId(), ns.getResourceId(), "orders_view", "select 1", "desc");

    var name =
        NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addPath("db")
            .addPath("sch")
            .setName(createdView.getDisplayName())
            .build();

    var begin = queries.beginQuery(BeginQueryRequest.newBuilder().setTtlSeconds(10).build());
    String qid = begin.getQuery().getQueryId();

    var response =
        schemaSvc.describeInputs(
            DescribeInputsRequest.newBuilder()
                .setQueryId(qid)
                .addInputs(QueryInput.newBuilder().setName(name))
                .build());

    assertEquals(1, response.getSchemasCount());
    assertEquals(
        0,
        response.getSchemas(0).getColumnsCount(),
        "view schema should currently be empty (no stored output columns)");
  }
}
