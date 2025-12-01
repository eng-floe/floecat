package ai.floedb.metacat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.query.rpc.BeginQueryRequest;
import ai.floedb.metacat.query.rpc.DescribeInputsRequest;
import ai.floedb.metacat.query.rpc.QueryInput;
import ai.floedb.metacat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.metacat.query.rpc.QueryServiceGrpc;
import ai.floedb.metacat.service.bootstrap.impl.SeedRunner;
import ai.floedb.metacat.service.util.TestDataResetter;
import ai.floedb.metacat.service.util.TestSupport;
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

  @GrpcClient("metacat")
  QueryServiceGrpc.QueryServiceBlockingStub queries;

  @GrpcClient("metacat")
  QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub schemaSvc;

  @GrpcClient("metacat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("metacat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("metacat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("metacat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

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
}
