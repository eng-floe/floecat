package ai.floedb.metacat.service.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.query.rpc.BeginQueryRequest;
import ai.floedb.metacat.query.rpc.QueryInput;
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

@QuarkusTest
class QueryServiceSchemaIT {

  @GrpcClient("metacat")
  QueryServiceGrpc.QueryServiceBlockingStub queries;

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
  void beginQueryReturnsSnapshotSchema() {
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

    var snap1 =
        TestSupport.createSnapshot(
            snapshot, tbl.getResourceId(), 1L, System.currentTimeMillis() - 10_000L);

    // update schema to v2 and create snapshot2
    table.updateTable(
        UpdateTableRequest.newBuilder()
            .setTableId(tbl.getResourceId())
            .setSpec(
                TableSpec.newBuilder()
                    .setSchemaJson(SchemaParser.toJson(schemaV2))
                    .setUpstream(tbl.getUpstream()))
            .setUpdateMask(FieldMask.newBuilder().addPaths("schema_json").build())
            .build());

    var snap2 =
        TestSupport.createSnapshot(
            snapshot, tbl.getResourceId(), 2L, System.currentTimeMillis() - 5_000L);

    var name =
        NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addPath("db")
            .addPath("sch")
            .setName("orders")
            .build();

    var beginSnap1 =
        queries.beginQuery(
            BeginQueryRequest.newBuilder()
                .addInputs(
                    QueryInput.newBuilder()
                        .setName(name)
                        .setTableId(tbl.getResourceId())
                        .setSnapshot(
                            SnapshotRef.newBuilder().setSnapshotId(snap1.getSnapshotId()).build())
                        .build())
                .setIncludeSchema(true)
                .build());

    assertEquals(1, beginSnap1.getSchema().getColumnsCount(), "snapshot1 schema should be v1");

    var beginSnap2 =
        queries.beginQuery(
            BeginQueryRequest.newBuilder()
                .addInputs(
                    QueryInput.newBuilder()
                        .setName(name)
                        .setTableId(tbl.getResourceId())
                        .setSnapshot(
                            SnapshotRef.newBuilder().setSnapshotId(snap2.getSnapshotId()).build())
                        .build())
                .setIncludeSchema(true)
                .build());
    assertEquals(2, beginSnap2.getSchema().getColumnsCount(), "snapshot2 schema should be v2");
    assertTrue(
        beginSnap2.getSchema().getColumnsList().stream().anyMatch(c -> c.getName().equals("qty")),
        "v2 column should appear in snapshot2 schema");
  }
}
