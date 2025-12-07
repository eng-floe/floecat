package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.*;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class ResourceMutationIT {
  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  private final Clock clock = Clock.systemUTC();

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void resourcesExist() throws Exception {
    var cat = TestSupport.createCatalog(catalog, "cat1", "cat1");
    assertDoesNotThrow(() -> TestSupport.createCatalog(catalog, "cat1", "cat1 catalog"));

    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "2025", List.of("staging"), "2025 ns");
    assertDoesNotThrow(
        () ->
            TestSupport.createNamespace(
                namespace, cat.getResourceId(), "2025", List.of("staging"), "2025 namespace"));

    TestSupport.createTable(
        table, cat.getResourceId(), ns.getResourceId(), "events", "s3://events", "{}", "none");

    assertDoesNotThrow(
        () ->
            TestSupport.createTable(
                table,
                cat.getResourceId(),
                ns.getResourceId(),
                "events",
                "s3://events",
                "{}",
                "A description"));
  }

  @Test
  void catalogCreateUpdateDelete() throws Exception {
    String catName = "it_mutation_cat_" + clock.millis();
    Catalog cat = TestSupport.createCatalog(catalog, catName, "IT cat");
    ResourceId catId = cat.getResourceId();

    assertEquals(ResourceKind.RK_CATALOG, catId.getKind());
    assertTrue(catId.getId().matches("^[0-9a-fA-F-]{36}$"), "id must look like UUID");

    assertEquals(catId.getId(), TestSupport.resolveCatalogId(directory, catName).getId());
    assertEquals(
        catName,
        catalog
            .getCatalog(GetCatalogRequest.newBuilder().setCatalogId(catId).build())
            .getCatalog()
            .getDisplayName());

    var nsPath = List.of("db_it", "schema_it");
    String nsLeaf = "it_schema";
    Namespace ns = TestSupport.createNamespace(namespace, catId, nsLeaf, nsPath, "IT ns");
    ResourceId nsId = ns.getResourceId();
    var nsFullPath = new ArrayList<>(nsPath);
    nsFullPath.add(nsLeaf);
    assertEquals(
        nsId.getId(), TestSupport.resolveNamespaceId(directory, catName, nsFullPath).getId());

    String schema =
        """
        {"type":"struct","fields":[{"name":"id","type":"long"}]}
        """
            .trim();
    Table tbl =
        TestSupport.createTable(
            table, catId, nsId, "orders_it", "s3://bucket/prefix/it", schema, "IT table");
    ResourceId tblId = tbl.getResourceId();
    assertEquals(
        tblId.getId(),
        TestSupport.resolveTableId(directory, catName, nsFullPath, "orders_it").getId());

    String schemaV2 =
        """
        {"type":"struct","fields":[{"name":"id","type":"long"},{"name":"amount","type":"double"}]}
        """
            .trim();
    Table upd = TestSupport.updateSchema(table, tblId, schemaV2);
    assertEquals(schemaV2, upd.getSchemaJson());

    String newName = "orders_it_renamed";
    Table renamed = TestSupport.renameTable(table, tblId, newName);
    assertEquals(newName, renamed.getDisplayName());
    assertEquals(
        tblId.getId(), TestSupport.resolveTableId(directory, catName, nsFullPath, newName).getId());

    StatusRuntimeException oldName404 =
        assertThrows(
            StatusRuntimeException.class,
            () -> TestSupport.resolveTableId(directory, catName, nsFullPath, "orders_it"));
    TestSupport.assertGrpcAndMc(oldName404, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, null);

    StatusRuntimeException nsDelBlocked =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.deleteNamespace(
                    DeleteNamespaceRequest.newBuilder()
                        .setNamespaceId(nsId)
                        .setRequireEmpty(true)
                        .build()));
    TestSupport.assertGrpcAndMc(
        nsDelBlocked,
        Status.Code.ABORTED,
        ErrorCode.MC_CONFLICT,
        "Namespace \"db_it.schema_it.it_schema\" contains tables and/or children.");

    TestSupport.deleteTable(table, nsId, tblId);

    StatusRuntimeException tblGone =
        assertThrows(
            StatusRuntimeException.class,
            () -> TestSupport.resolveTableId(directory, catName, nsFullPath, newName));
    TestSupport.assertGrpcAndMc(tblGone, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, null);

    TestSupport.deleteNamespace(namespace, nsId, true);

    var schemaNsId =
        TestSupport.resolveNamespaceId(
            directory, cat.getDisplayName(), List.of("db_it", "schema_it"));
    TestSupport.deleteNamespace(namespace, schemaNsId, true);

    var dbNsId = TestSupport.resolveNamespaceId(directory, cat.getDisplayName(), List.of("db_it"));
    TestSupport.deleteNamespace(namespace, dbNsId, true);

    TestSupport.deleteCatalog(catalog, catId, true);

    StatusRuntimeException catGone =
        assertThrows(
            StatusRuntimeException.class, () -> TestSupport.resolveCatalogId(directory, catName));
    TestSupport.assertGrpcAndMc(
        catGone, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "Catalog not found");
  }

  @Test
  void catalogCreateUpdateDeletePrecondition() throws Exception {
    var c1 = TestSupport.createCatalog(catalog, "cat_pre", "desc");
    var id = c1.getResourceId();
    FieldMask mask_name_desc =
        FieldMask.newBuilder().addAllPaths(List.of("display_name", "description")).build();
    var m1 =
        catalog
            .updateCatalog(
                UpdateCatalogRequest.newBuilder()
                    .setCatalogId(id)
                    .setSpec(
                        CatalogSpec.newBuilder()
                            .setDisplayName("cat_pre")
                            .setDescription("desc")
                            .build())
                    .setUpdateMask(mask_name_desc)
                    .build())
            .getMeta();

    var resolved =
        directory.resolveCatalog(
            ResolveCatalogRequest.newBuilder()
                .setRef(NameRef.newBuilder().setCatalog("cat_pre"))
                .build());
    assertEquals(id.getId(), resolved.getResourceId().getId());

    var spec2 =
        CatalogSpec.newBuilder().setDisplayName("cat_pre_2").setDescription("desc2").build();
    var updOk =
        catalog.updateCatalog(
            UpdateCatalogRequest.newBuilder()
                .setCatalogId(id)
                .setSpec(spec2)
                .setUpdateMask(mask_name_desc)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(m1.getPointerVersion())
                        .setExpectedEtag(m1.getEtag())
                        .build())
                .build());
    assertEquals("cat_pre_2", updOk.getCatalog().getDisplayName());
    assertTrue(updOk.getMeta().getPointerVersion() > m1.getPointerVersion());

    FieldMask mask_name = FieldMask.newBuilder().addAllPaths(List.of("display_name")).build();
    var bad =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                catalog.updateCatalog(
                    UpdateCatalogRequest.newBuilder()
                        .setCatalogId(id)
                        .setSpec(CatalogSpec.newBuilder().setDisplayName("cat_pre_3"))
                        .setUpdateMask(mask_name)
                        .setPrecondition(
                            Precondition.newBuilder()
                                .setExpectedVersion(123456L)
                                .setExpectedEtag("bogus")
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        bad, Status.Code.FAILED_PRECONDITION, ErrorCode.MC_PRECONDITION_FAILED, null);

    var m2 = updOk.getMeta();
    var delOk =
        catalog.deleteCatalog(
            DeleteCatalogRequest.newBuilder()
                .setCatalogId(id)
                .setRequireEmpty(true)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(m2.getPointerVersion())
                        .setExpectedEtag(m2.getEtag())
                        .build())
                .build());
    assertEquals(m2.getPointerKey(), delOk.getMeta().getPointerKey());

    var notFound =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                directory.resolveCatalog(
                    ResolveCatalogRequest.newBuilder()
                        .setRef(NameRef.newBuilder().setCatalog("cat_pre_2"))
                        .build()));
    TestSupport.assertGrpcAndMc(
        notFound, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "Catalog not found");
  }
}
