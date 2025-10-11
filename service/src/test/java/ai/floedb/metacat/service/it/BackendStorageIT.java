package ai.floedb.metacat.service.it;

import java.time.Clock;
import java.util.List;

import com.google.protobuf.Any;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

@QuarkusTest
class BackendStorageIT {

  @GrpcClient("resource-mutation")
  ai.floedb.metacat.catalog.rpc.ResourceMutationGrpc.ResourceMutationBlockingStub mutation;

  @GrpcClient("directory")
  DirectoryGrpc.DirectoryBlockingStub directory;

  @GrpcClient("resource-access")
  ResourceAccessGrpc.ResourceAccessBlockingStub access;

  @Inject PointerStore ptr;
  @Inject BlobStore blobs;

  private final Clock clock = Clock.systemUTC();

  private static ai.floedb.metacat.common.rpc.Error unpackMcError(StatusRuntimeException ex) throws Exception {
    var st = StatusProto.fromThrowable(ex);
    if (st == null) return null;
    for (Any any : st.getDetailsList()) {
      if (any.is(ai.floedb.metacat.common.rpc.Error.class)) {
        return any.unpack(ai.floedb.metacat.common.rpc.Error.class);
      }
    }
    return null;
  }

  @Test
  void storageLifecycle_catalog_namespace_table_pointerAndBlob() throws Exception {
    var ref = NameRef.newBuilder().setCatalog("sales").build();
    var seeded = directory.resolveCatalog(ResolveCatalogRequest.newBuilder().setRef(ref).build());
    var tenantId = seeded.getResourceId().getTenantId();

    var catName = "it_storage_cat_" + clock.millis();
    ref = NameRef.newBuilder().setCatalog(catName).build();
    var cResp = mutation.createCatalog(CreateCatalogRequest.newBuilder()
      .setSpec(CatalogSpec.newBuilder().setDisplayName(catName).setDescription("storage cat").build())
      .build());

    var catRid = cResp.getCatalog().getResourceId();
    var catId = catRid.getId();

    String catPtrKey = Keys.catPtr(tenantId, catId);
    String catBlobUri = Keys.catBlob(tenantId, catId);

    Pointer p0 = ptr.get(catPtrKey).orElseThrow(() -> new AssertionError("catalog pointer missing"));
    assertEquals(catPtrKey, p0.getKey());
    assertEquals(catBlobUri, p0.getBlobUri());
    assertTrue(p0.getVersion() >= 1);

    assertTrue(blobs.head(catBlobUri).isPresent(), "catalog blob header missing");

    var resolved = directory.resolveCatalog(ResolveCatalogRequest.newBuilder().setRef(ref).build());
    assertEquals(catId, resolved.getResourceId().getId());

    var nsName = "it_ns";
    var nResp = mutation.createNamespace(CreateNamespaceRequest.newBuilder()
      .setSpec(NamespaceSpec.newBuilder()
        .setCatalogId(catRid)
        .setDisplayName(nsName)
        .addAllPath(List.of("db_it", "schema_it"))
        .setDescription("storage ns")
        .build())
      .build());

    var nsRid = nResp.getNamespace().getResourceId();
    var nsId = nsRid.getId();

    String nsPtrKey = Keys.nsPtr(tenantId, catId, nsId);
    String nsBlobUri = Keys.nsBlob(tenantId, catId, nsId);

    Pointer np = ptr.get(nsPtrKey).orElseThrow(() -> new AssertionError("namespace pointer missing"));
    assertEquals(nsBlobUri, np.getBlobUri());
    assertTrue(blobs.head(nsBlobUri).isPresent(), "namespace blob header missing");

    var tblName = "it_tbl";
    var tResp = mutation.createTable(CreateTableRequest.newBuilder()
      .setSpec(TableSpec.newBuilder()
        .setCatalogId(catRid)
        .setNamespaceId(nsRid)
        .setDisplayName(tblName)
        .setDescription("storage table")
        .setRootUri("s3://bucket/prefix/it")
        .setSchemaJson("{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}")
        .build())
      .build());

    var tbl = tResp.getTable();
    var tblRid = tbl.getResourceId();
    var tblId = tblRid.getId();

    String canonPtrKey = Keys.tblCanonicalPtr(tenantId, tblId);
    String nsIdxPtrKey = Keys.tblIndexPtr(tenantId, catId, nsId, tblId);
    String tblBlobUri = Keys.tblBlob(tenantId, tblId);

    Pointer tpCanon = ptr.get(canonPtrKey).orElseThrow(() -> new AssertionError("table canonical pointer missing"));
    Pointer tpNsIdx = ptr.get(nsIdxPtrKey).orElseThrow(() -> new AssertionError("table ns-index pointer missing"));
    assertEquals(tblBlobUri, tpCanon.getBlobUri());
    assertEquals(tblBlobUri, tpNsIdx.getBlobUri());
    assertTrue(blobs.head(tblBlobUri).isPresent(), "table blob header missing");

    long vCanonBefore = tpCanon.getVersion();

    var newSchema = "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"a\",\"type\":\"double\"}]}";
    mutation.updateTableSchema(UpdateTableSchemaRequest.newBuilder()
      .setTableId(tblRid)
      .setSchemaJson(newSchema)
      .build());

    Pointer tpCanonAfter = ptr.get(canonPtrKey).orElseThrow();
    assertTrue(tpCanonAfter.getVersion() > vCanonBefore, "canonical pointer version did not bump on schema update");
    assertEquals(tblBlobUri, tpCanonAfter.getBlobUri(), "expect same blob URI keyspace; content updated behind it");

    var newName = "it_tbl_renamed";
    mutation.renameTable(RenameTableRequest.newBuilder()
      .setTableId(tblRid)
      .setNewDisplayName(newName)
      .build());

    var tNew = directory.resolveTable(ResolveTableRequest.newBuilder()
      .setRef(NameRef.newBuilder()
        .setCatalog(catName)
        .addAllPath(List.of("db_it", "schema_it"))
        .setName(newName)
        .build())
      .build());
      assertEquals(tblId, tNew.getResourceId().getId());

    assertThrows(StatusRuntimeException.class, () ->
      directory.resolveTable(ResolveTableRequest.newBuilder()
        .setRef(NameRef.newBuilder()
          .setCatalog(catName)
          .addAllPath(List.of("db_it", "schema_it"))
          .setName(tblName)
          .build())
        .build()));

    mutation.deleteTable(DeleteTableRequest.newBuilder().setTableId(tblRid).build());

    assertTrue(ptr.get(canonPtrKey).isEmpty(), "canonical pointer should be deleted");
    assertTrue(ptr.get(nsIdxPtrKey).isEmpty(), "ns-index pointer should be deleted");
    assertTrue(blobs.head(tblBlobUri).isEmpty(), "table blob should be deleted");

    var t2 = mutation.createTable(CreateTableRequest.newBuilder()
      .setSpec(TableSpec.newBuilder()
        .setCatalogId(catRid)
        .setNamespaceId(nsRid)
        .setDisplayName("hold_me")
        .setDescription("tmp")
        .setRootUri("s3://bucket/tmp")
        .setSchemaJson("{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}")
        .build())
      .build());

    var nsFail = assertThrows(StatusRuntimeException.class, () ->
      mutation.deleteNamespace(DeleteNamespaceRequest.newBuilder()
        .setNamespaceId(nsRid)
        .setRequireEmpty(true)
        .build()));
    var mc = unpackMcError(nsFail);
    assertNotNull(mc);
    assertTrue(mc.getMessage().contains("Namespace contains tables"));

    mutation.deleteTable(DeleteTableRequest.newBuilder().setTableId(t2.getTable().getResourceId()).build());

    mutation.deleteNamespace(DeleteNamespaceRequest.newBuilder().setNamespaceId(nsRid).setRequireEmpty(true).build());
    assertTrue(ptr.get(nsPtrKey).isEmpty(), "namespace pointer should be deleted");
    assertTrue(blobs.head(nsBlobUri).isEmpty(), "namespace blob should be deleted");

    mutation.deleteCatalog(DeleteCatalogRequest.newBuilder().setCatalogId(catRid).setRequireEmpty(true).build());
    assertTrue(ptr.get(catPtrKey).isEmpty(), "catalog pointer should be deleted");
    assertTrue(blobs.head(catBlobUri).isEmpty(), "catalog blob should be deleted");
  }
}