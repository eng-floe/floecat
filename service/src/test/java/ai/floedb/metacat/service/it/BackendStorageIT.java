package ai.floedb.metacat.service.it;

import java.time.Clock;
import java.util.List;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

@QuarkusTest
class BackendStorageIT {

  @GrpcClient("resource-mutation")
  ResourceMutationGrpc.ResourceMutationBlockingStub mutation;

  @GrpcClient("directory")
  DirectoryGrpc.DirectoryBlockingStub directory;

  @Inject PointerStore ptr;
  @Inject BlobStore blobs;

  private final Clock clock = Clock.systemUTC();

  @Test
  void storage_invariants_pointerBlobVersion_indexKeys_idempotence_and_deletes() {
    String tenantId = TestSupport.seedTenantId(directory, "sales");

    String catName = "it_storage_cat_" + clock.millis();
    Catalog cat = TestSupport.createCatalog(mutation, catName, "storage cat");
    ResourceId catId = cat.getResourceId();

    var nsPath = List.of("db_it", "schema_it");
    Namespace ns = TestSupport.createNamespace(mutation, catId, "it_ns", nsPath, "storage ns");
    ResourceId nsId = ns.getResourceId();

    String schemaV1 = """
        {"type":"struct","fields":[{"name":"id","type":"long"}]}
        """.trim();
    TableDescriptor tbl = TestSupport.createTable(
        mutation, catId, nsId, "it_tbl", "s3://bucket/prefix/it", schemaV1, "storage table");
    ResourceId tblId = tbl.getResourceId();

    String canonPtrKey = Keys.tblCanonicalPtr(tenantId, tblId.getId());
    String nsIdxPtrKey = Keys.tblIndexPtr(tenantId, catId.getId(), nsId.getId(), tblId.getId());
    String tblBlobUri  = Keys.tblBlob(tenantId, tblId.getId());

    Pointer tpCanon = ptr.get(canonPtrKey).orElseThrow(() -> new AssertionError("canonical pointer missing"));
    Pointer tpNsIdx = ptr.get(nsIdxPtrKey).orElseThrow(() -> new AssertionError("ns-index pointer missing"));
    assertEquals(tblBlobUri, tpCanon.getBlobUri());
    assertEquals(tblBlobUri, tpNsIdx.getBlobUri());
    assertTrue(blobs.head(tblBlobUri).isPresent(), "table blob header missing");

    long vCanonBefore = tpCanon.getVersion();
    String schemaV2 = """
        {"type":"struct","fields":[{"name":"id","type":"long"},{"name":"amount","type":"double"}]}
        """.trim();
    TestSupport.updateSchema(mutation, tblId, schemaV2);

    Pointer tpCanonAfter = ptr.get(canonPtrKey).orElseThrow();
    assertTrue(tpCanonAfter.getVersion() > vCanonBefore, "version must bump on content change");
    assertEquals(tblBlobUri, tpCanonAfter.getBlobUri(), "blob URI stable; content updated behind it");

    long vBeforeIdempotent = tpCanonAfter.getVersion();
    TestSupport.updateSchema(mutation, tblId, schemaV2);
    Pointer tpCanonAfterIdem = ptr.get(canonPtrKey).orElseThrow();
    assertEquals(vBeforeIdempotent, tpCanonAfterIdem.getVersion(), "version must NOT bump on identical content");

    String oldName = tbl.getDisplayName();
    String newName = "it_tbl_renamed";
    TestSupport.renameTable(mutation, tblId, newName);

    String oldFq = String.join("/", catName, String.join("/", nsPath), oldName);
    String newFq = String.join("/", catName, String.join("/", nsPath), newName);

    String idxOldKey = Keys.idxTblByName(tenantId, oldFq);
    String idxNewKey = Keys.idxTblByName(tenantId, newFq);

    assertTrue(ptr.get(idxNewKey).isPresent(), "new name-index pointer must exist");
    assertTrue(ptr.get(idxOldKey).isEmpty(), "old name-index pointer must be removed");

    TestSupport.deleteTable(mutation, tblId);
    assertTrue(ptr.get(canonPtrKey).isEmpty(), "canonical pointer should be deleted");
    assertTrue(ptr.get(nsIdxPtrKey).isEmpty(), "ns-index pointer should be deleted");
    assertTrue(blobs.head(tblBlobUri).isEmpty(), "table blob should be deleted");

    TestSupport.deleteNamespace(mutation, nsId, true);
    String nsPtrKey = Keys.nsPtr(tenantId, catId.getId(), nsId.getId());
    String nsBlobUri = Keys.nsBlob(tenantId, catId.getId(), nsId.getId());
    assertTrue(ptr.get(nsPtrKey).isEmpty(), "namespace pointer should be deleted");
    assertTrue(blobs.head(nsBlobUri).isEmpty(), "namespace blob should be deleted");

    TestSupport.deleteCatalog(mutation, catId, true);
    String catPtrKey = Keys.catPtr(tenantId, catId.getId());
    String catBlobUri = Keys.catBlob(tenantId, catId.getId());
    assertTrue(ptr.get(catPtrKey).isEmpty(), "catalog pointer should be deleted");
    assertTrue(blobs.head(catBlobUri).isEmpty(), "catalog blob should be deleted");
  }
}
