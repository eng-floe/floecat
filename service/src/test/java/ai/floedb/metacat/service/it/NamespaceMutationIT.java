package ai.floedb.metacat.service.it;

import java.util.ArrayList;
import java.util.List;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.ErrorCode;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

@QuarkusTest
class NamespaceMutationIT {
  @Inject PointerStore ptr;
  @Inject BlobStore blob;

  @GrpcClient("resource-mutation")
  ResourceMutationGrpc.ResourceMutationBlockingStub mutation;

  @GrpcClient("resource-access")
  ResourceAccessGrpc.ResourceAccessBlockingStub access;

  @GrpcClient("directory")
  DirectoryGrpc.DirectoryBlockingStub directory;

  String NS_PREFIX = this.getClass().getSimpleName() + "_";

  @Test
  void namespace_exists() throws Exception {
    var cat = TestSupport.createCatalog(mutation, NS_PREFIX + "cat1", "cat1");

    var ns = TestSupport.createNamespace(
        mutation, cat.getResourceId(), "2025", List.of("staging"), "2025 ns");

    StatusRuntimeException nsExists = assertThrows(
        StatusRuntimeException.class,
        () -> TestSupport.createNamespace(
            mutation, cat.getResourceId(), "2025",
                List.of("staging"), "2025 ns"));
    TestSupport.assertGrpcAndMc(
        nsExists, Status.Code.ABORTED, ErrorCode.MC_CONFLICT,
            "Namespace \"staging/2025\" already exists");
  }

  @Test
  void namespace_create_rename_move_delete_with_preconditions() throws Exception {
    var cat = TestSupport.createCatalog(mutation, NS_PREFIX + "cat2", "cat2");
    String tenantId = TestSupport.seedTenantId(directory, NS_PREFIX + "cat2");
    assertEquals(tenantId, cat.getResourceId().getTenantId());

    var parents = List.of("db_it","schema_it");
    var leaf = "it_schema";
    var ns = TestSupport.createNamespace(
        mutation, cat.getResourceId(), leaf, parents, "ns desc");
    ResourceId nsId = ns.getResourceId();
    assertEquals(ResourceKind.RK_NAMESPACE, nsId.getKind());

    var full = new ArrayList<>(parents);
    full.add(leaf);
    var resolved = directory.resolveNamespace(ResolveNamespaceRequest.newBuilder()
        .setRef(NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addAllPath(full))
        .build());
    assertEquals(nsId.getId(), resolved.getResourceId().getId());

    var m1 = mutation.renameNamespace(RenameNamespaceRequest.newBuilder()
        .setNamespaceId(nsId)
        .setNewDisplayName(leaf + "_ren")
        .setPrecondition(Precondition.newBuilder()
            .setExpectedVersion(TestSupport.metaForNamespace(
                ptr, blob, tenantId, cat.getDisplayName(), full).getPointerVersion())
            .setExpectedEtag(TestSupport.metaForNamespace(
                ptr, blob, tenantId, cat.getDisplayName(), full).getEtag())
            .build())
        .build()).getMeta();

    var fullRen = new ArrayList<>(parents); fullRen.add(leaf + "_ren");
    var resolvedRen = directory.resolveNamespace(ResolveNamespaceRequest.newBuilder()
        .setRef(NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addAllPath(fullRen))
        .build());
    assertEquals(nsId.getId(), resolvedRen.getResourceId().getId());

    var m2Resp = mutation.renameNamespace(RenameNamespaceRequest.newBuilder()
        .setNamespaceId(nsId)
        .addAllNewPath(List.of(leaf + "_root"))
        .setPrecondition(Precondition.newBuilder()
            .setExpectedVersion(m1.getPointerVersion())
            .setExpectedEtag(m1.getEtag())
            .build())
        .build());
    var m2 = m2Resp.getMeta();
    assertTrue(m2.getPointerVersion() > m1.getPointerVersion());

    var resolvedRoot = directory.resolveNamespace(ResolveNamespaceRequest.newBuilder()
        .setRef(NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addPath(leaf + "_root"))
        .build());
    assertEquals(nsId.getId(), resolvedRoot.getResourceId().getId());

    var bad = assertThrows(StatusRuntimeException.class, () ->
        mutation.renameNamespace(RenameNamespaceRequest.newBuilder()
            .setNamespaceId(nsId)
            .setNewDisplayName(leaf + "_root2")
            .setPrecondition(Precondition.newBuilder()
                .setExpectedVersion(123456L)
                .setExpectedEtag("bogus")
                .build())
            .build()));
    TestSupport.assertGrpcAndMc(
        bad, Status.Code.FAILED_PRECONDITION,
            ErrorCode.MC_PRECONDITION_FAILED, "mismatch");

    var before = TestSupport.metaForNamespace(
        ptr, blob, tenantId, cat.getDisplayName(), List.of(leaf + "_root"));

    // Bump the version
    var m3Resp = mutation.renameNamespace(RenameNamespaceRequest.newBuilder()
        .setNamespaceId(nsId)
        .setNewDisplayName(leaf + "_root3")
        .setPrecondition(Precondition.newBuilder()
          .setExpectedVersion(before.getPointerVersion())
          .setExpectedEtag(before.getEtag())
          .build())
        .build());
    var m3 = m3Resp.getMeta();

    // Now try to delete with the stale precondition
    var stale = assertThrows(StatusRuntimeException.class, () ->
        mutation.deleteNamespace(DeleteNamespaceRequest.newBuilder()
            .setNamespaceId(nsId)
            .setRequireEmpty(true)
            .setPrecondition(Precondition.newBuilder()
              .setExpectedVersion(before.getPointerVersion()) // stale
              .setExpectedEtag(before.getEtag())              // stale
              .build())
            .build())
    );

    TestSupport.assertGrpcAndMc(stale, Status.Code.FAILED_PRECONDITION,
        ErrorCode.MC_PRECONDITION_FAILED, "mismatch");

    var tbl = TestSupport.createTable(
        mutation, cat.getResourceId(), nsId, "orders",
            "s3://ns/orders", "{}", "none");

    StatusRuntimeException nsDelBlocked = assertThrows(StatusRuntimeException.class, () ->
        mutation.deleteNamespace(DeleteNamespaceRequest.newBuilder()
            .setNamespaceId(nsId)
            .setRequireEmpty(true)
            .setPrecondition(Precondition.newBuilder()
                .setExpectedVersion(m3.getPointerVersion())
                .setExpectedEtag(m3.getEtag())
                .build())
            .build()));
    TestSupport.assertGrpcAndMc(
        nsDelBlocked, Status.Code.ABORTED, ErrorCode.MC_CONFLICT,
        "Namespace \"" + leaf + "_root3" + "\" contains tables and/or children.");

    TestSupport.deleteTable(mutation, nsId, tbl.getResourceId());

    var delOk = mutation.deleteNamespace(DeleteNamespaceRequest.newBuilder()
        .setNamespaceId(nsId)
        .setRequireEmpty(true)
        .setPrecondition(Precondition.newBuilder()
            .setExpectedVersion(m3.getPointerVersion())
            .setExpectedEtag(m3.getEtag())
            .build())
        .build());
    assertFalse(delOk.getMeta().getPointerKey().isEmpty());

    var nf = assertThrows(
        StatusRuntimeException.class,
        () -> directory.resolveNamespace(ResolveNamespaceRequest.newBuilder()
            .setRef(NameRef.newBuilder()
                .setCatalog(cat.getDisplayName())
                .addPath(leaf + "_root3"))
            .build()));
    TestSupport.assertGrpcAndMc(nf, Status.Code.NOT_FOUND,
        ErrorCode.MC_NOT_FOUND, "Namespace not found");
  }

  @Test
  void namespace_create_is_idempotent_sameKey_sameSpec() throws Exception {
    var cat = TestSupport.createCatalog(mutation, NS_PREFIX + "cat3", "cat3");
    TestSupport.seedTenantId(directory,  cat.getDisplayName());

    var key = IdempotencyKey.newBuilder().setKey(NS_PREFIX + "k-ns-1").build();
    var spec = NamespaceSpec.newBuilder()
        .setCatalogId(cat.getResourceId())
        .setDisplayName("idem_ns")
        .addAllPath(List.of("staging"))
        .setDescription("x")
        .build();

    var r1 = mutation.createNamespace(CreateNamespaceRequest.newBuilder()
        .setSpec(spec).setIdempotency(key).build());
    var r2 = mutation.createNamespace(CreateNamespaceRequest.newBuilder()
        .setSpec(spec).setIdempotency(key).build());

    assertEquals(r1.getNamespace().getResourceId().getId(),
        r2.getNamespace().getResourceId().getId());
    assertEquals(r1.getMeta().getPointerKey(), r2.getMeta().getPointerKey());
    assertEquals(r1.getMeta().getPointerVersion(), r2.getMeta().getPointerVersion());
    assertEquals(r1.getMeta().getEtag(), r2.getMeta().getEtag());
  }

  @Test
  void namespace_create_idempotency_mismatch_sameKey_differentSpec_conflict() throws Exception {
    var cat = TestSupport.createCatalog(mutation, NS_PREFIX + "cat4", "cat4");
    var key = IdempotencyKey.newBuilder().setKey(NS_PREFIX + "k-ns-2").build();

    mutation.createNamespace(CreateNamespaceRequest.newBuilder()
        .setSpec(NamespaceSpec.newBuilder()
            .setCatalogId(cat.getResourceId())
            .setDisplayName("idem_ns2")
            .addAllPath(List.of("db"))
            .build())
        .setIdempotency(key)
        .build());

    var ex = assertThrows(StatusRuntimeException.class, () ->
        mutation.createNamespace(CreateNamespaceRequest.newBuilder()
            .setSpec(NamespaceSpec.newBuilder()
                .setCatalogId(cat.getResourceId())
                .setDisplayName("idem_ns2_DIFFERENT")
                .addAllPath(List.of("db"))
                .build())
            .setIdempotency(key)
            .build()));
    TestSupport.assertGrpcAndMc(ex, Status.Code.ABORTED,
        ErrorCode.MC_CONFLICT, "Idempotency key mismatch");
  }
}
