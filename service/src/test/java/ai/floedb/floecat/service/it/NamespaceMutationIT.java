package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.*;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.PointerStore;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class NamespaceMutationIT {
  @Inject PointerStore ptr;
  @Inject BlobStore blob;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  String namespacePrefix = this.getClass().getSimpleName() + "_";

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void namespaceExists() throws Exception {
    var cat = TestSupport.createCatalog(catalog, namespacePrefix + "cat1", "cat1");

    TestSupport.createNamespace(
        namespace, cat.getResourceId(), "2025", List.of("staging"), "2025 ns");

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                TestSupport.createNamespace(
                    namespace, cat.getResourceId(), "2025", List.of("staging"), "2025 namespace"));
    TestSupport.assertGrpcAndMc(ex, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "already exists");
  }

  @Test
  void namespaceCreateRenameDelete() throws Exception {
    var cat = TestSupport.createCatalog(catalog, namespacePrefix + "cat2", "cat2");

    var parents = List.of("db_it", "schema_it");
    var leaf = "it_schema";
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), leaf, parents, "ns desc");
    ResourceId nsId = ns.getResourceId();
    assertEquals(ResourceKind.RK_NAMESPACE, nsId.getKind());

    var full = new ArrayList<>(parents);
    full.add(leaf);
    var resolved =
        directory.resolveNamespace(
            ResolveNamespaceRequest.newBuilder()
                .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName()).addAllPath(full))
                .build());
    assertEquals(nsId.getId(), resolved.getResourceId().getId());

    FieldMask mask_name = FieldMask.newBuilder().addPaths("display_name").build();
    var nsSpec = NamespaceSpec.newBuilder().setDisplayName(leaf + "_ren").build();
    var m1 =
        namespace
            .updateNamespace(
                UpdateNamespaceRequest.newBuilder()
                    .setNamespaceId(nsId)
                    .setSpec(nsSpec)
                    .setUpdateMask(mask_name)
                    .setPrecondition(
                        Precondition.newBuilder()
                            .setExpectedVersion(
                                TestSupport.metaForNamespace(
                                        ptr,
                                        blob,
                                        cat.getResourceId().getAccountId(),
                                        cat.getDisplayName(),
                                        full)
                                    .getPointerVersion())
                            .setExpectedEtag(
                                TestSupport.metaForNamespace(
                                        ptr,
                                        blob,
                                        cat.getResourceId().getAccountId(),
                                        cat.getDisplayName(),
                                        full)
                                    .getEtag())
                            .build())
                    .build())
            .getMeta();

    var fullRen = new ArrayList<>(parents);
    fullRen.add(leaf + "_ren");
    var resolvedRen =
        directory.resolveNamespace(
            ResolveNamespaceRequest.newBuilder()
                .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName()).addAllPath(fullRen))
                .build());
    assertEquals(nsId.getId(), resolvedRen.getResourceId().getId());

    FieldMask mask_path = FieldMask.newBuilder().addPaths("path").build();
    var m2Spec = NamespaceSpec.newBuilder().addAllPath(List.of(leaf + "_root")).build();
    var m2Resp =
        namespace.updateNamespace(
            UpdateNamespaceRequest.newBuilder()
                .setNamespaceId(nsId)
                .setSpec(m2Spec)
                .setUpdateMask(mask_path)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(m1.getPointerVersion())
                        .setExpectedEtag(m1.getEtag())
                        .build())
                .build());
    var m2 = m2Resp.getMeta();
    assertTrue(m2.getPointerVersion() > m1.getPointerVersion());

    var resolvedRoot =
        directory.resolveNamespace(
            ResolveNamespaceRequest.newBuilder()
                .setRef(
                    NameRef.newBuilder().setCatalog(cat.getDisplayName()).addPath(leaf + "_root"))
                .build());
    assertEquals(nsId.getId(), resolvedRoot.getResourceId().getId());

    var badSpec = NamespaceSpec.newBuilder().setDisplayName(leaf + "_root2").build();
    var bad =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.updateNamespace(
                    UpdateNamespaceRequest.newBuilder()
                        .setNamespaceId(nsId)
                        .setSpec(badSpec)
                        .setUpdateMask(mask_name)
                        .setPrecondition(
                            Precondition.newBuilder()
                                .setExpectedVersion(123456L)
                                .setExpectedEtag("bogus")
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        bad, Status.Code.FAILED_PRECONDITION, ErrorCode.MC_PRECONDITION_FAILED, "mismatch");

    var before =
        TestSupport.metaForNamespace(
            ptr,
            blob,
            cat.getResourceId().getAccountId(),
            cat.getDisplayName(),
            List.of(leaf + "_root"));

    // Bump the version
    var m3Spec = NamespaceSpec.newBuilder().setDisplayName(leaf + "_root3").build();
    var m3Resp =
        namespace.updateNamespace(
            UpdateNamespaceRequest.newBuilder()
                .setNamespaceId(nsId)
                .setSpec(m3Spec)
                .setUpdateMask(mask_name)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(before.getPointerVersion())
                        .setExpectedEtag(before.getEtag())
                        .build())
                .build());
    var m3 = m3Resp.getMeta();

    // Now try to delete with the stale precondition
    var stale =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.deleteNamespace(
                    DeleteNamespaceRequest.newBuilder()
                        .setNamespaceId(nsId)
                        .setRequireEmpty(true)
                        .setPrecondition(
                            Precondition.newBuilder()
                                .setExpectedVersion(before.getPointerVersion()) // stale
                                .setExpectedEtag(before.getEtag()) // stale
                                .build())
                        .build()));

    TestSupport.assertGrpcAndMc(
        stale, Status.Code.FAILED_PRECONDITION, ErrorCode.MC_PRECONDITION_FAILED, "mismatch");

    var tbl =
        TestSupport.createTable(
            table, cat.getResourceId(), nsId, "orders", "s3://ns/orders", "{}", "none");

    StatusRuntimeException nsDelBlocked =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.deleteNamespace(
                    DeleteNamespaceRequest.newBuilder()
                        .setNamespaceId(nsId)
                        .setRequireEmpty(true)
                        .setPrecondition(
                            Precondition.newBuilder()
                                .setExpectedVersion(m3.getPointerVersion())
                                .setExpectedEtag(m3.getEtag())
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        nsDelBlocked,
        Status.Code.ABORTED,
        ErrorCode.MC_CONFLICT,
        "Namespace \"" + leaf + "_root3" + "\" contains tables and/or children.");

    TestSupport.deleteTable(table, nsId, tbl.getResourceId());

    var delOk =
        namespace.deleteNamespace(
            DeleteNamespaceRequest.newBuilder()
                .setNamespaceId(nsId)
                .setRequireEmpty(true)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(m3.getPointerVersion())
                        .setExpectedEtag(m3.getEtag())
                        .build())
                .build());
    assertFalse(delOk.getMeta().getPointerKey().isEmpty());

    var nf =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                directory.resolveNamespace(
                    ResolveNamespaceRequest.newBuilder()
                        .setRef(
                            NameRef.newBuilder()
                                .setCatalog(cat.getDisplayName())
                                .addPath(leaf + "_root3"))
                        .build()));
    TestSupport.assertGrpcAndMc(
        nf, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "Namespace not found");
  }

  @Test
  void namespaceCreateIdempotent() throws Exception {
    var cat = TestSupport.createCatalog(catalog, namespacePrefix + "cat3", "cat3");

    var key = IdempotencyKey.newBuilder().setKey(namespacePrefix + "k-ns-1").build();

    var spec =
        NamespaceSpec.newBuilder()
            .setCatalogId(cat.getResourceId())
            .setDisplayName("idem_ns")
            .addAllPath(List.of("staging"))
            .setDescription("x")
            .build();

    var r1 =
        namespace.createNamespace(
            CreateNamespaceRequest.newBuilder().setSpec(spec).setIdempotency(key).build());
    var r2 =
        namespace.createNamespace(
            CreateNamespaceRequest.newBuilder().setSpec(spec).setIdempotency(key).build());

    assertEquals(
        r1.getNamespace().getResourceId().getId(), r2.getNamespace().getResourceId().getId());
    assertEquals(r1.getMeta().getPointerKey(), r2.getMeta().getPointerKey());
    assertEquals(r1.getMeta().getPointerVersion(), r2.getMeta().getPointerVersion());
    assertEquals(r1.getMeta().getEtag(), r2.getMeta().getEtag());
  }

  @Test
  void namespaceCreateIdempotencyMismatch() throws Exception {
    var cat = TestSupport.createCatalog(catalog, namespacePrefix + "cat4", "cat4");
    var key = IdempotencyKey.newBuilder().setKey(namespacePrefix + "k-ns-2").build();

    namespace.createNamespace(
        CreateNamespaceRequest.newBuilder()
            .setSpec(
                NamespaceSpec.newBuilder()
                    .setCatalogId(cat.getResourceId())
                    .setDisplayName("idem_ns2")
                    .addAllPath(List.of("db"))
                    .build())
            .setIdempotency(key)
            .build());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespace.createNamespace(
                    CreateNamespaceRequest.newBuilder()
                        .setSpec(
                            NamespaceSpec.newBuilder()
                                .setCatalogId(cat.getResourceId())
                                .setDisplayName("idem_ns2_DIFFERENT")
                                .addAllPath(List.of("db"))
                                .build())
                        .setIdempotency(key)
                        .build()));
    TestSupport.assertGrpcAndMc(
        ex, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "Idempotency key mismatch");
  }
}
