package ai.floedb.metacat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.metacat.common.rpc.ErrorCode;
import ai.floedb.metacat.common.rpc.IdempotencyKey;
import ai.floedb.metacat.common.rpc.Precondition;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.bootstrap.impl.SeedRunner;
import ai.floedb.metacat.service.util.TestDataResetter;
import ai.floedb.metacat.service.util.TestSupport;
import ai.floedb.metacat.tenancy.rpc.CreateTenantRequest;
import ai.floedb.metacat.tenancy.rpc.DeleteTenantRequest;
import ai.floedb.metacat.tenancy.rpc.GetTenantRequest;
import ai.floedb.metacat.tenancy.rpc.ListTenantsRequest;
import ai.floedb.metacat.tenancy.rpc.TenancyGrpc;
import ai.floedb.metacat.tenancy.rpc.TenantSpec;
import ai.floedb.metacat.tenancy.rpc.UpdateTenantRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class TenantMutationIT {

  @GrpcClient("tenancy")
  TenancyGrpc.TenancyBlockingStub tenancy;

  @GrpcClient("catalog")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  String tenantPrefix = this.getClass().getSimpleName() + "_";

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void tenantExists() throws Exception {
    var spec =
        TenantSpec.newBuilder().setDisplayName(tenantPrefix + "t1").setDescription("desc").build();

    var r1 = tenancy.createTenant(CreateTenantRequest.newBuilder().setSpec(spec).build());

    assertNotNull(r1.getTenant());
    assertEquals(tenantPrefix + "t1", r1.getTenant().getDisplayName());

    var newSpec =
        TenantSpec.newBuilder()
            .setDisplayName(tenantPrefix + "t1")
            .setDescription("description")
            .build();

    var conflict =
        assertThrows(
            StatusRuntimeException.class,
            () -> tenancy.createTenant(CreateTenantRequest.newBuilder().setSpec(newSpec).build()));

    TestSupport.assertGrpcAndMc(
        conflict,
        Status.Code.ABORTED,
        ErrorCode.MC_CONFLICT,
        "Tenant \"" + tenantPrefix + "t1\" already exists");
  }

  @Test
  void tenantCreateUpdateDelete() throws Exception {
    var spec1 =
        TenantSpec.newBuilder()
            .setDisplayName(tenantPrefix + "t_pre")
            .setDescription("pre")
            .build();

    var created = tenancy.createTenant(CreateTenantRequest.newBuilder().setSpec(spec1).build());

    var id = created.getTenant().getResourceId();
    assertEquals(ResourceKind.RK_TENANT, id.getKind());

    var upd1 =
        tenancy.updateTenant(
            UpdateTenantRequest.newBuilder()
                .setTenantId(id)
                .setSpec(
                    TenantSpec.newBuilder()
                        .setDisplayName(tenantPrefix + "t_pre")
                        .setDescription("desc1")
                        .build())
                .build());

    var m1 = upd1.getMeta();
    assertTrue(m1.getPointerVersion() >= 1);
    assertEquals(tenantPrefix + "t_pre", upd1.getTenant().getDisplayName());
    assertEquals("desc1", upd1.getTenant().getDescription());

    String expectedName = tenantPrefix + "t_pre_2";

    var updOk =
        tenancy.updateTenant(
            UpdateTenantRequest.newBuilder()
                .setTenantId(id)
                .setSpec(
                    TenantSpec.newBuilder()
                        .setDisplayName(expectedName)
                        .setDescription("desc2")
                        .build())
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(m1.getPointerVersion())
                        .setExpectedEtag(m1.getEtag())
                        .build())
                .build());

    assertEquals(expectedName, updOk.getTenant().getDisplayName());
    assertEquals("desc2", updOk.getTenant().getDescription());
    assertTrue(updOk.getMeta().getPointerVersion() > m1.getPointerVersion());

    String next = "";
    boolean hasMatch = false;
    do {
      var resp = tenancy.listTenants(ListTenantsRequest.newBuilder().build());

      hasMatch |=
          resp.getTenantsList().stream().anyMatch(t -> t.getDisplayName().equals(expectedName));

      next = resp.getPage().getNextPageToken();
    } while (!next.isEmpty());

    assertTrue(hasMatch, "Expected to find tenant with displayName=" + expectedName);

    var bad =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                tenancy.updateTenant(
                    UpdateTenantRequest.newBuilder()
                        .setTenantId(id)
                        .setSpec(
                            TenantSpec.newBuilder()
                                .setDisplayName(tenantPrefix + "t_pre_3")
                                .build())
                        .setPrecondition(
                            Precondition.newBuilder()
                                .setExpectedVersion(424242L)
                                .setExpectedEtag("bogus")
                                .build())
                        .build()));

    TestSupport.assertGrpcAndMc(
        bad, Status.Code.FAILED_PRECONDITION, ErrorCode.MC_PRECONDITION_FAILED, "mismatch");

    var m2 = updOk.getMeta();
    var del =
        tenancy.deleteTenant(
            DeleteTenantRequest.newBuilder()
                .setTenantId(id)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(m2.getPointerVersion())
                        .setExpectedEtag(m2.getEtag())
                        .build())
                .build());
    assertEquals(m2.getPointerKey(), del.getMeta().getPointerKey());

    var notFound =
        assertThrows(
            StatusRuntimeException.class,
            () -> tenancy.getTenant(GetTenantRequest.newBuilder().setTenantId(id).build()));

    TestSupport.assertGrpcAndMc(
        notFound, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "Tenant not found");

    var delNotEmpty =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                tenancy.deleteTenant(
                    DeleteTenantRequest.newBuilder()
                        .setTenantId(TestSupport.createTenantId(TestSupport.DEFAULT_SEED_TENANT))
                        .setPrecondition(
                            Precondition.newBuilder()
                                .setExpectedVersion(m2.getPointerVersion())
                                .setExpectedEtag(m2.getEtag())
                                .build())
                        .build()));

    TestSupport.assertGrpcAndMc(
        delNotEmpty,
        Status.Code.ABORTED,
        ErrorCode.MC_CONFLICT,
        "Tenant \"" + TestSupport.DEFAULT_SEED_TENANT + "\" contains catalogs.");
  }

  @Test
  void tenantCreateIdempotent() throws Exception {
    var key = IdempotencyKey.newBuilder().setKey(tenantPrefix + "k-ten-1").build();
    var spec =
        TenantSpec.newBuilder()
            .setDisplayName(tenantPrefix + "idem_tenant")
            .setDescription("x")
            .build();

    var r1 =
        tenancy.createTenant(
            CreateTenantRequest.newBuilder().setSpec(spec).setIdempotency(key).build());
    var r2 =
        tenancy.createTenant(
            CreateTenantRequest.newBuilder().setSpec(spec).setIdempotency(key).build());

    assertEquals(r1.getTenant().getResourceId().getId(), r2.getTenant().getResourceId().getId());
    assertEquals(r1.getMeta().getPointerKey(), r2.getMeta().getPointerKey());
    assertEquals(r1.getMeta().getPointerVersion(), r2.getMeta().getPointerVersion());
    assertEquals(r1.getMeta().getEtag(), r2.getMeta().getEtag());
  }

  @Test
  void tenantCreateIdempotencyMismatch() throws Exception {
    var key = IdempotencyKey.newBuilder().setKey(tenantPrefix + "k-ten-2").build();

    tenancy.createTenant(
        CreateTenantRequest.newBuilder()
            .setSpec(TenantSpec.newBuilder().setDisplayName(tenantPrefix + "idem_tenant2").build())
            .setIdempotency(key)
            .build());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                tenancy.createTenant(
                    CreateTenantRequest.newBuilder()
                        .setSpec(
                            TenantSpec.newBuilder()
                                .setDisplayName(tenantPrefix + "idem_tenant2_DIFFERENT")
                                .build())
                        .setIdempotency(key)
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "Idempotency key mismatch");
  }
}
