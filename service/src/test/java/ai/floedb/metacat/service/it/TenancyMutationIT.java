package ai.floedb.metacat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.common.rpc.ErrorCode;
import ai.floedb.metacat.common.rpc.IdempotencyKey;
import ai.floedb.metacat.common.rpc.Precondition;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.util.TestSupport;
import ai.floedb.metacat.tenancy.rpc.CreateTenantRequest;
import ai.floedb.metacat.tenancy.rpc.DeleteTenantRequest;
import ai.floedb.metacat.tenancy.rpc.GetTenantRequest;
import ai.floedb.metacat.tenancy.rpc.TenancyGrpc;
import ai.floedb.metacat.tenancy.rpc.TenantSpec;
import ai.floedb.metacat.tenancy.rpc.UpdateTenantRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
class TenantMutationIT {

  @GrpcClient("tenancy")
  TenancyGrpc.TenancyBlockingStub tenancy;

  String tenantPrefix = this.getClass().getSimpleName() + "_";

  @Test
  void tenantExists() throws Exception {
    var spec =
        TenantSpec.newBuilder().setDisplayName(tenantPrefix + "t1").setDescription("desc").build();

    var r1 = tenancy.createTenant(CreateTenantRequest.newBuilder().setSpec(spec).build());

    assertNotNull(r1.getTenant());
    assertEquals(tenantPrefix + "t1", r1.getTenant().getDisplayName());

    var conflict =
        assertThrows(
            StatusRuntimeException.class,
            () -> tenancy.createTenant(CreateTenantRequest.newBuilder().setSpec(spec).build()));

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
    assertTrue(id.getId().matches("^[0-9a-fA-F-]{36}$"), "id must look like UUID");

    var upd1 =
        tenancy.updateTenant(
            UpdateTenantRequest.newBuilder()
                .setTenantId(id)
                .setSpec(
                    TenantSpec.newBuilder()
                        .setDisplayName(tenantPrefix + "t_pre")
                        .setDescription("pre")
                        .build())
                .build());

    var m1 = upd1.getMeta();
    assertTrue(m1.getPointerVersion() >= 1);

    var updOk =
        tenancy.updateTenant(
            UpdateTenantRequest.newBuilder()
                .setTenantId(id)
                .setSpec(
                    TenantSpec.newBuilder()
                        .setDisplayName(tenantPrefix + "t_pre_2")
                        .setDescription("desc2")
                        .build())
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(m1.getPointerVersion())
                        .setExpectedEtag(m1.getEtag())
                        .build())
                .build());

    assertEquals(tenantPrefix + "t_pre_2", updOk.getTenant().getDisplayName());
    assertTrue(updOk.getMeta().getPointerVersion() > m1.getPointerVersion());

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
