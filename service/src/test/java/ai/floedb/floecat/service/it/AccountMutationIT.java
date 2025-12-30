package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.account.rpc.AccountServiceGrpc;
import ai.floedb.floecat.account.rpc.AccountSpec;
import ai.floedb.floecat.account.rpc.CreateAccountRequest;
import ai.floedb.floecat.account.rpc.DeleteAccountRequest;
import ai.floedb.floecat.account.rpc.GetAccountRequest;
import ai.floedb.floecat.account.rpc.ListAccountsRequest;
import ai.floedb.floecat.account.rpc.UpdateAccountRequest;
import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.Precondition;
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
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class AccountMutationIT {

  @GrpcClient("floecat")
  AccountServiceGrpc.AccountServiceBlockingStub tenancy;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  String accountPrefix = this.getClass().getSimpleName() + "_";

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void accountExists() throws Exception {
    var spec =
        AccountSpec.newBuilder()
            .setDisplayName(accountPrefix + "t1")
            .setDescription("desc")
            .build();

    var r1 = tenancy.createAccount(CreateAccountRequest.newBuilder().setSpec(spec).build());

    assertNotNull(r1.getAccount());
    assertEquals(accountPrefix + "t1", r1.getAccount().getDisplayName());

    var newSpec =
        AccountSpec.newBuilder()
            .setDisplayName(accountPrefix + "t1")
            .setDescription("description")
            .build();

    assertDoesNotThrow(
        () -> tenancy.createAccount(CreateAccountRequest.newBuilder().setSpec(newSpec).build()));
  }

  @Test
  void accountCreateUpdateDelete() throws Exception {
    var spec1 =
        AccountSpec.newBuilder()
            .setDisplayName(accountPrefix + "t_pre")
            .setDescription("pre")
            .build();

    var created = tenancy.createAccount(CreateAccountRequest.newBuilder().setSpec(spec1).build());

    var id = created.getAccount().getResourceId();
    assertEquals(ResourceKind.RK_ACCOUNT, id.getKind());

    FieldMask mask =
        FieldMask.newBuilder().addAllPaths(List.of("display_name", "description")).build();
    var upd1 =
        tenancy.updateAccount(
            UpdateAccountRequest.newBuilder()
                .setAccountId(id)
                .setSpec(
                    AccountSpec.newBuilder()
                        .setDisplayName(accountPrefix + "t_pre")
                        .setDescription("desc1")
                        .build())
                .setUpdateMask(mask)
                .build());

    var m1 = upd1.getMeta();
    assertTrue(m1.getPointerVersion() >= 1);
    assertEquals(accountPrefix + "t_pre", upd1.getAccount().getDisplayName());
    assertEquals("desc1", upd1.getAccount().getDescription());

    String expectedName = accountPrefix + "t_pre_2";

    var updOk =
        tenancy.updateAccount(
            UpdateAccountRequest.newBuilder()
                .setAccountId(id)
                .setSpec(
                    AccountSpec.newBuilder()
                        .setDisplayName(expectedName)
                        .setDescription("desc2")
                        .build())
                .setUpdateMask(mask)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(m1.getPointerVersion())
                        .setExpectedEtag(m1.getEtag())
                        .build())
                .build());

    assertEquals(expectedName, updOk.getAccount().getDisplayName());
    assertEquals("desc2", updOk.getAccount().getDescription());
    assertTrue(updOk.getMeta().getPointerVersion() > m1.getPointerVersion());

    String next = "";
    boolean hasMatch = false;
    do {
      var resp = tenancy.listAccounts(ListAccountsRequest.newBuilder().build());

      hasMatch |=
          resp.getAccountsList().stream().anyMatch(t -> t.getDisplayName().equals(expectedName));

      next = resp.getPage().getNextPageToken();
    } while (!next.isEmpty());

    assertTrue(hasMatch, "Expected to find account with displayName=" + expectedName);

    var bad =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                tenancy.updateAccount(
                    UpdateAccountRequest.newBuilder()
                        .setAccountId(id)
                        .setSpec(
                            AccountSpec.newBuilder()
                                .setDisplayName(accountPrefix + "t_pre_3")
                                .build())
                        .setUpdateMask(mask)
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
        tenancy.deleteAccount(
            DeleteAccountRequest.newBuilder()
                .setAccountId(id)
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
            () -> tenancy.getAccount(GetAccountRequest.newBuilder().setAccountId(id).build()));

    TestSupport.assertGrpcAndMc(
        notFound, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "Account not found");

    var delNotEmpty =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                tenancy.deleteAccount(
                    DeleteAccountRequest.newBuilder()
                        .setAccountId(TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT))
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
        "Account \"" + TestSupport.DEFAULT_SEED_ACCOUNT + "\" contains catalogs.");
  }

  @Test
  void accountCreateIdempotent() throws Exception {
    var key = IdempotencyKey.newBuilder().setKey(accountPrefix + "k-ten-1").build();
    var spec =
        AccountSpec.newBuilder()
            .setDisplayName(accountPrefix + "idem_account")
            .setDescription("x")
            .build();

    var r1 =
        tenancy.createAccount(
            CreateAccountRequest.newBuilder().setSpec(spec).setIdempotency(key).build());
    var r2 =
        tenancy.createAccount(
            CreateAccountRequest.newBuilder().setSpec(spec).setIdempotency(key).build());

    assertEquals(r1.getAccount().getResourceId().getId(), r2.getAccount().getResourceId().getId());
    assertEquals(r1.getMeta().getPointerKey(), r2.getMeta().getPointerKey());
    assertEquals(r1.getMeta().getPointerVersion(), r2.getMeta().getPointerVersion());
    assertEquals(r1.getMeta().getEtag(), r2.getMeta().getEtag());
  }

  @Test
  void accountCreateIdempotencyMismatch() throws Exception {
    var key = IdempotencyKey.newBuilder().setKey(accountPrefix + "k-ten-2").build();

    tenancy.createAccount(
        CreateAccountRequest.newBuilder()
            .setSpec(
                AccountSpec.newBuilder().setDisplayName(accountPrefix + "idem_account2").build())
            .setIdempotency(key)
            .build());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                tenancy.createAccount(
                    CreateAccountRequest.newBuilder()
                        .setSpec(
                            AccountSpec.newBuilder()
                                .setDisplayName(accountPrefix + "idem_account2_DIFFERENT")
                                .build())
                        .setIdempotency(key)
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "Idempotency key mismatch");
  }

  @Test
  void accountCreateIdempotencyMismatchOnDescription() throws Exception {
    var key = IdempotencyKey.newBuilder().setKey(accountPrefix + "k-ten-3").build();

    tenancy.createAccount(
        CreateAccountRequest.newBuilder()
            .setSpec(
                AccountSpec.newBuilder()
                    .setDisplayName(accountPrefix + "idem_account3")
                    .setDescription("desc-a")
                    .build())
            .setIdempotency(key)
            .build());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                tenancy.createAccount(
                    CreateAccountRequest.newBuilder()
                        .setSpec(
                            AccountSpec.newBuilder()
                                .setDisplayName(accountPrefix + "idem_account3")
                                .setDescription("desc-b")
                                .build())
                        .setIdempotency(key)
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "Idempotency key mismatch");
  }
}
