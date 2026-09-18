/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.account.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.account.rpc.AccountServingMode;
import ai.floedb.floecat.account.rpc.ApplyAssignmentRequest;
import ai.floedb.floecat.account.rpc.AssignmentPhase;
import ai.floedb.floecat.account.rpc.GetAssignmentStatusRequest;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.service.account.AccountAssignment;
import ai.floedb.floecat.service.account.AssignmentControl;
import ai.floedb.floecat.service.security.RolePermissions;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.telemetry.TestObservability;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

class AccountAssignmentControlImplTest {
  private static final String INCARNATION = "floecat-0/inc";
  private static final PrincipalContext CONTROLLER =
      PrincipalContext.newBuilder()
          .setSubject("core")
          .setCorrelationId("corr")
          .addPermissions(RolePermissions.ACCOUNT_ASSIGNMENT_CONTROL_INTERNAL)
          .build();

  private final InMemoryPointerStore raw = new InMemoryPointerStore();
  private final TestObservability observability = new TestObservability();

  private AccountAssignmentControlImpl service(AccountAssignment assignment, PrincipalContext pc) {
    AccountAssignmentControlImpl service = new AccountAssignmentControlImpl();
    service.assignment = assignment;
    service.principalProvider = mock(PrincipalProvider.class);
    when(service.principalProvider.get()).thenReturn(pc);
    service.authorizer = new Authorizer();
    return service;
  }

  private AccountAssignment managed() {
    return AccountAssignment.managedForTesting("floecat-0", INCARNATION, raw, observability);
  }

  private static ApplyAssignmentRequest.Builder apply(long epoch, AssignmentPhase phase) {
    return ApplyAssignmentRequest.newBuilder()
        .setEpoch(epoch)
        .setPhase(phase)
        .setTargetIncarnation(INCARNATION);
  }

  @Test
  void applyReturnsTheCompleteLocalStatus() {
    var service = service(managed(), CONTROLLER);

    var response =
        service
            .applyAssignment(
                apply(4L, AssignmentPhase.AP_SERVING)
                    .addAccountIds("acct-1")
                    .addAccountIds("acct-2")
                    .addGcAllowedAccountIds("acct-1")
                    .build())
            .await()
            .indefinitely();

    var status = response.getStatus();
    assertThat(status.getMemberId()).isEqualTo("floecat-0");
    assertThat(status.getIncarnation()).isEqualTo(INCARNATION);
    assertThat(status.getEpoch()).isEqualTo(4L);
    assertThat(status.getPhase()).isEqualTo(AssignmentPhase.AP_SERVING);
    assertThat(status.getRecoveredFromStore()).isFalse();
    assertThat(status.getAccountsList()).hasSize(2);
    var first = status.getAccounts(0);
    assertThat(first.getAccountId()).isEqualTo("acct-1");
    assertThat(first.getMode()).isEqualTo(AccountServingMode.ASM_SERVING);
    assertThat(first.getGcAllowed()).isTrue();
    assertThat(first.getDrained()).isTrue();
    assertThat(status.getAccounts(1).getGcAllowed()).isFalse();

    var read =
        service
            .getAssignmentStatus(GetAssignmentStatusRequest.getDefaultInstance())
            .await()
            .indefinitely();
    assertThat(read.getStatus()).isEqualTo(status);
  }

  @Test
  void rejectedAppliesAnswerFailedPreconditionAndLeaveTheStateAlone() {
    var assignment = managed();
    var service = service(assignment, CONTROLLER);
    service
        .applyAssignment(apply(4L, AssignmentPhase.AP_SERVING).addAccountIds("a").build())
        .await()
        .indefinitely();

    assertCode(
        Status.Code.FAILED_PRECONDITION,
        () ->
            service.applyAssignment(
                apply(3L, AssignmentPhase.AP_SERVING).addAccountIds("a").build()));
    assertCode(
        Status.Code.FAILED_PRECONDITION,
        () ->
            service.applyAssignment(
                apply(4L, AssignmentPhase.AP_DRAINING).addAccountIds("a").build()));
    assertCode(
        Status.Code.FAILED_PRECONDITION,
        () ->
            service.applyAssignment(
                apply(5L, AssignmentPhase.AP_SERVING)
                    .addAccountIds("a")
                    .setTargetIncarnation("floecat-0/other")
                    .build()));
    assertCode(
        Status.Code.INVALID_ARGUMENT,
        () ->
            service.applyAssignment(
                apply(5L, AssignmentPhase.AP_UNSPECIFIED).addAccountIds("a").build()));
    assertThat(assignment.status().epoch()).isEqualTo(4L);
    assertThat(assignment.status("a").mode()).isEqualTo(AssignmentControl.AccountMode.SERVING);
  }

  @Test
  void standaloneAnswersFailedPrecondition() {
    var service = service(AccountAssignment.standaloneForTesting(raw, observability), CONTROLLER);

    assertCode(
        Status.Code.FAILED_PRECONDITION,
        () ->
            service.applyAssignment(
                apply(1L, AssignmentPhase.AP_SERVING).addAccountIds("a").build()));
    assertCode(
        Status.Code.FAILED_PRECONDITION,
        () -> service.getAssignmentStatus(GetAssignmentStatusRequest.getDefaultInstance()));
  }

  @Test
  void controlRequiresTheInternalPermission() {
    var assignment = managed();
    var service = service(assignment, PrincipalContext.newBuilder().setSubject("someone").build());

    assertCode(
        Status.Code.PERMISSION_DENIED,
        () ->
            service.applyAssignment(
                apply(1L, AssignmentPhase.AP_SERVING).addAccountIds("a").build()));
    assertCode(
        Status.Code.PERMISSION_DENIED,
        () -> service.getAssignmentStatus(GetAssignmentStatusRequest.getDefaultInstance()));
    assertThat(assignment.status().epoch()).isZero();
  }

  private static void assertCode(
      Status.Code code, java.util.function.Supplier<io.smallrye.mutiny.Uni<?>> call) {
    assertThatThrownBy(() -> call.get().await().indefinitely())
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            failure -> assertThat(failure.getStatus().getCode()).isEqualTo(code));
  }
}
