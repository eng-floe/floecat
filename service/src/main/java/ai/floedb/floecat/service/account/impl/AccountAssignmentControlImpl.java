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

import ai.floedb.floecat.account.rpc.AccountAssignmentControl;
import ai.floedb.floecat.account.rpc.AccountOwnershipStatus;
import ai.floedb.floecat.account.rpc.AccountServingMode;
import ai.floedb.floecat.account.rpc.ApplyAssignmentRequest;
import ai.floedb.floecat.account.rpc.ApplyAssignmentResponse;
import ai.floedb.floecat.account.rpc.AssignmentMode;
import ai.floedb.floecat.account.rpc.AssignmentStatus;
import ai.floedb.floecat.account.rpc.GetAssignmentStatusRequest;
import ai.floedb.floecat.account.rpc.GetAssignmentStatusResponse;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.service.account.AccountAssignment;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.security.RolePermissions;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.grpc.Status;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

/** Core-to-Floecat assignment control. Answers only in managed mode. */
@GrpcService
public class AccountAssignmentControlImpl extends BaseServiceImpl
    implements AccountAssignmentControl {

  @Inject AccountAssignment assignment;
  @Inject PrincipalProvider principalProvider;
  @Inject Authorizer authorizer;

  @Override
  public Uni<ApplyAssignmentResponse> applyAssignment(ApplyAssignmentRequest request) {
    return run(
        () -> {
          requireControl();
          requireManaged();
          if (request == null || request.getMode() == AssignmentMode.AM_UNSPECIFIED) {
            throw Status.INVALID_ARGUMENT
                .withDescription("assignment mode is required")
                .asRuntimeException();
          }
          AccountAssignment.Status status;
          try {
            status =
                assignment.apply(
                    request.getEpoch(),
                    fromProto(request.getMode()),
                    request.getAccountIdsList(),
                    request.getGcAllowedAccountIdsList(),
                    request.getTargetIncarnation());
          } catch (IllegalArgumentException | IllegalStateException rejected) {
            throw Status.FAILED_PRECONDITION
                .withDescription(rejected.getMessage())
                .asRuntimeException();
          }
          return ApplyAssignmentResponse.newBuilder().setStatus(toProto(status)).build();
        });
  }

  @Override
  public Uni<GetAssignmentStatusResponse> getAssignmentStatus(GetAssignmentStatusRequest request) {
    return run(
        () -> {
          requireControl();
          requireManaged();
          return GetAssignmentStatusResponse.newBuilder()
              .setStatus(toProto(assignment.status()))
              .build();
        });
  }

  private void requireControl() {
    PrincipalContext principal = principalProvider.get();
    authorizer.require(principal, RolePermissions.ACCOUNT_ASSIGNMENT_CONTROL_INTERNAL);
  }

  private void requireManaged() {
    if (!assignment.managed()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "account assignment control requires floecat.account-ownership.mode=managed")
          .asRuntimeException();
    }
  }

  private static AccountAssignment.AssignmentMode fromProto(AssignmentMode mode) {
    return switch (mode) {
      case AM_DRAINING -> AccountAssignment.AssignmentMode.DRAINING;
      case AM_SERVING -> AccountAssignment.AssignmentMode.SERVING;
      case AM_UNSPECIFIED, UNRECOGNIZED ->
          throw Status.INVALID_ARGUMENT
              .withDescription("assignment mode is required")
              .asRuntimeException();
    };
  }

  static AssignmentMode toProto(AccountAssignment.AssignmentMode mode) {
    return switch (mode) {
      case DRAINING -> AssignmentMode.AM_DRAINING;
      case SERVING -> AssignmentMode.AM_SERVING;
    };
  }

  static AccountServingMode toProto(AccountAssignment.AccountMode mode) {
    return switch (mode) {
      case UNASSIGNED -> AccountServingMode.ASM_UNASSIGNED;
      case SERVING -> AccountServingMode.ASM_SERVING;
      case DRAINING -> AccountServingMode.ASM_DRAINING;
    };
  }

  static AssignmentStatus toProto(AccountAssignment.Status status) {
    AssignmentStatus.Builder builder =
        AssignmentStatus.newBuilder()
            .setMemberId(status.memberId())
            .setIncarnation(status.incarnation())
            .setEpoch(status.epoch())
            .setMode(toProto(status.mode()))
            .setRecoveredFromStore(status.recoveredFromStore());
    for (AccountAssignment.AccountStatus account : status.accounts()) {
      builder.addAccounts(
          AccountOwnershipStatus.newBuilder()
              .setAccountId(account.accountId())
              .setMode(toProto(account.mode()))
              .setGcAllowed(account.gcAllowed())
              .setActiveResolutions(account.activeResolutions())
              .setActiveMutations(account.activeMutations())
              .setActiveGc(account.activeGc())
              .setDrained(account.drained())
              .setPointerIndexState(account.pointerIndexState()));
    }
    return builder.build();
  }
}
