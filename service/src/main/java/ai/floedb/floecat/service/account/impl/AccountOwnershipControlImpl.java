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

import ai.floedb.floecat.account.rpc.AccountOwnershipControl;
import ai.floedb.floecat.account.rpc.AccountOwnershipStatus;
import ai.floedb.floecat.account.rpc.AccountServingMode;
import ai.floedb.floecat.account.rpc.ApplyAccountModeRequest;
import ai.floedb.floecat.account.rpc.ApplyAccountModeResponse;
import ai.floedb.floecat.account.rpc.GetAccountStatusRequest;
import ai.floedb.floecat.account.rpc.GetAccountStatusResponse;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.service.account.AccountGcAuthority;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.security.RolePermissions;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.grpc.Status;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

/** Authenticated Core-to-Floecat adapter for the process-local ownership module. */
@GrpcService
public class AccountOwnershipControlImpl extends BaseServiceImpl
    implements AccountOwnershipControl {

  @Inject AccountGcAuthority authority;
  @Inject PrincipalProvider principalProvider;
  @Inject Authorizer authorizer;

  @Override
  public Uni<ApplyAccountModeResponse> applyAccountMode(ApplyAccountModeRequest request) {
    return run(
        () -> {
          requireControl();
          if (request == null || request.getMode() == AccountServingMode.ASM_UNSPECIFIED) {
            throw Status.INVALID_ARGUMENT
                .withDescription("account mode is required")
                .asRuntimeException();
          }
          AccountGcAuthority.Status status;
          try {
            status =
                authority.apply(
                    request.getAccountId(),
                    request.getAssignmentVersion(),
                    request.getTargetProcessIncarnation(),
                    fromProto(request.getMode()),
                    request.getGcAllowed(),
                    request.getGcLeaseTtlMs());
          } catch (IllegalArgumentException | IllegalStateException rejected) {
            throw Status.FAILED_PRECONDITION
                .withDescription(rejected.getMessage())
                .asRuntimeException();
          }
          return ApplyAccountModeResponse.newBuilder().setStatus(toProto(status)).build();
        });
  }

  @Override
  public Uni<GetAccountStatusResponse> getAccountStatus(GetAccountStatusRequest request) {
    return run(
        () -> {
          requireControl();
          if (request == null || request.getAccountId().isBlank()) {
            throw Status.INVALID_ARGUMENT
                .withDescription("account_id is required")
                .asRuntimeException();
          }
          return GetAccountStatusResponse.newBuilder()
              .setStatus(toProto(authority.status(request.getAccountId())))
              .build();
        });
  }

  private void requireControl() {
    PrincipalContext principal = principalProvider.get();
    authorizer.require(principal, RolePermissions.ACCOUNT_OWNERSHIP_CONTROL_INTERNAL);
  }

  private static AccountGcAuthority.AccountMode fromProto(AccountServingMode mode) {
    return switch (mode) {
      case ASM_UNASSIGNED -> AccountGcAuthority.AccountMode.UNASSIGNED;
      case ASM_SERVING -> AccountGcAuthority.AccountMode.SERVING;
      case ASM_DRAINING -> AccountGcAuthority.AccountMode.DRAINING;
      case ASM_UNSPECIFIED, UNRECOGNIZED ->
          throw new IllegalArgumentException("account mode is required");
    };
  }

  private static AccountServingMode toProto(AccountGcAuthority.AccountMode mode) {
    return switch (mode) {
      case UNASSIGNED -> AccountServingMode.ASM_UNASSIGNED;
      case SERVING -> AccountServingMode.ASM_SERVING;
      case DRAINING -> AccountServingMode.ASM_DRAINING;
    };
  }

  private static AccountOwnershipStatus toProto(AccountGcAuthority.Status status) {
    return AccountOwnershipStatus.newBuilder()
        .setAccountId(status.accountId())
        .setAssignmentVersion(status.assignmentVersion())
        .setProcessIncarnation(status.processIncarnation())
        .setMode(toProto(status.mode()))
        .setGcAllowed(status.gcAllowed())
        .setActiveResolutions(status.activeResolutions())
        .setActiveMutations(status.activeMutations())
        .setActiveGc(status.activeGc())
        .setReferencedRoots(status.referencedRoots())
        .setGcLeaseRemainingMs(status.gcLeaseRemainingMillis())
        .setPointerCacheState(status.pointerCacheState())
        .setDrained(status.drained())
        .build();
  }
}
