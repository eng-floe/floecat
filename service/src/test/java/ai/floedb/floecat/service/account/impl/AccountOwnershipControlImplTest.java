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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.account.rpc.AccountServingMode;
import ai.floedb.floecat.account.rpc.ApplyAccountModeRequest;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.service.account.AccountGcAuthority;
import ai.floedb.floecat.service.security.RolePermissions;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import org.junit.jupiter.api.Test;

class AccountOwnershipControlImplTest {

  @Test
  void applyIsAuthenticatedAndReturnsTheCompleteLocalStatus() {
    AccountOwnershipControlImpl service = new AccountOwnershipControlImpl();
    service.authority = mock(AccountGcAuthority.class);
    service.principalProvider = mock(PrincipalProvider.class);
    service.authorizer = mock(Authorizer.class);
    PrincipalContext principal = PrincipalContext.newBuilder().setCorrelationId("corr").build();
    when(service.principalProvider.get()).thenReturn(principal);
    doNothing().when(service.authorizer).require(any(), org.mockito.ArgumentMatchers.anyString());
    when(service.authority.apply(
            "acct-1", 4L, "pod-a/start-1", AccountGcAuthority.AccountMode.DRAINING, false, 0L))
        .thenReturn(
            new AccountGcAuthority.Status(
                "acct-1",
                4L,
                "pod-a/start-1",
                AccountGcAuthority.AccountMode.DRAINING,
                false,
                1L,
                2L,
                0L,
                3L,
                "COMPLETE",
                0L));

    var response =
        service
            .applyAccountMode(
                ApplyAccountModeRequest.newBuilder()
                    .setAccountId("acct-1")
                    .setAssignmentVersion(4L)
                    .setTargetProcessIncarnation("pod-a/start-1")
                    .setMode(AccountServingMode.ASM_DRAINING)
                    .build())
            .await()
            .indefinitely();

    verify(service.authorizer)
        .require(principal, RolePermissions.ACCOUNT_OWNERSHIP_CONTROL_INTERNAL);
    assertThat(response.getStatus().getMode()).isEqualTo(AccountServingMode.ASM_DRAINING);
    assertThat(response.getStatus().getActiveResolutions()).isEqualTo(1L);
    assertThat(response.getStatus().getActiveMutations()).isEqualTo(2L);
    assertThat(response.getStatus().getReferencedRoots()).isEqualTo(3L);
    assertThat(response.getStatus().getDrained()).isFalse();
  }
}
