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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.account.rpc.AccountSpec;
import ai.floedb.floecat.account.rpc.CreateAccountRequest;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class AccountServiceImplTest {

  @Test
  void createAccount_usesCapturedPrincipalAcrossRetry() {
    var service = new AccountServiceImpl();
    service.accountRepo = mock(AccountRepository.class);
    service.principal = mock(PrincipalProvider.class);
    service.authz = mock(Authorizer.class);

    PrincipalContext requestPrincipal =
        PrincipalContext.newBuilder()
            .setAccountId("acct-1")
            .setSubject("bootstrap-user")
            .setCorrelationId("corr-1")
            .addPermissions("account.write")
            .build();
    when(service.principal.get())
        .thenReturn(requestPrincipal)
        .thenReturn(PrincipalContext.getDefaultInstance());
    doNothing().when(service.authz).require(any(), eq("account.write"));

    when(service.accountRepo.getByName("Example")).thenReturn(Optional.empty());
    doThrow(new BaseResourceRepository.AbortRetryableException("retry once"))
        .doNothing()
        .when(service.accountRepo)
        .create(any(Account.class));
    when(service.accountRepo.metaForSafe(any()))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(1L).build());

    var response =
        service
            .createAccount(
                CreateAccountRequest.newBuilder()
                    .setSpec(AccountSpec.newBuilder().setDisplayName("Example").build())
                    .build())
            .await()
            .indefinitely();

    assertEquals("Example", response.getAccount().getDisplayName());
    assertEquals("acct-1", response.getAccount().getResourceId().getAccountId());
    verify(service.principal, times(1)).get();
    verify(service.authz, times(2)).require(requestPrincipal, "account.write");
  }

  @Test
  void createAccount_rejectsBlankPrincipalContext() {
    var service = new AccountServiceImpl();
    service.principal = mock(PrincipalProvider.class);
    service.authz = mock(Authorizer.class);

    when(service.principal.get()).thenReturn(PrincipalContext.getDefaultInstance());

    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                service
                    .createAccount(
                        CreateAccountRequest.newBuilder()
                            .setSpec(AccountSpec.newBuilder().setDisplayName("Example").build())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(
        "createAccount principal context missing required fields: subject= permissions=[]",
        ex.getMessage());
  }
}
