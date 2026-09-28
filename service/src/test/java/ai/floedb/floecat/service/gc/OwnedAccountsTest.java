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

package ai.floedb.floecat.service.gc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.cache.PlanningPointerIndex.Ownership;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

class OwnedAccountsTest {

  private static final Ownership OWNS_B =
      (accountId, access) ->
          accountId.equals("b") ? Optional.of(Ownership.Permit.NOOP) : Optional.empty();

  @Test
  void skipsPagesWithoutAnOwnedAccount() {
    AccountRepository repo = mock(AccountRepository.class);
    when(repo.list(anyInt(), eq(""), any())).thenAnswer(page(List.of(account("a")), "t1"));
    when(repo.list(anyInt(), eq("t1"), any())).thenAnswer(page(List.of(account("b")), "t2"));
    when(repo.list(anyInt(), eq("t2"), any())).thenAnswer(page(List.of(account("c")), ""));
    var owned = new OwnedAccounts(repo, OWNS_B);

    StringBuilder next = new StringBuilder();
    assertEquals(List.of(account("b")), owned.list(1, "", next));
    assertEquals("t2", next.toString());
    assertTrue(owned.list(1, next.toString(), next).isEmpty());
    assertEquals("", next.toString());
  }

  @Test
  void getByIdHidesAccountsThisReplicaDoesNotOwn() {
    AccountRepository repo = mock(AccountRepository.class);
    when(repo.getById(any())).thenAnswer(call -> Optional.of(account("a")));
    var owned = new OwnedAccounts(repo, OWNS_B);

    assertTrue(owned.getById(ResourceId.newBuilder().setId("a").build()).isEmpty());
  }

  private static Answer<List<Account>> page(List<Account> accounts, String nextToken) {
    return call -> {
      StringBuilder next = call.getArgument(2);
      next.setLength(0);
      next.append(nextToken);
      return accounts;
    };
  }

  private static Account account(String id) {
    return Account.newBuilder().setResourceId(ResourceId.newBuilder().setId(id)).build();
  }
}
