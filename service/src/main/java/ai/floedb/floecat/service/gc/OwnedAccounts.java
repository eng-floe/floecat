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

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.cache.PlanningPointerIndex.Ownership;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BooleanSupplier;

/**
 * The accounts this replica may collect: the account directory filtered by write ownership, so
 * every collector runs only where the account's pointer index and publication guard live. It is the
 * only way GC reads the account directory. Placement is fixed for a process's lifetime, so the
 * ownership check is not held across a sweep.
 */
@ApplicationScoped
public class OwnedAccounts {

  private final AccountRepository accounts;
  private final Ownership ownership;

  @Inject
  public OwnedAccounts(AccountRepository accounts, Instance<Ownership> ownership) {
    this(accounts, Ownership.configured(ownership));
  }

  public OwnedAccounts(AccountRepository accounts, Ownership ownership) {
    this.accounts = accounts;
    this.ownership = ownership;
  }

  /**
   * One page of owned accounts, with {@link AccountRepository#list}'s paging contract. Skips pages
   * with no owned account, so an empty result means the directory is exhausted.
   */
  public List<Account> list(int limit, String pageToken, StringBuilder nextOut) {
    return list(limit, pageToken, nextOut, () -> false);
  }

  /** One page of owned accounts, stopping between directory pages when requested. */
  public List<Account> list(
      int limit, String pageToken, StringBuilder nextOut, BooleanSupplier stop) {
    String token = pageToken;
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Account> owned = new ArrayList<>();
      for (Account account : accounts.list(limit, token, next)) {
        if (owns(account.getResourceId().getId())) {
          owned.add(account);
        }
      }
      token = next.toString();
      if (!owned.isEmpty() || token.isEmpty() || stop.getAsBoolean()) {
        nextOut.setLength(0);
        nextOut.append(token);
        return owned;
      }
    }
  }

  /** Lists every account this replica owns, following the directory pages. */
  public List<Account> listAll(int pageSize) {
    List<Account> out = new ArrayList<>();
    String token = "";
    do {
      StringBuilder next = new StringBuilder();
      out.addAll(list(pageSize, token, next));
      token = next.toString();
    } while (!token.isBlank());
    return out;
  }

  /** The account, when it exists and this replica owns it. */
  public Optional<Account> getById(ResourceId accountId) {
    return owns(accountId.getId()) ? accounts.getById(accountId) : Optional.empty();
  }

  private boolean owns(String accountId) {
    Optional<Ownership.Permit> permit = ownership.acquire(accountId, Ownership.Access.WRITE);
    permit.ifPresent(Ownership.Permit::close);
    return permit.isPresent();
  }
}
