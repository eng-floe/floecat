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

package ai.floedb.floecat.service.cache;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.service.repo.cache.PlanningPointerIndex;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.List;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * Fills the planner pointer index with the accounts this instance owns, and carries the operator
 * switch that takes the index out of the read path.
 *
 * <p>Warming is scheduled, never awaited: a loading partition already answers from durable KV, so
 * an account that is still filling is slower rather than wrong.
 */
@ApplicationScoped
public class PointerIndexWarmer {
  private static final Logger LOG = Logger.getLogger(PointerIndexWarmer.class);

  private final PlanningPointerIndex index;
  private final Instance<AccountRepository> accounts;
  private final boolean warmOnStartup;
  private final int accountsPageSize;

  @Inject
  public PointerIndexWarmer(
      PlanningPointerIndex index,
      Instance<AccountRepository> accounts,
      @ConfigProperty(
              name = "floecat.planner.pointer-index.warm-on-startup",
              defaultValue = "false")
          boolean warmOnStartup,
      @ConfigProperty(
              name = "floecat.planner.pointer-index.accounts-page-size",
              defaultValue = "50")
          int accountsPageSize) {
    this.index = index;
    this.accounts = accounts;
    this.warmOnStartup = warmOnStartup;
    this.accountsPageSize = accountsPageSize;
  }

  void onStartup(@Observes StartupEvent ignored) {
    if (warmOnStartup) {
      warmOwnedAccounts();
    }
  }

  /**
   * Schedules a warm for every account. Ownership is not consulted here: the index skips an account
   * this instance does not own, which keeps one answer to "do I own this" rather than two.
   */
  public long warmOwnedAccounts() {
    if (!index.enabled() || accounts.isUnsatisfied()) {
      return 0;
    }
    AccountRepository repository = accounts.get();
    int pageSize = Math.max(1, accountsPageSize);
    long scheduled = 0;
    String token = "";
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Account> page = repository.list(pageSize, token, next);
      for (Account account : page) {
        String accountId = account.getResourceId().getId();
        if (accountId != null && !accountId.isBlank()) {
          index.warm(accountId);
          scheduled++;
        }
      }
      String newToken = next.toString();
      // Same guard the partition load uses: a token that stops advancing would page forever.
      if (newToken.isBlank() || newToken.equals(token)) {
        break;
      }
      token = newToken;
    }
    LOG.infof("planner_pointer_index_warm_scheduled accounts=%d", scheduled);
    return scheduled;
  }
}
