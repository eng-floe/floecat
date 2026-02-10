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

package ai.floedb.floecat.service.transaction.impl;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.scheduler.ScheduledExecution;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.microprofile.config.ConfigProvider;

@ApplicationScoped
public class TransactionIntentApplyScheduler {

  @Inject Provider<AccountRepository> accounts;
  @Inject Provider<TransactionIntentApplier> applier;
  @Inject MeterRegistry registry;

  private Counter tickCounter;
  private Counter accountCounter;
  private Counter intentsScanned;
  private Counter intentsApplied;
  private Counter intentsSkipped;
  private Timer tickTimer;
  private Timer accountTimer;
  private final AtomicInteger running = new AtomicInteger(0);
  private final AtomicInteger enabledGauge = new AtomicInteger(0);
  private final AtomicLong lastTickStartMs = new AtomicLong(0);
  private final AtomicLong lastTickEndMs = new AtomicLong(0);

  private volatile boolean stopping;

  @PostConstruct
  void initMeters() {
    tickCounter =
        Counter.builder("floecat_tx_intent_apply_ticks")
            .description("Transaction intent applier ticks")
            .register(registry);
    accountCounter =
        Counter.builder("floecat_tx_intent_apply_accounts")
            .description("Accounts processed per tick")
            .register(registry);
    intentsScanned =
        Counter.builder("floecat_tx_intent_apply_scanned")
            .description("Intents scanned")
            .register(registry);
    intentsApplied =
        Counter.builder("floecat_tx_intent_apply_applied")
            .description("Intents applied")
            .register(registry);
    intentsSkipped =
        Counter.builder("floecat_tx_intent_apply_skipped")
            .description("Intents skipped")
            .register(registry);
    tickTimer =
        Timer.builder("floecat_tx_intent_apply_tick_duration")
            .description("Duration of transaction intent apply ticks")
            .register(registry);
    accountTimer =
        Timer.builder("floecat_tx_intent_apply_account_duration")
            .description("Duration of transaction intent apply per account")
            .register(registry);
    registry.gauge("floecat_tx_intent_apply_running", running);
    registry.gauge("floecat_tx_intent_apply_enabled", enabledGauge);
    registry.gauge("floecat_tx_intent_apply_last_tick_start_ms", lastTickStartMs);
    registry.gauge("floecat_tx_intent_apply_last_tick_end_ms", lastTickEndMs);
  }

  void onStop(@Observes ShutdownEvent ev) {
    stopping = true;
    DisabledOrStopping.signalStopping();
  }

  @Scheduled(
      every = "{floecat.transaction.apply.tick-every}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
      skipExecutionIf = DisabledOrStopping.class)
  void tick(ScheduledExecution exec) {
    if (stopping) {
      return;
    }

    var cfg = ConfigProvider.getConfig();
    boolean enabled =
        cfg.getOptionalValue("floecat.transaction.apply.enabled", Boolean.class).orElse(true);
    enabledGauge.set(enabled ? 1 : 0);
    if (!enabled) {
      return;
    }

    final AccountRepository accountRepo;
    final TransactionIntentApplier worker;
    try {
      accountRepo = accounts.get();
      worker = applier.get();
    } catch (Throwable ignored) {
      return;
    }

    final long now = System.currentTimeMillis();
    lastTickStartMs.set(now);
    running.set(1);
    tickCounter.increment();

    long maxMillis =
        cfg.getOptionalValue("floecat.transaction.apply.max-tick-millis", Long.class).orElse(4000L);
    long deadline = now + Math.max(100L, maxMillis);

    int pageSize =
        cfg.getOptionalValue("floecat.transaction.apply.accounts-page-size", Integer.class)
            .orElse(50);

    try {
      tickTimer.record(
          () -> {
            List<Account> page = new ArrayList<>();
            String token = "";
            StringBuilder next = new StringBuilder();
            do {
              page.clear();
              page.addAll(accountRepo.list(pageSize, token, next));
              for (Account account : page) {
                if (System.currentTimeMillis() > deadline) {
                  return;
                }
                accountTimer.record(
                    () -> {
                      var res = worker.runForAccount(account.getResourceId().getId(), deadline);
                      accountCounter.increment();
                      intentsScanned.increment(res.scanned());
                      intentsApplied.increment(res.applied());
                      intentsSkipped.increment(res.skipped());
                    });
              }
              token = next.toString();
              next.setLength(0);
            } while (!token.isEmpty() && System.currentTimeMillis() <= deadline);
          });
    } finally {
      running.set(0);
      lastTickEndMs.set(System.currentTimeMillis());
    }
  }

  public static final class DisabledOrStopping implements Scheduled.SkipPredicate {
    private static volatile boolean stopping;

    @Override
    public boolean test(ScheduledExecution execution) {
      return stopping;
    }

    static void signalStopping() {
      stopping = true;
    }
  }
}
