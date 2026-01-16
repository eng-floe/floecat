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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.microprofile.config.ConfigProvider;

@ApplicationScoped
public class PointerGcScheduler {

  @Inject Provider<AccountRepository> accounts;
  @Inject Provider<PointerGc> pointerGc;
  @Inject MeterRegistry registry;

  private Counter tickCounter;
  private Counter accountCounter;
  private Counter pointersScannedCounter;
  private Counter pointersDeletedCounter;
  private Counter missingBlobsCounter;
  private Counter staleSecondariesCounter;
  private Timer tickTimer;
  private Timer globalTimer;
  private Timer accountTimer;
  private final AtomicInteger running = new AtomicInteger(0);
  private final AtomicInteger enabledGauge = new AtomicInteger(0);
  private final AtomicLong lastTickStartMs = new AtomicLong(0);
  private final AtomicLong lastTickEndMs = new AtomicLong(0);

  private volatile boolean stopping;

  @PostConstruct
  void initMeters() {
    tickCounter =
        Counter.builder("floecat_gc_pointer_ticks")
            .description("Scheduler ticks")
            .register(registry);
    accountCounter =
        Counter.builder("floecat_gc_pointer_accounts")
            .description("Accounts processed per tick")
            .register(registry);
    pointersScannedCounter =
        Counter.builder("floecat_gc_pointer_pointers_scanned")
            .description("Pointers scanned by pointer GC")
            .register(registry);
    pointersDeletedCounter =
        Counter.builder("floecat_gc_pointer_pointers_deleted")
            .description("Pointers deleted by pointer GC")
            .register(registry);
    missingBlobsCounter =
        Counter.builder("floecat_gc_pointer_missing_blobs")
            .description("Pointers referencing missing blobs during pointer GC")
            .register(registry);
    staleSecondariesCounter =
        Counter.builder("floecat_gc_pointer_stale_secondaries")
            .description("Stale secondary pointers detected by pointer GC")
            .register(registry);
    tickTimer =
        Timer.builder("floecat_gc_pointer_tick_duration")
            .description("Duration of pointer GC ticks")
            .register(registry);
    globalTimer =
        Timer.builder("floecat_gc_pointer_global_duration")
            .description("Duration of pointer GC global account pass")
            .register(registry);
    accountTimer =
        Timer.builder("floecat_gc_pointer_account_duration")
            .description("Duration of pointer GC per account")
            .register(registry);
    registry.gauge("floecat_gc_pointer_running", running);
    registry.gauge("floecat_gc_pointer_enabled", enabledGauge);
    registry.gauge("floecat_gc_pointer_last_tick_start_ms", lastTickStartMs);
    registry.gauge("floecat_gc_pointer_last_tick_end_ms", lastTickEndMs);
  }

  void onStop(@Observes ShutdownEvent ev) {
    stopping = true;
    DisabledOrStopping.signalStopping();
  }

  @Scheduled(
      every = "{floecat.gc.pointer.tick-every}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
      skipExecutionIf = DisabledOrStopping.class)
  void tick() {
    if (stopping) {
      return;
    }

    var cfg = ConfigProvider.getConfig();
    boolean enabled =
        cfg.getOptionalValue("floecat.gc.pointer.enabled", Boolean.class).orElse(true);
    enabledGauge.set(enabled ? 1 : 0);
    if (!enabled) {
      return;
    }

    final AccountRepository accountRepo;
    final PointerGc gc;
    try {
      accountRepo = accounts.get();
      gc = pointerGc.get();
    } catch (Throwable ignored) {
      return;
    }

    final long now = System.currentTimeMillis();
    lastTickStartMs.set(now);
    running.set(1);
    tickCounter.increment();

    final long maxTickMillis =
        cfg.getOptionalValue("floecat.gc.pointer.max-tick-millis", Long.class).orElse(4000L);
    final int accountsPageSize =
        cfg.getOptionalValue("floecat.gc.pointer.accounts-page-size", Integer.class).orElse(200);
    final long deadline = now + maxTickMillis;

    Timer.Sample tickSample = Timer.start(registry);
    try {
      Timer.Sample globalSample = Timer.start(registry);
      var globalResult = gc.runGlobalAccountPointers(deadline);
      globalSample.stop(globalTimer);
      pointersScannedCounter.increment(globalResult.scanned());
      pointersDeletedCounter.increment(globalResult.deleted());
      missingBlobsCounter.increment(globalResult.missingBlobs());
      staleSecondariesCounter.increment(globalResult.staleSecondaries());

      List<Account> allAccounts = fetchAllAccounts(accountRepo, accountsPageSize);
      Collections.shuffle(allAccounts);

      for (Account account : allAccounts) {
        if (System.currentTimeMillis() >= deadline || stopping) {
          break;
        }
        Timer.Sample accountSample = Timer.start(registry);
        var result = gc.runForAccount(account.getResourceId().getId(), deadline);
        accountSample.stop(accountTimer);
        accountCounter.increment();
        pointersScannedCounter.increment(result.scanned());
        pointersDeletedCounter.increment(result.deleted());
        missingBlobsCounter.increment(result.missingBlobs());
        staleSecondariesCounter.increment(result.staleSecondaries());
      }
    } finally {
      tickSample.stop(tickTimer);
      lastTickEndMs.set(System.currentTimeMillis());
      running.set(0);
    }
  }

  private static List<Account> fetchAllAccounts(AccountRepository repo, int pageSize) {
    List<Account> out = new ArrayList<>();
    String tok = "";
    StringBuilder next = new StringBuilder();
    do {
      var page = repo.list(pageSize, tok, next);
      out.addAll(page);
      tok = next.toString();
      next.setLength(0);
    } while (!tok.isBlank());
    return out;
  }

  public static final class DisabledOrStopping implements Scheduled.SkipPredicate {
    private static volatile boolean stopping;

    static void signalStopping() {
      stopping = true;
    }

    @Override
    public boolean test(ScheduledExecution execution) {
      boolean enabled =
          ConfigProvider.getConfig()
              .getOptionalValue("floecat.gc.pointer.enabled", Boolean.class)
              .orElse(true);
      return !enabled || stopping;
    }
  }
}
