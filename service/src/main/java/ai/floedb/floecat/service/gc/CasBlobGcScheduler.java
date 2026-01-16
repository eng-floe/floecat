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
public class CasBlobGcScheduler {

  @Inject Provider<AccountRepository> accounts;
  @Inject Provider<CasBlobGc> casBlobGc;
  @Inject MeterRegistry registry;

  private Counter tickCounter;
  private Counter accountCounter;
  private Counter pointersScannedCounter;
  private Counter blobsScannedCounter;
  private Counter blobsDeletedCounter;
  private Counter referencedCounter;
  private Counter tablesScannedCounter;
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
        Counter.builder("floecat_gc_cas_ticks").description("Scheduler ticks").register(registry);
    accountCounter =
        Counter.builder("floecat_gc_cas_accounts")
            .description("Accounts processed per tick")
            .register(registry);
    pointersScannedCounter =
        Counter.builder("floecat_gc_cas_pointers_scanned")
            .description("Pointers scanned by CAS GC")
            .register(registry);
    blobsScannedCounter =
        Counter.builder("floecat_gc_cas_blobs_scanned")
            .description("Blobs scanned by CAS GC")
            .register(registry);
    blobsDeletedCounter =
        Counter.builder("floecat_gc_cas_blobs_deleted")
            .description("Blobs deleted by CAS GC")
            .register(registry);
    referencedCounter =
        Counter.builder("floecat_gc_cas_referenced")
            .description("Referenced blobs discovered by CAS GC")
            .register(registry);
    tablesScannedCounter =
        Counter.builder("floecat_gc_cas_tables_scanned")
            .description("Tables scanned by CAS GC")
            .register(registry);
    tickTimer =
        Timer.builder("floecat_gc_cas_tick_duration")
            .description("Duration of CAS GC ticks")
            .register(registry);
    accountTimer =
        Timer.builder("floecat_gc_cas_account_duration")
            .description("Duration of CAS GC per account")
            .register(registry);
    registry.gauge("floecat_gc_cas_running", running);
    registry.gauge("floecat_gc_cas_enabled", enabledGauge);
    registry.gauge("floecat_gc_cas_last_tick_start_ms", lastTickStartMs);
    registry.gauge("floecat_gc_cas_last_tick_end_ms", lastTickEndMs);
  }

  void onStop(@Observes ShutdownEvent ev) {
    stopping = true;
    DisabledOrStopping.signalStopping();
  }

  @Scheduled(
      every = "{floecat.gc.cas.tick-every}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
      skipExecutionIf = DisabledOrStopping.class)
  void tick() {
    if (stopping) {
      return;
    }

    var cfg = ConfigProvider.getConfig();
    boolean enabled = cfg.getOptionalValue("floecat.gc.cas.enabled", Boolean.class).orElse(true);
    enabledGauge.set(enabled ? 1 : 0);
    if (!enabled) {
      return;
    }

    final AccountRepository accountRepo;
    final CasBlobGc gc;
    try {
      accountRepo = accounts.get();
      gc = casBlobGc.get();
    } catch (Throwable ignored) {
      return;
    }

    final long now = System.currentTimeMillis();
    lastTickStartMs.set(now);
    running.set(1);
    tickCounter.increment();

    final long maxTickMillis =
        cfg.getOptionalValue("floecat.gc.cas.max-tick-millis", Long.class).orElse(4000L);
    final int accountsPageSize =
        cfg.getOptionalValue("floecat.gc.cas.accounts-page-size", Integer.class).orElse(200);
    final long deadline = now + maxTickMillis;

    Timer.Sample tickSample = Timer.start(registry);
    try {
      List<Account> allAccounts = fetchAllAccounts(accountRepo, accountsPageSize);
      Collections.shuffle(allAccounts);

      for (Account account : allAccounts) {
        if (System.currentTimeMillis() >= deadline || stopping) {
          break;
        }
        Timer.Sample accountSample = Timer.start(registry);
        var result = gc.runForAccount(account.getResourceId().getId());
        accountSample.stop(accountTimer);
        accountCounter.increment();
        pointersScannedCounter.increment(result.pointersScanned());
        blobsScannedCounter.increment(result.blobsScanned());
        blobsDeletedCounter.increment(result.blobsDeleted());
        referencedCounter.increment(result.referenced());
        tablesScannedCounter.increment(result.tablesScanned());
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
              .getOptionalValue("floecat.gc.cas.enabled", Boolean.class)
              .orElse(true);
      return !enabled || stopping;
    }
  }
}
