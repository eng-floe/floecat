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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.microprofile.config.ConfigProvider;

@ApplicationScoped
public class IdempotencyGcScheduler {

  @Inject Provider<AccountRepository> accounts;
  @Inject Provider<IdempotencyGc> idempotencyGc;
  @Inject MeterRegistry registry;

  private Counter tickCounter;
  private Counter sliceCounter;
  private Counter scannedCounter;
  private Counter expiredCounter;
  private Counter ptrDeletedCounter;
  private Counter blobDeletedCounter;
  private Timer tickTimer;
  private Timer sliceTimer;
  private final AtomicInteger running = new AtomicInteger(0);
  private final AtomicInteger enabledGauge = new AtomicInteger(0);
  private final AtomicLong lastTickStartMs = new AtomicLong(0);
  private final AtomicLong lastTickEndMs = new AtomicLong(0);

  @PostConstruct
  void initMeters() {
    tickCounter =
        Counter.builder("floecat_gc_idempotency_ticks")
            .description("Scheduler ticks")
            .register(registry);
    sliceCounter =
        Counter.builder("floecat_gc_idempotency_slices")
            .description("Per-account slices")
            .register(registry);
    scannedCounter =
        Counter.builder("floecat_gc_idempotency_scanned")
            .description("Idempotency pointers scanned")
            .register(registry);
    expiredCounter =
        Counter.builder("floecat_gc_idempotency_expired")
            .description("Expired idempotency records found")
            .register(registry);
    ptrDeletedCounter =
        Counter.builder("floecat_gc_idempotency_ptr_deleted")
            .description("Idempotency pointers deleted")
            .register(registry);
    blobDeletedCounter =
        Counter.builder("floecat_gc_idempotency_blob_deleted")
            .description("Idempotency blobs deleted")
            .register(registry);
    tickTimer =
        Timer.builder("floecat_gc_idempotency_tick_duration")
            .description("Duration of idempotency GC ticks")
            .register(registry);
    sliceTimer =
        Timer.builder("floecat_gc_idempotency_slice_duration")
            .description("Duration of idempotency GC per account slice")
            .register(registry);
    registry.gauge("floecat_gc_idempotency_running", running);
    registry.gauge("floecat_gc_idempotency_enabled", enabledGauge);
    registry.gauge("floecat_gc_idempotency_last_tick_start_ms", lastTickStartMs);
    registry.gauge("floecat_gc_idempotency_last_tick_end_ms", lastTickEndMs);
  }

  private final Map<String, String> tokenByAccount = new ConcurrentHashMap<>();
  private volatile boolean stopping;

  void onStop(@Observes ShutdownEvent ev) {
    stopping = true;
    DisabledOrStopping.signalStopping();
  }

  @Scheduled(
      every = "{floecat.gc.idempotency.tick-every}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
      skipExecutionIf = DisabledOrStopping.class)
  void tick() {
    if (stopping) {
      return;
    }

    var cfg = ConfigProvider.getConfig();
    boolean enabled =
        cfg.getOptionalValue("floecat.gc.idempotency.enabled", Boolean.class).orElse(true);
    enabledGauge.set(enabled ? 1 : 0);
    if (!enabled) {
      return;
    }

    final AccountRepository accountRepo;
    final IdempotencyGc gc;
    try {
      accountRepo = accounts.get();
      gc = idempotencyGc.get();
    } catch (Throwable ignored) {
      return;
    }

    final long now = System.currentTimeMillis();
    lastTickStartMs.set(now);
    running.set(1);
    tickCounter.increment();

    final long maxTickMillis =
        cfg.getOptionalValue("floecat.gc.idempotency.max-tick-millis", Long.class).orElse(4000L);
    final int accountsPageSize =
        cfg.getOptionalValue("floecat.gc.idempotency.accounts-page-size", Integer.class)
            .orElse(200);
    final long deadline = now + maxTickMillis;

    Timer.Sample tickSample = Timer.start(registry);
    try {
      List<Account> allAccounts = fetchAllAccounts(accountRepo, accountsPageSize);
      Collections.shuffle(allAccounts);

      for (Account account : allAccounts) {
        if (System.currentTimeMillis() >= deadline || stopping) {
          break;
        }

        String accountId = account.getResourceId().getId();
        String token = tokenByAccount.getOrDefault(accountId, "");

        Timer.Sample sliceSample = Timer.start(registry);
        var result = gc.runSliceForAccount(accountId, token);
        sliceSample.stop(sliceTimer);
        sliceCounter.increment();
        scannedCounter.increment(result.scanned());
        expiredCounter.increment(result.expired());
        ptrDeletedCounter.increment(result.ptrDeleted());
        blobDeletedCounter.increment(result.blobDeleted());

        if (result.nextToken() == null || result.nextToken().isBlank()) {
          tokenByAccount.remove(accountId);
        } else {
          tokenByAccount.put(accountId, result.nextToken());
        }
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
              .getOptionalValue("floecat.gc.idempotency.enabled", Boolean.class)
              .orElse(true);
      return !enabled || stopping;
    }
  }
}
