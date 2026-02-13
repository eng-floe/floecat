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
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.helpers.GcMetrics;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.scheduler.ScheduledExecution;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import java.time.Duration;
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
  @Inject Observability observability;
  private GcMetrics gcMetrics;
  private final AtomicInteger running = new AtomicInteger(0);
  private final AtomicInteger enabledGauge = new AtomicInteger(0);
  private final AtomicLong lastTickStartMs = new AtomicLong(0);
  private final AtomicLong lastTickEndMs = new AtomicLong(0);

  private volatile boolean stopping;

  @PostConstruct
  void initMeters() {
    this.gcMetrics = new GcMetrics(observability, "service", "gc.pointer", "pointer");
    registerGauges();
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
    gcMetrics.recordCollection(1, Tag.of(TagKey.RESULT, "tick"));

    final long maxTickMillis =
        cfg.getOptionalValue("floecat.gc.pointer.max-tick-millis", Long.class).orElse(4000L);
    final int accountsPageSize =
        cfg.getOptionalValue("floecat.gc.pointer.accounts-page-size", Integer.class).orElse(200);
    final long deadline = now + maxTickMillis;

    long tickStart = System.nanoTime();
    try {
      var globalResult = gc.runGlobalAccountPointers(deadline);
      gcMetrics.recordCollection(globalResult.scanned(), Tag.of(TagKey.RESULT, "global-scanned"));
      gcMetrics.recordCollection(globalResult.deleted(), Tag.of(TagKey.RESULT, "global-deleted"));
      gcMetrics.recordCollection(
          globalResult.missingBlobs(), Tag.of(TagKey.RESULT, "missing-blobs"));
      gcMetrics.recordCollection(
          globalResult.staleSecondaries(), Tag.of(TagKey.RESULT, "stale-secondaries"));

      List<Account> allAccounts = fetchAllAccounts(accountRepo, accountsPageSize);
      Collections.shuffle(allAccounts);

      for (Account account : allAccounts) {
        if (System.currentTimeMillis() >= deadline || stopping) {
          break;
        }
        long accountStart = System.nanoTime();
        var result = gc.runForAccount(account.getResourceId().getId(), deadline);
        gcMetrics.recordCollection(result.scanned(), Tag.of(TagKey.RESULT, "account-scanned"));
        gcMetrics.recordCollection(result.deleted(), Tag.of(TagKey.RESULT, "account-deleted"));
        gcMetrics.recordCollection(result.missingBlobs(), Tag.of(TagKey.RESULT, "missing-blobs"));
        gcMetrics.recordCollection(
            result.staleSecondaries(), Tag.of(TagKey.RESULT, "stale-secondaries"));
        gcMetrics.recordPause(
            Duration.ofNanos(System.nanoTime() - accountStart),
            Tag.of(TagKey.RESULT, "account-run"));
      }
    } finally {
      gcMetrics.recordPause(
          Duration.ofNanos(System.nanoTime() - tickStart), Tag.of(TagKey.RESULT, "tick"));
      lastTickEndMs.set(System.currentTimeMillis());
      running.set(0);
    }
  }

  private void registerGauges() {
    Tag gcTag = Tag.of(TagKey.GC_NAME, "pointer");
    observability.gauge(
        ServiceMetrics.GC.SCHEDULER_RUNNING,
        () -> (double) running.get(),
        "Pointer GC running flag",
        gcTag);
    observability.gauge(
        ServiceMetrics.GC.SCHEDULER_ENABLED,
        () -> (double) enabledGauge.get(),
        "Pointer GC enabled",
        gcTag);
    observability.gauge(
        ServiceMetrics.GC.SCHEDULER_LAST_TICK_START,
        () -> (double) lastTickStartMs.get(),
        "Pointer GC last tick start millis",
        gcTag);
    observability.gauge(
        ServiceMetrics.GC.SCHEDULER_LAST_TICK_END,
        () -> (double) lastTickEndMs.get(),
        "Pointer GC last tick end millis",
        gcTag);
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
