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
public class CasBlobGcScheduler {

  @Inject Provider<AccountRepository> accounts;
  @Inject Provider<CasBlobGc> casBlobGc;
  @Inject Observability observability;

  private GcMetrics gcMetrics;
  private final AtomicInteger running = new AtomicInteger(0);
  private final AtomicInteger enabledGauge = new AtomicInteger(0);
  private final AtomicLong lastTickStartMs = new AtomicLong(0);
  private final AtomicLong lastTickEndMs = new AtomicLong(0);

  private volatile boolean stopping;

  @PostConstruct
  void initMeters() {
    this.gcMetrics = new GcMetrics(observability, "service", "gc.cas", "cas");
    registerGauges();
  }

  private void registerGauges() {
    observability.gauge(
        ServiceMetrics.GC.CAS_RUNNING, () -> (double) running.get(), "CAS GC running flag");
    observability.gauge(
        ServiceMetrics.GC.CAS_ENABLED, () -> (double) enabledGauge.get(), "CAS GC enabled");
    observability.gauge(
        ServiceMetrics.GC.CAS_LAST_TICK_START,
        () -> (double) lastTickStartMs.get(),
        "CAS GC last tick start millis");
    observability.gauge(
        ServiceMetrics.GC.CAS_LAST_TICK_END,
        () -> (double) lastTickEndMs.get(),
        "CAS GC last tick end millis");
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
    gcMetrics.recordCollection(1, Tag.of(TagKey.RESULT, "tick"));

    final long maxTickMillis =
        cfg.getOptionalValue("floecat.gc.cas.max-tick-millis", Long.class).orElse(4000L);
    final int accountsPageSize =
        cfg.getOptionalValue("floecat.gc.cas.accounts-page-size", Integer.class).orElse(200);
    final long deadline = now + maxTickMillis;

    long tickStart = System.nanoTime();
    try {
      List<Account> allAccounts = fetchAllAccounts(accountRepo, accountsPageSize);
      Collections.shuffle(allAccounts);

      for (Account account : allAccounts) {
        if (System.currentTimeMillis() >= deadline || stopping) {
          break;
        }
        long accountStart = System.nanoTime();
        var result = gc.runForAccount(account.getResourceId().getId());
        gcMetrics.recordCollection(
            result.pointersScanned(), Tag.of(TagKey.RESULT, "pointers-scanned"));
        gcMetrics.recordCollection(result.blobsScanned(), Tag.of(TagKey.RESULT, "blobs-scanned"));
        gcMetrics.recordCollection(result.blobsDeleted(), Tag.of(TagKey.RESULT, "blobs-deleted"));
        gcMetrics.recordCollection(result.referenced(), Tag.of(TagKey.RESULT, "referenced"));
        gcMetrics.recordCollection(result.tablesScanned(), Tag.of(TagKey.RESULT, "tables-scanned"));
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
