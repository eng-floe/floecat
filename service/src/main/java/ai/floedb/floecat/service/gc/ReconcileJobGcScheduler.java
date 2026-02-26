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
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.helpers.GcMetrics;
import ai.floedb.floecat.telemetry.helpers.ScheduledTaskMetrics;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.microprofile.config.ConfigProvider;

@ApplicationScoped
public class ReconcileJobGcScheduler {

  @Inject Provider<AccountRepository> accounts;
  @Inject Provider<ReconcileJobGc> reconcileJobGc;
  @Inject Observability observability;

  private GcMetrics gcMetrics;
  private final AtomicInteger running = new AtomicInteger(0);
  private final AtomicInteger enabledGauge = new AtomicInteger(0);
  private final AtomicLong lastTickStartMs = new AtomicLong(0);
  private final AtomicLong lastTickEndMs = new AtomicLong(0);
  private ScheduledTaskMetrics taskMetrics;

  private final Map<String, String> jobTokenByAccount = new ConcurrentHashMap<>();
  private final Map<String, String> dedupeTokenByAccount = new ConcurrentHashMap<>();
  private volatile String readyToken = "";
  private volatile boolean stopping;

  @PostConstruct
  void initMeters() {
    this.gcMetrics = new GcMetrics(observability, "service", "gc.reconcile-jobs", "reconcile-jobs");
    this.taskMetrics =
        new ScheduledTaskMetrics(observability, "service", "gc.reconcile-jobs", "reconcile-jobs");
    registerGauges();
  }

  private void registerGauges() {
    taskMetrics.gaugeRunning(() -> (double) running.get(), "Reconcile job GC running flag");
    taskMetrics.gaugeEnabled(() -> (double) enabledGauge.get(), "Reconcile job GC enabled");
    taskMetrics.gaugeLastTickStart(
        () -> (double) lastTickStartMs.get(), "Reconcile job GC last tick start millis");
    taskMetrics.gaugeLastTickEnd(
        () -> (double) lastTickEndMs.get(), "Reconcile job GC last tick end millis");
  }

  void onStop(@Observes ShutdownEvent ev) {
    stopping = true;
    DisabledOrStopping.signalStopping();
  }

  @Scheduled(
      every = "{floecat.gc.reconcile-jobs.tick-every}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
      skipExecutionIf = DisabledOrStopping.class)
  void tick() {
    if (stopping) {
      return;
    }

    var cfg = ConfigProvider.getConfig();
    boolean enabled =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.enabled", Boolean.class).orElse(true);
    enabledGauge.set(enabled ? 1 : 0);
    if (!enabled) {
      return;
    }

    final AccountRepository accountRepo;
    final ReconcileJobGc gc;
    try {
      accountRepo = accounts.get();
      gc = reconcileJobGc.get();
    } catch (Throwable ignored) {
      return;
    }

    final long now = System.currentTimeMillis();
    lastTickStartMs.set(now);
    running.set(1);
    gcMetrics.recordCollection(1, Tag.of(TagKey.RESULT, "tick"));

    final long maxTickMillis =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.max-tick-millis", Long.class).orElse(4000L);
    final int accountsPageSize =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.accounts-page-size", Integer.class)
            .orElse(200);
    final long deadline = now + maxTickMillis;

    long tickStart = System.nanoTime();
    try {
      long readyStart = System.nanoTime();
      var readyResult = gc.runReadySlice(readyToken);
      gcMetrics.recordCollection(readyResult.scanned(), Tag.of(TagKey.RESULT, "ready-scanned"));
      gcMetrics.recordCollection(readyResult.deleted(), Tag.of(TagKey.RESULT, "ready-deleted"));
      gcMetrics.recordPause(
          Duration.ofNanos(System.nanoTime() - readyStart), Tag.of(TagKey.RESULT, "ready-slice"));
      readyToken = readyResult.nextToken() == null ? "" : readyResult.nextToken();

      List<Account> allAccounts = fetchAllAccounts(accountRepo, accountsPageSize);
      Collections.shuffle(allAccounts);

      for (Account account : allAccounts) {
        if (System.currentTimeMillis() >= deadline || stopping) {
          break;
        }

        String accountId = account.getResourceId().getId();
        String jobToken = jobTokenByAccount.getOrDefault(accountId, "");
        String dedupeToken = dedupeTokenByAccount.getOrDefault(accountId, "");

        long sliceStart = System.nanoTime();
        var result = gc.runAccountSlice(accountId, jobToken, dedupeToken);
        gcMetrics.recordCollection(result.scanned(), Tag.of(TagKey.RESULT, "account-scanned"));
        gcMetrics.recordCollection(result.expired(), Tag.of(TagKey.RESULT, "expired"));
        gcMetrics.recordCollection(result.ptrDeleted(), Tag.of(TagKey.RESULT, "ptr-deleted"));
        gcMetrics.recordCollection(result.blobDeleted(), Tag.of(TagKey.RESULT, "blob-deleted"));
        gcMetrics.recordCollection(result.dedupeDeleted(), Tag.of(TagKey.RESULT, "dedupe-deleted"));
        gcMetrics.recordCollection(result.readyDeleted(), Tag.of(TagKey.RESULT, "ready-deleted"));
        gcMetrics.recordPause(
            Duration.ofNanos(System.nanoTime() - sliceStart),
            Tag.of(TagKey.RESULT, "account-slice"));

        if (result.nextJobToken() == null || result.nextJobToken().isBlank()) {
          jobTokenByAccount.remove(accountId);
        } else {
          jobTokenByAccount.put(accountId, result.nextJobToken());
        }
        if (result.nextDedupeToken() == null || result.nextDedupeToken().isBlank()) {
          dedupeTokenByAccount.remove(accountId);
        } else {
          dedupeTokenByAccount.put(accountId, result.nextDedupeToken());
        }
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
              .getOptionalValue("floecat.gc.reconcile-jobs.enabled", Boolean.class)
              .orElse(true);
      return !enabled || stopping;
    }
  }
}
