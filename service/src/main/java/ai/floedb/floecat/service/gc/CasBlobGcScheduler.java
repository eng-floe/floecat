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
import ai.floedb.floecat.storage.kv.dynamodb.DynamoDbBootstrapReadiness;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

@ApplicationScoped
public class CasBlobGcScheduler {

  private static final Logger LOG = Logger.getLogger(CasBlobGcScheduler.class);

  @Inject Provider<AccountRepository> accounts;
  @Inject Provider<CasBlobGc> casBlobGc;
  @Inject Observability observability;

  private GcMetrics gcMetrics;
  private final AtomicInteger running = new AtomicInteger(0);
  private final AtomicInteger enabledGauge = new AtomicInteger(0);
  private final AtomicLong lastTickStartMs = new AtomicLong(0);
  private final AtomicLong lastTickEndMs = new AtomicLong(0);
  // Backlog health: last wall-clock ms each present account completed a CLEAN (unpoisoned, fully
  // reached) sweep. A poisoned or deadline-starved account keeps its stale timestamp, so its age
  // climbs — the direct "GC is falling behind on this account" signal.
  private final Map<String, Long> lastCleanSweepMs = new ConcurrentHashMap<>();
  private final AtomicInteger poisonedAccountsLastTick = new AtomicInteger(0);
  private final AtomicInteger deleteUnsupportedAccountsLastTick = new AtomicInteger(0);
  private ScheduledTaskMetrics taskMetrics;

  private volatile boolean stopping;

  @PostConstruct
  void initMeters() {
    this.gcMetrics = new GcMetrics(observability, "service", "gc.cas", "cas");
    this.taskMetrics = new ScheduledTaskMetrics(observability, "service", "gc.cas", "cas");
    registerGauges();
  }

  private void registerGauges() {
    taskMetrics.gaugeRunning(() -> (double) running.get(), "CAS GC running flag");
    taskMetrics.gaugeEnabled(() -> (double) enabledGauge.get(), "CAS GC enabled");
    taskMetrics.gaugeLastTickStart(
        () -> (double) lastTickStartMs.get(), "CAS GC last tick start millis");
    taskMetrics.gaugeLastTickEnd(() -> (double) lastTickEndMs.get(), "CAS GC last tick end millis");
    observability.gauge(
        ServiceMetrics.Gc.CAS_POISONED_ACCOUNTS,
        () -> (double) poisonedAccountsLastTick.get(),
        "Accounts whose CAS GC delete phase was poisoned in the last tick",
        Tag.of(TagKey.COMPONENT, "service"),
        Tag.of(TagKey.OPERATION, "gc_cas"));
    observability.gauge(
        ServiceMetrics.Gc.CAS_DELETE_UNSUPPORTED_ACCOUNTS,
        () -> (double) deleteUnsupportedAccountsLastTick.get(),
        "Accounts whose CAS GC sweep was skipped: store cannot delete by immutable version",
        Tag.of(TagKey.COMPONENT, "service"),
        Tag.of(TagKey.OPERATION, "gc_cas"));
    observability.gauge(
        ServiceMetrics.Gc.CAS_OLDEST_SWEEP_AGE,
        this::oldestCleanSweepAgeMs,
        "Age in ms of the least-recently cleanly-swept account (GC backlog signal)",
        Tag.of(TagKey.COMPONENT, "service"),
        Tag.of(TagKey.OPERATION, "gc_cas"));
  }

  /** Age of the least-recently cleanly-swept account; 0 when nothing is tracked yet. */
  private double oldestCleanSweepAgeMs() {
    long oldest = Long.MAX_VALUE;
    for (long ts : lastCleanSweepMs.values()) {
      if (ts < oldest) {
        oldest = ts;
      }
    }
    return oldest == Long.MAX_VALUE ? 0.0 : Math.max(0, System.currentTimeMillis() - oldest);
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
    int poisonedThisTick = 0;
    int deleteUnsupportedThisTick = 0;
    try {
      List<Account> allAccounts = fetchAllAccounts(accountRepo, accountsPageSize);

      // Backlog bookkeeping: forget accounts that no longer exist, and seed the first sight of a
      // new account with "now" so its age starts at 0 and grows only if it is never cleanly swept.
      Set<String> present =
          allAccounts.stream().map(a -> a.getResourceId().getId()).collect(Collectors.toSet());
      lastCleanSweepMs.keySet().retainAll(present);
      for (String id : present) {
        lastCleanSweepMs.putIfAbsent(id, now);
      }

      Collections.shuffle(allAccounts);

      for (Account account : allAccounts) {
        if (System.currentTimeMillis() >= deadline || stopping) {
          break;
        }
        long accountStart = System.nanoTime();
        String accountId = account.getResourceId().getId();
        CasBlobGc.Result result;
        try {
          result = gc.runForAccount(accountId);
        } catch (RuntimeException e) {
          // Isolate one account's failure from the rest of the tick. A version-targeted delete
          // throws StorageAbortRetryableException on a transient SDK fault and maps non-404 S3
          // errors (e.g. 403 AccessDenied when the role lacks s3:DeleteObjectVersion) — unguarded,
          // one such fault would skip every remaining shuffled account. Treat it like a poisoned
          // sweep (backlog age keeps climbing) and move on.
          LOG.warnf(
              e, "cas gc for account %s failed; skipping to next account this tick", accountId);
          poisonedThisTick++;
          gcMetrics.recordPause(
              Duration.ofNanos(System.nanoTime() - accountStart),
              Tag.of(TagKey.RESULT, "account-error"));
          continue;
        }
        if (result.deletesUnsupported()) {
          // Fail-closed skip (store cannot delete by immutable version): nothing was collected,
          // so the account's backlog age must keep climbing, exactly like a poisoned sweep.
          deleteUnsupportedThisTick++;
        } else if (result.poisoned()) {
          poisonedThisTick++;
        } else {
          // A clean, fully-reached sweep resets this account's backlog age.
          lastCleanSweepMs.put(accountId, System.currentTimeMillis());
        }
        gcMetrics.recordCollection(
            result.pointersScanned(), Tag.of(TagKey.RESULT, "pointers-scanned"));
        gcMetrics.recordCollection(result.blobsScanned(), Tag.of(TagKey.RESULT, "blobs-scanned"));
        gcMetrics.recordCollection(result.blobsDeleted(), Tag.of(TagKey.RESULT, "blobs-deleted"));
        gcMetrics.recordCollection(result.blobsRescued(), Tag.of(TagKey.RESULT, "blobs-rescued"));
        gcMetrics.recordCollection(result.referenced(), Tag.of(TagKey.RESULT, "referenced"));
        gcMetrics.recordCollection(result.tablesScanned(), Tag.of(TagKey.RESULT, "tables-scanned"));
        gcMetrics.recordPause(
            Duration.ofNanos(System.nanoTime() - accountStart),
            Tag.of(TagKey.RESULT, "account-run"));
      }
    } finally {
      poisonedAccountsLastTick.set(poisonedThisTick);
      deleteUnsupportedAccountsLastTick.set(deleteUnsupportedThisTick);
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
      return !enabled || stopping || DynamoDbBootstrapReadiness.shouldWaitForBootstrap();
    }
  }
}
