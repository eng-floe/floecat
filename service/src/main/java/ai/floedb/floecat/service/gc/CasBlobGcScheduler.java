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
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.service.telemetry.StorageUsageMetrics;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

@ApplicationScoped
public class CasBlobGcScheduler {

  private static final Logger LOG = Logger.getLogger(CasBlobGcScheduler.class);

  @Inject Provider<AccountRepository> accounts;
  @Inject Provider<CasBlobGc> casBlobGc;
  @Inject Provider<StorageUsageMetrics> storageUsageMetrics;
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
  private String continuationAccountId = "";
  private int consecutiveContinuationTicks;
  private String accountToken = "";
  private List<Account> accountPage = List.of();
  private int accountPageIndex;
  private String accountPageNextToken = "";
  private final Set<String> accountsSeenThisCycle = new HashSet<>();

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
    boolean enabled = cfg.getOptionalValue("floecat.gc.cas.enabled", Boolean.class).orElse(false);
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
        Math.max(
            1_000L,
            cfg.getOptionalValue("floecat.gc.cas.max-tick-millis", Long.class).orElse(45_000L));
    final int accountsPageSize =
        cfg.getOptionalValue("floecat.gc.cas.accounts-page-size", Integer.class).orElse(200);
    final int maxConsecutiveContinuationTicks =
        Math.max(
            1,
            cfg.getOptionalValue("floecat.gc.cas.max-consecutive-continuation-ticks", Integer.class)
                .orElse(10));
    final long deadline = now + maxTickMillis;

    long tickStart = System.nanoTime();
    int poisonedThisTick = 0;
    int deleteUnsupportedThisTick = 0;
    try {
      while (System.currentTimeMillis() < deadline && !stopping) {
        String retainedAccountId = gc.continuationAccountId().orElse("");
        boolean fromPage = retainedAccountId.isBlank();
        Account account;
        if (fromPage) {
          account = nextPagedAccount(accountRepo, accountsPageSize, deadline, gc, now);
          if (account == null) {
            break;
          }
        } else {
          account =
              accountRepo
                  .getById(
                      ResourceId.newBuilder()
                          .setId(retainedAccountId)
                          .setKind(ResourceKind.RK_ACCOUNT)
                          .build())
                  .orElse(null);
          if (account == null) {
            gc.abandonContinuation();
            continuationAccountId = "";
            consecutiveContinuationTicks = 0;
            lastCleanSweepMs.remove(retainedAccountId);
            continue;
          }
        }
        if (System.currentTimeMillis() >= deadline || stopping) {
          break;
        }
        long accountStart = System.nanoTime();
        String accountId = account.getResourceId().getId();
        CasBlobGc.Result result;
        try {
          result = gc.runForAccount(accountId, deadline);
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
          if (fromPage && advanceAccountCursor(gc)) {
            break;
          }
          continue;
        }
        boolean discoveryCycleComplete = fromPage && advanceAccountCursor(gc);
        if (result.deletesUnsupported()) {
          // Fail-closed skip (store cannot delete by immutable version): nothing was collected,
          // so the account's backlog age must keep climbing, exactly like a poisoned sweep.
          deleteUnsupportedThisTick++;
        } else if (result.poisoned()) {
          poisonedThisTick++;
        } else if (result.generationCleanupPending()) {
          // A bounded generation deletion has more work. Keep the backlog clock running until a
          // later pass drains it instead of reporting this account as fully swept.
        } else {
          // A clean, fully-reached sweep resets this account's backlog age.
          lastCleanSweepMs.put(accountId, System.currentTimeMillis());
        }
        if (!result.deletesUnsupported()
            && !result.poisoned()
            && gc.continuationAccountId().isEmpty()) {
          storageUsageMetrics
              .get()
              .recordGcEstimate(
                  accountId,
                  result.pointersScanned(),
                  result.referencedBytes(),
                  result.sizedBlobPointers(),
                  result.blobPointers());
        }
        gcMetrics.recordCollection(
            result.pointersScanned(), Tag.of(TagKey.RESULT, "pointers-scanned"));
        gcMetrics.recordCollection(result.blobsScanned(), Tag.of(TagKey.RESULT, "blobs-scanned"));
        gcMetrics.recordCollection(result.blobsDeleted(), Tag.of(TagKey.RESULT, "blobs-deleted"));
        gcMetrics.recordCollection(result.blobsRescued(), Tag.of(TagKey.RESULT, "blobs-rescued"));
        gcMetrics.recordCollection(
            result.referenced(), Tag.of(TagKey.RESULT, "reference-index-insertions"));
        gcMetrics.recordCollection(
            result.referenceIndexSaturationPpm(),
            Tag.of(TagKey.RESULT, "reference-index-saturation-ppm"));
        gcMetrics.recordCollection(
            result.referenceIndexEstimatedFalsePositivePpb(),
            Tag.of(TagKey.RESULT, "reference-index-estimated-fpp-ppb"));
        gcMetrics.recordCollection(result.tablesScanned(), Tag.of(TagKey.RESULT, "tables-scanned"));
        gcMetrics.recordPause(
            Duration.ofNanos(System.nanoTime() - accountStart),
            Tag.of(TagKey.RESULT, "account-run"));
        var continuing = gc.continuationAccountId();
        if (continuing.isPresent()) {
          String continuingAccountId = continuing.get();
          if (continuingAccountId.equals(continuationAccountId)) {
            consecutiveContinuationTicks++;
          } else {
            continuationAccountId = continuingAccountId;
            consecutiveContinuationTicks = 1;
          }
          // Keep only one local epoch at a time, but cap how long it can monopolize the scheduler.
          // An oversized account may need a larger cap; the backlog metric exposes that condition.
          if (consecutiveContinuationTicks >= maxConsecutiveContinuationTicks) {
            gc.abandonContinuation();
            continuationAccountId = "";
            consecutiveContinuationTicks = 0;
          }
          break;
        } else if (accountId.equals(continuationAccountId)) {
          continuationAccountId = "";
          consecutiveContinuationTicks = 0;
        }
        if (discoveryCycleComplete) {
          break;
        }
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

  private Account nextPagedAccount(
      AccountRepository repo, int pageSize, long deadline, CasBlobGc gc, long now) {
    while (System.currentTimeMillis() < deadline && !stopping) {
      if (accountPageIndex < accountPage.size()) {
        return accountPage.get(accountPageIndex);
      }
      StringBuilder next = new StringBuilder();
      List<Account> page = repo.list(pageSize, accountToken, next);
      accountPage = List.copyOf(page);
      accountPageIndex = 0;
      accountPageNextToken = next.toString();
      for (Account account : accountPage) {
        String accountId = account.getResourceId().getId();
        accountsSeenThisCycle.add(accountId);
        lastCleanSweepMs.putIfAbsent(accountId, now);
      }
      if (System.currentTimeMillis() >= deadline || stopping) {
        return null;
      }
      if (!accountPage.isEmpty()) {
        return accountPage.get(0);
      }
      accountToken = accountPageNextToken;
      accountPageNextToken = "";
      if (accountToken.isBlank()) {
        completeAccountDiscoveryCycle(gc);
        return null;
      }
    }
    return null;
  }

  private boolean advanceAccountCursor(CasBlobGc gc) {
    accountPageIndex++;
    if (accountPageIndex < accountPage.size()) {
      return false;
    }
    accountToken = accountPageNextToken;
    accountPage = List.of();
    accountPageIndex = 0;
    accountPageNextToken = "";
    if (!accountToken.isBlank()) {
      return false;
    }
    completeAccountDiscoveryCycle(gc);
    return true;
  }

  private void completeAccountDiscoveryCycle(CasBlobGc gc) {
    Set<String> present = Set.copyOf(accountsSeenThisCycle);
    gc.abandonContinuationIfAccountMissing(present);
    lastCleanSweepMs.keySet().retainAll(present);
    accountsSeenThisCycle.clear();
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
              .orElse(false);
      return !enabled || stopping || DynamoDbBootstrapReadiness.shouldWaitForBootstrap();
    }
  }
}
