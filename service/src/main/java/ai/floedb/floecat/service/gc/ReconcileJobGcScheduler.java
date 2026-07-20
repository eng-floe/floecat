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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileJobGcScheduler {
  private static final Logger LOG = Logger.getLogger(ReconcileJobGcScheduler.class);

  @Inject Provider<AccountRepository> accounts;
  @Inject Provider<ReconcileJobGc> reconcileJobGc;
  @Inject Observability observability;

  private GcMetrics gcMetrics;
  private final AtomicInteger running = new AtomicInteger(0);
  private final AtomicInteger enabledGauge = new AtomicInteger(0);
  private final AtomicLong lastTickStartMs = new AtomicLong(0);
  private final AtomicLong lastTickEndMs = new AtomicLong(0);
  private final AtomicInteger accountsProcessedLastTick = new AtomicInteger(0);
  private final AtomicInteger quarantinedLastTick = new AtomicInteger(0);
  private final AtomicInteger deletedLastTick = new AtomicInteger(0);
  private ScheduledTaskMetrics taskMetrics;

  private final Map<String, String> jobTokenByAccount = new ConcurrentHashMap<>();
  private final Map<String, String> canonicalQuarantineTokenByAccount = new ConcurrentHashMap<>();
  private final Map<String, String> dedupeTokenByAccount = new ConcurrentHashMap<>();
  private final Map<String, String> rootSummaryTokenByAccount = new ConcurrentHashMap<>();
  private final Map<String, String> connectorRootSummaryTokenByAccount = new ConcurrentHashMap<>();
  private volatile String readyToken = "";
  private volatile String accountToken = "";
  private volatile List<Account> accountPage = List.of();
  private volatile int accountPageIndex = 0;
  private volatile String accountPageNextToken = "";
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
    observability.gauge(
        ServiceMetrics.Gc.RECONCILE_JOB_ACCOUNTS_LAST_TICK,
        () -> (double) accountsProcessedLastTick.get(),
        "Reconcile job GC accounts processed in the last completed tick",
        Tag.of(TagKey.COMPONENT, "service"),
        Tag.of(TagKey.OPERATION, "gc_reconcile_jobs"));
    observability.gauge(
        ServiceMetrics.Gc.RECONCILE_JOB_ACCOUNT_PAGE_INDEX,
        () -> (double) accountPageIndex,
        "Reconcile job GC account page index",
        Tag.of(TagKey.COMPONENT, "service"),
        Tag.of(TagKey.OPERATION, "gc_reconcile_jobs"));
    observability.gauge(
        ServiceMetrics.Gc.RECONCILE_JOB_ACCOUNT_PAGE_SIZE,
        () -> (double) accountPage.size(),
        "Reconcile job GC account page size",
        Tag.of(TagKey.COMPONENT, "service"),
        Tag.of(TagKey.OPERATION, "gc_reconcile_jobs"));
    observability.gauge(
        ServiceMetrics.Gc.RECONCILE_JOB_ACTIVE_ACCOUNT_TOKENS,
        () ->
            (double)
                (jobTokenByAccount.size()
                    + canonicalQuarantineTokenByAccount.size()
                    + dedupeTokenByAccount.size()
                    + rootSummaryTokenByAccount.size()
                    + connectorRootSummaryTokenByAccount.size()),
        "Accounts with active reconcile job GC continuation tokens",
        Tag.of(TagKey.COMPONENT, "service"),
        Tag.of(TagKey.OPERATION, "gc_reconcile_jobs"));
    observability.gauge(
        ServiceMetrics.Gc.RECONCILE_JOB_QUARANTINED_LAST_TICK,
        () -> (double) quarantinedLastTick.get(),
        "Unreadable reconcile job GC payloads retained in the last completed tick",
        Tag.of(TagKey.COMPONENT, "service"),
        Tag.of(TagKey.OPERATION, "gc_reconcile_jobs"));
    observability.gauge(
        ServiceMetrics.Gc.RECONCILE_JOB_DELETED_LAST_TICK,
        () -> (double) deletedLastTick.get(),
        "Reconcile job GC deletes completed in the last completed tick",
        Tag.of(TagKey.COMPONENT, "service"),
        Tag.of(TagKey.OPERATION, "gc_reconcile_jobs"));
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
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.max-tick-millis", Long.class)
            .orElse(30_000L);
    final int accountsPageSize =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.accounts-page-size", Integer.class)
            .orElse(200);
    final long deadline = now + maxTickMillis;

    long tickStart = System.nanoTime();
    int accountsProcessed = 0;
    int totalAccountScanned = 0;
    int totalExpired = 0;
    int totalPtrDeleted = 0;
    int totalBlobDeleted = 0;
    int totalDedupeDeleted = 0;
    int totalReadyDeleted = 0;
    int totalQuarantined = 0;
    int readyScanned = 0;
    int readyDeleted = 0;
    int readyQuarantined = 0;
    try {
      long readyStart = System.nanoTime();
      var readyResult = gc.runReadySlice(readyToken);
      readyScanned = readyResult.scanned();
      readyDeleted = readyResult.deleted();
      readyQuarantined = readyResult.quarantined();
      gcMetrics.recordCollection(readyResult.scanned(), Tag.of(TagKey.RESULT, "ready-scanned"));
      gcMetrics.recordCollection(readyResult.deleted(), Tag.of(TagKey.RESULT, "ready-deleted"));
      gcMetrics.recordCollection(
          readyResult.quarantined(), Tag.of(TagKey.RESULT, "ready-quarantined"));
      gcMetrics.recordPause(
          Duration.ofNanos(System.nanoTime() - readyStart), Tag.of(TagKey.RESULT, "ready-slice"));
      readyToken = readyResult.nextToken() == null ? "" : readyResult.nextToken();

      while (System.currentTimeMillis() < deadline && !stopping) {
        if (accountPageIndex >= accountPage.size()) {
          StringBuilder next = new StringBuilder();
          List<Account> page =
              accountRepo.list(accountsPageSize, accountToken == null ? "" : accountToken, next);
          accountPage = List.copyOf(page);
          accountPageIndex = 0;
          accountPageNextToken = next.toString();
          if (accountPage.isEmpty()) {
            accountToken = "";
            accountPageNextToken = "";
            break;
          }
        }

        Account account = accountPage.get(accountPageIndex);
        String accountId = account.getResourceId().getId();
        String jobToken = jobTokenByAccount.getOrDefault(accountId, "");
        String canonicalQuarantineToken =
            canonicalQuarantineTokenByAccount.getOrDefault(accountId, "");
        String dedupeToken = dedupeTokenByAccount.getOrDefault(accountId, "");
        String rootSummaryToken = rootSummaryTokenByAccount.getOrDefault(accountId, "");
        String connectorRootSummaryToken =
            connectorRootSummaryTokenByAccount.getOrDefault(accountId, "");

        long sliceStart = System.nanoTime();
        try {
          var result =
              gc.runAccountSlice(
                  accountId,
                  jobToken,
                  canonicalQuarantineToken,
                  dedupeToken,
                  rootSummaryToken,
                  connectorRootSummaryToken);
          accountsProcessed++;
          totalAccountScanned += result.scanned();
          totalExpired += result.expired();
          totalPtrDeleted += result.ptrDeleted();
          totalBlobDeleted += result.blobDeleted();
          totalDedupeDeleted += result.dedupeDeleted();
          totalReadyDeleted += result.readyDeleted();
          totalQuarantined +=
              result.canonicalQuarantined()
                  + result.dedupeQuarantined()
                  + result.rootSummaryQuarantined();
          gcMetrics.recordCollection(result.scanned(), Tag.of(TagKey.RESULT, "account-scanned"));
          gcMetrics.recordCollection(result.expired(), Tag.of(TagKey.RESULT, "expired"));
          gcMetrics.recordCollection(result.ptrDeleted(), Tag.of(TagKey.RESULT, "ptr-deleted"));
          gcMetrics.recordCollection(result.blobDeleted(), Tag.of(TagKey.RESULT, "blob-deleted"));
          gcMetrics.recordCollection(
              result.dedupeDeleted(), Tag.of(TagKey.RESULT, "dedupe-deleted"));
          gcMetrics.recordCollection(result.readyDeleted(), Tag.of(TagKey.RESULT, "ready-deleted"));
          gcMetrics.recordCollection(
              result.canonicalQuarantined(), Tag.of(TagKey.RESULT, "canonical-quarantined"));
          gcMetrics.recordCollection(
              result.dedupeQuarantined(), Tag.of(TagKey.RESULT, "dedupe-quarantined"));
          gcMetrics.recordCollection(
              result.rootSummaryQuarantined(), Tag.of(TagKey.RESULT, "root-summary-quarantined"));

          updateAccountTokens(accountId, result);
        } catch (Throwable t) {
          gcMetrics.recordError(1, Tag.of(TagKey.RESULT, "account-failed"));
          LOG.warnf(t, "reconcile job gc account slice failed accountId=%s", accountId);
        } finally {
          gcMetrics.recordPause(
              Duration.ofNanos(System.nanoTime() - sliceStart),
              Tag.of(TagKey.RESULT, "account-slice"));
        }

        if (advanceAccountCursor()) {
          break;
        }
      }
    } catch (Throwable t) {
      gcMetrics.recordError(1, Tag.of(TagKey.RESULT, "tick-failed"));
      LOG.warnf(t, "reconcile job gc tick failed");
    } finally {
      int totalDeleted =
          totalPtrDeleted
              + totalBlobDeleted
              + totalDedupeDeleted
              + totalReadyDeleted
              + readyDeleted;
      int allQuarantined = totalQuarantined + readyQuarantined;
      accountsProcessedLastTick.set(accountsProcessed);
      quarantinedLastTick.set(allQuarantined);
      deletedLastTick.set(totalDeleted);
      gcMetrics.recordPause(
          Duration.ofNanos(System.nanoTime() - tickStart), Tag.of(TagKey.RESULT, "tick"));
      lastTickEndMs.set(System.currentTimeMillis());
      running.set(0);
      LOG.infof(
          "reconcile job gc tick summary accounts=%d readyScanned=%d readyDeleted=%d"
              + " readyQuarantined=%d accountScanned=%d expired=%d ptrDeleted=%d blobDeleted=%d"
              + " dedupeDeleted=%d readyPointerDeleted=%d quarantined=%d accountPageIndex=%d"
              + " accountPageSize=%d accountTokenPresent=%s activeAccountTokens=%d"
              + " durationMs=%d",
          accountsProcessed,
          readyScanned,
          readyDeleted,
          readyQuarantined,
          totalAccountScanned,
          totalExpired,
          totalPtrDeleted,
          totalBlobDeleted,
          totalDedupeDeleted,
          totalReadyDeleted,
          allQuarantined,
          accountPageIndex,
          accountPage.size(),
          accountToken != null && !accountToken.isBlank(),
          jobTokenByAccount.size()
              + canonicalQuarantineTokenByAccount.size()
              + dedupeTokenByAccount.size()
              + rootSummaryTokenByAccount.size()
              + connectorRootSummaryTokenByAccount.size(),
          Duration.ofNanos(System.nanoTime() - tickStart).toMillis());
    }
  }

  private void updateAccountTokens(String accountId, ReconcileJobGc.AccountResult result) {
    if (result.nextJobToken() == null || result.nextJobToken().isBlank()) {
      jobTokenByAccount.remove(accountId);
    } else {
      jobTokenByAccount.put(accountId, result.nextJobToken());
    }
    if (result.nextCanonicalQuarantineToken() == null
        || result.nextCanonicalQuarantineToken().isBlank()) {
      canonicalQuarantineTokenByAccount.remove(accountId);
    } else {
      canonicalQuarantineTokenByAccount.put(accountId, result.nextCanonicalQuarantineToken());
    }
    if (result.nextDedupeToken() == null || result.nextDedupeToken().isBlank()) {
      dedupeTokenByAccount.remove(accountId);
    } else {
      dedupeTokenByAccount.put(accountId, result.nextDedupeToken());
    }
    if (result.nextRootSummaryToken() == null || result.nextRootSummaryToken().isBlank()) {
      rootSummaryTokenByAccount.remove(accountId);
    } else {
      rootSummaryTokenByAccount.put(accountId, result.nextRootSummaryToken());
    }
    if (result.nextConnectorRootSummaryToken() == null
        || result.nextConnectorRootSummaryToken().isBlank()) {
      connectorRootSummaryTokenByAccount.remove(accountId);
    } else {
      connectorRootSummaryTokenByAccount.put(accountId, result.nextConnectorRootSummaryToken());
    }
  }

  private boolean advanceAccountCursor() {
    accountPageIndex++;
    if (accountPageIndex < accountPage.size()) {
      return false;
    }
    accountToken = accountPageNextToken == null ? "" : accountPageNextToken;
    accountPage = List.of();
    accountPageIndex = 0;
    accountPageNextToken = "";
    return accountToken.isBlank();
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
      return !enabled || stopping || DynamoDbBootstrapReadiness.shouldWaitForBootstrap();
    }
  }
}
