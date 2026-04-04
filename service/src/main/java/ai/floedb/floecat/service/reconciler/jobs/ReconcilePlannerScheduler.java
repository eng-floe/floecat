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

package ai.floedb.floecat.service.reconciler.jobs;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.ReconcileMode;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.service.gc.ReconcileJobGcScheduler;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcilePlannerScheduler {
  private static final Logger LOG = Logger.getLogger(ReconcilePlannerScheduler.class);

  @Inject AccountRepository accounts;
  @Inject ConnectorRepository connectors;
  @Inject ReconcileJobStore jobs;
  @Inject ReconcilerSettingsStore settings;
  @Inject Observability observability;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final Map<String, Long> lastEnqueueMs = new ConcurrentHashMap<>();
  private volatile PlannerCursor plannerCursor = PlannerCursor.start();

  @Scheduled(
      every = "{floecat.reconciler.auto.tick-every:30s}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
      skipExecutionIf = ReconcileJobGcScheduler.DisabledOrStopping.class)
  void tick() {
    long startedNanos = System.nanoTime();
    if (!running.compareAndSet(false, true)) {
      return;
    }
    String result = "success";
    String reason = null;
    try {
      var cfg = ConfigProvider.getConfig();
      boolean enabled = settings.isAutoEnabled();
      if (!enabled) {
        result = "disabled";
        return;
      }
      long maxTickMillis =
          cfg.getOptionalValue("floecat.reconciler.auto.max-tick-millis", Long.class).orElse(4000L);
      int accountsPageSize =
          cfg.getOptionalValue("floecat.reconciler.auto.accounts-page-size", Integer.class)
              .orElse(50);
      int connectorsPageSize =
          cfg.getOptionalValue("floecat.reconciler.auto.connectors-page-size", Integer.class)
              .orElse(200);
      long defaultIntervalMs = settings.defaultIntervalMs();
      ReconcileMode defaultMode = settings.defaultMode();
      if (defaultIntervalMs <= 0L) {
        result = "disabled";
        return;
      }
      long deadline = nowMs() + Math.max(1000L, maxTickMillis);
      runPlannerPass(
          deadline, accountsPageSize, connectorsPageSize, defaultIntervalMs, defaultMode);
    } catch (RuntimeException e) {
      result = "error";
      reason = normalizeReason(e);
      LOG.debugf(e, "Reconcile planner tick failed");
    } finally {
      observePlannerTick(
          result, reason, Duration.ofNanos(Math.max(0L, System.nanoTime() - startedNanos)));
      running.set(false);
    }
  }

  void runPlannerPass(
      long deadlineMs,
      int accountsPageSize,
      int connectorsPageSize,
      long defaultIntervalMs,
      ReconcileMode defaultMode) {
    String accountToken = plannerCursor.accountToken();
    while (nowMs() < deadlineMs) {
      StringBuilder accountNext = new StringBuilder();
      List<Account> accountRows;
      try {
        accountRows = accounts.list(accountsPageSize, accountToken, accountNext);
      } catch (IllegalArgumentException e) {
        if (accountToken.isBlank()) {
          throw e;
        }
        LOG.debugf(e, "Resetting planner cursor after invalid account page token");
        plannerCursor = PlannerCursor.start();
        return;
      }
      if (accountRows.isEmpty()) {
        plannerCursor = PlannerCursor.start();
        return;
      }
      for (Account account : accountRows) {
        if (nowMs() >= deadlineMs) {
          plannerCursor = new PlannerCursor(accountToken);
          return;
        }
        String accountId = account.getResourceId().getId();
        processAccount(accountId, deadlineMs, connectorsPageSize, defaultIntervalMs, defaultMode);
      }
      if (accountNext.length() == 0) {
        plannerCursor = PlannerCursor.start();
        return;
      }
      accountToken = accountNext.toString();
      plannerCursor = new PlannerCursor(accountToken);
    }
    plannerCursor = new PlannerCursor(accountToken);
  }

  private void processAccount(
      String accountId,
      long deadlineMs,
      int connectorsPageSize,
      long defaultIntervalMs,
      ReconcileMode defaultMode) {
    String connectorToken = "";
    while (nowMs() < deadlineMs) {
      StringBuilder connectorNext = new StringBuilder();
      List<Connector> connectorRows;
      try {
        connectorRows =
            connectors.list(accountId, connectorsPageSize, connectorToken, connectorNext);
      } catch (IllegalArgumentException e) {
        if (connectorToken.isBlank()) {
          throw e;
        }
        LOG.debugf(
            e,
            "Stopping connector scan after invalid page token account=%s token=%s",
            accountId,
            connectorToken);
        return;
      }
      if (connectorRows.isEmpty()) {
        return;
      }
      for (Connector connector : connectorRows) {
        if (nowMs() >= deadlineMs) {
          return;
        }
        maybeEnqueue(connector, defaultIntervalMs, defaultMode);
      }
      if (connectorNext.length() == 0) {
        return;
      }
      String nextToken = connectorNext.toString();
      if (nextToken.equals(connectorToken)) {
        LOG.warnf(
            "Stopping connector scan after stagnant page token account=%s token=%s",
            accountId, connectorToken);
        return;
      }
      connectorToken = nextToken;
    }
  }

  PlannerCursor plannerCursor() {
    return plannerCursor;
  }

  ReconcileExecutionPolicy autoExecutionPolicy() {
    var cfg = ConfigProvider.getConfig();
    return ReconcileExecutionPolicy.of(
        ReconcileExecutionClass.fromString(
            cfg.getOptionalValue("floecat.reconciler.auto.execution-class", String.class)
                .orElse("DEFAULT")),
        cfg.getOptionalValue("floecat.reconciler.auto.execution-lane", String.class).orElse(""),
        java.util.Map.of());
  }

  long nowMs() {
    return System.currentTimeMillis();
  }

  private void maybeEnqueue(
      Connector connector, long defaultIntervalMs, ReconcileMode defaultMode) {
    if (connector == null || !connector.hasResourceId()) {
      observePlannerEnqueue("skipped", "n_a", "missing_connector");
      return;
    }
    if (isAutoPolicyDisabled(connector)) {
      observePlannerEnqueue("skipped", "n_a", "policy_disabled");
      return;
    }
    ReconcileMode effectiveMode = effectiveMode(connector, defaultMode);
    String mode = modeValue(effectiveMode);
    if (connector.getState() != ConnectorState.CS_ACTIVE) {
      observePlannerEnqueue("skipped", mode, "inactive");
      return;
    }
    long intervalMs = effectiveIntervalMs(connector, defaultIntervalMs);
    if (intervalMs <= 0L) {
      observePlannerEnqueue("skipped", mode, "interval_disabled");
      return;
    }

    String key = connector.getResourceId().getAccountId() + ":" + connector.getResourceId().getId();
    boolean fullRescan = effectiveMode == ReconcileMode.RM_FULL;
    long now = nowMs();
    long notBeforeMs = 0L;
    if (connector.hasPolicy() && connector.getPolicy().hasNotBefore()) {
      notBeforeMs =
          com.google.protobuf.util.Timestamps.toMillis(connector.getPolicy().getNotBefore());
    }
    long last = lastEnqueueMs.getOrDefault(key, 0L);
    long dueMs = Math.max(last + intervalMs, notBeforeMs);
    if (now < dueMs) {
      observePlannerEnqueue("skipped", mode, "not_due");
      return;
    }

    try {
      jobs.enqueuePlan(
          connector.getResourceId().getAccountId(),
          connector.getResourceId().getId(),
          fullRescan,
          CaptureMode.METADATA_AND_STATS,
          ai.floedb.floecat.reconciler.jobs.ReconcileScope.empty(),
          autoExecutionPolicy(),
          "");
      lastEnqueueMs.put(key, now);
      observePlannerEnqueue("enqueued", mode, null);
    } catch (RuntimeException e) {
      observePlannerEnqueue("error", mode, normalizeReason(e));
      LOG.debugf(
          e,
          "Reconcile planner enqueue failed account=%s connector=%s",
          connector.getResourceId().getAccountId(),
          connector.getResourceId().getId());
    }
  }

  private static long effectiveIntervalMs(Connector connector, long defaultIntervalMs) {
    if (connector.hasPolicy()
        && connector.getPolicy().getEnabled()
        && connector.getPolicy().hasInterval()) {
      Duration override =
          Duration.ofSeconds(connector.getPolicy().getInterval().getSeconds())
              .plusNanos(connector.getPolicy().getInterval().getNanos());
      long overrideMs = override.toMillis();
      if (overrideMs > 0L) {
        return overrideMs;
      }
    }
    return defaultIntervalMs;
  }

  private static boolean isAutoPolicyDisabled(Connector connector) {
    return connector != null && connector.hasPolicy() && !connector.getPolicy().getEnabled();
  }

  private static ReconcileMode effectiveMode(Connector connector, ReconcileMode defaultMode) {
    if (connector.hasPolicy()
        && connector.getPolicy().getEnabled()
        && connector.getPolicy().getMode() != ReconcileMode.RM_UNSPECIFIED) {
      return connector.getPolicy().getMode();
    }
    return defaultMode == null || defaultMode == ReconcileMode.RM_UNSPECIFIED
        ? ReconcileMode.RM_INCREMENTAL
        : defaultMode;
  }

  private void observePlannerTick(String result, String reason, Duration latency) {
    if (observability == null) {
      return;
    }
    Tag[] tags = plannerTickTags(result, reason);
    observability.counter(ServiceMetrics.Reconcile.PLANNER_TICKS, 1.0, tags);
    observability.timer(ServiceMetrics.Reconcile.PLANNER_TICK_LATENCY, latency, tags);
  }

  private void observePlannerEnqueue(String result, String mode, String reason) {
    if (observability == null) {
      return;
    }
    if (reason == null || reason.isBlank()) {
      observability.counter(
          ServiceMetrics.Reconcile.PLANNER_ENQUEUE,
          1.0,
          Tag.of(TagKey.COMPONENT, "service"),
          Tag.of(TagKey.OPERATION, "planner_enqueue"),
          Tag.of(TagKey.RESULT, result),
          Tag.of(TagKey.MODE, mode));
      return;
    }
    observability.counter(
        ServiceMetrics.Reconcile.PLANNER_ENQUEUE,
        1.0,
        Tag.of(TagKey.COMPONENT, "service"),
        Tag.of(TagKey.OPERATION, "planner_enqueue"),
        Tag.of(TagKey.RESULT, result),
        Tag.of(TagKey.MODE, mode),
        Tag.of(TagKey.REASON, reason));
  }

  private static Tag[] plannerTickTags(String result, String reason) {
    if (reason == null || reason.isBlank()) {
      return new Tag[] {
        Tag.of(TagKey.COMPONENT, "service"),
        Tag.of(TagKey.OPERATION, "planner_tick"),
        Tag.of(TagKey.RESULT, result)
      };
    }
    return new Tag[] {
      Tag.of(TagKey.COMPONENT, "service"),
      Tag.of(TagKey.OPERATION, "planner_tick"),
      Tag.of(TagKey.RESULT, result),
      Tag.of(TagKey.REASON, reason)
    };
  }

  private static String modeValue(ReconcileMode mode) {
    return mode == ReconcileMode.RM_FULL ? "full" : "incremental";
  }

  private static String normalizeReason(Throwable t) {
    if (t == null) {
      return "unknown";
    }
    String simple = t.getClass().getSimpleName();
    if (simple == null || simple.isBlank()) {
      return "runtime_exception";
    }
    return simple
        .replaceAll("([a-z])([A-Z])", "$1_$2")
        .replaceAll("[^A-Za-z0-9_]+", "_")
        .toLowerCase(java.util.Locale.ROOT);
  }

  record PlannerCursor(String accountToken) {
    PlannerCursor {
      accountToken = accountToken == null ? "" : accountToken;
    }

    static PlannerCursor start() {
      return new PlannerCursor("");
    }
  }
}
