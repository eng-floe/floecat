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

import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.ReconcileMode;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
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

      long now = System.currentTimeMillis();
      long deadline = now + Math.max(1000L, maxTickMillis);
      if (defaultIntervalMs <= 0L) {
        result = "disabled";
        return;
      }

      String accountToken = "";
      while (System.currentTimeMillis() < deadline) {
        StringBuilder accountNext = new StringBuilder();
        var accountRows = accounts.list(accountsPageSize, accountToken, accountNext);
        for (var account : accountRows) {
          if (System.currentTimeMillis() >= deadline) {
            break;
          }
          String connectorToken = "";
          String accountId = account.getResourceId().getId();
          while (System.currentTimeMillis() < deadline) {
            StringBuilder connectorNext = new StringBuilder();
            var connectorRows =
                connectors.list(accountId, connectorsPageSize, connectorToken, connectorNext);
            for (Connector connector : connectorRows) {
              if (System.currentTimeMillis() >= deadline) {
                break;
              }
              maybeEnqueue(connector, defaultIntervalMs, defaultMode);
            }
            connectorToken = connectorNext.toString();
            if (connectorToken.isBlank()) {
              break;
            }
          }
        }
        accountToken = accountNext.toString();
        if (accountToken.isBlank()) {
          break;
        }
      }
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

  private void maybeEnqueue(
      Connector connector, long defaultIntervalMs, ReconcileMode defaultMode) {
    if (connector == null || !connector.hasResourceId()) {
      observePlannerEnqueue("skipped", "incremental", "missing_connector");
      return;
    }
    String mode = modeValue(effectiveMode(connector, defaultMode));
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
    boolean fullRescan = "full".equals(mode);
    long now = System.currentTimeMillis();
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
      jobs.enqueue(
          connector.getResourceId().getAccountId(),
          connector.getResourceId().getId(),
          fullRescan,
          CaptureMode.METADATA_AND_STATS,
          ai.floedb.floecat.reconciler.jobs.ReconcileScope.empty());
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
}
