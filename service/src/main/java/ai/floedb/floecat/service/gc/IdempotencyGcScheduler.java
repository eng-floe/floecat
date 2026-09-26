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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.microprofile.config.ConfigProvider;

@ApplicationScoped
public class IdempotencyGcScheduler {

  @Inject Provider<OwnedAccounts> accounts;
  @Inject Provider<IdempotencyGc> idempotencyGc;
  @Inject Observability observability;

  private GcMetrics gcMetrics;
  private final AtomicInteger running = new AtomicInteger(0);
  private final AtomicInteger enabledGauge = new AtomicInteger(0);
  private final AtomicLong lastTickStartMs = new AtomicLong(0);
  private final AtomicLong lastTickEndMs = new AtomicLong(0);
  private ScheduledTaskMetrics taskMetrics;

  @PostConstruct
  void initMeters() {
    this.gcMetrics = new GcMetrics(observability, "service", "gc.idempotency", "idempotency");
    this.taskMetrics =
        new ScheduledTaskMetrics(observability, "service", "gc.idempotency", "idempotency");
    registerGauges();
  }

  private void registerGauges() {
    taskMetrics.gaugeRunning(() -> (double) running.get(), "Idempotency GC running flag");
    taskMetrics.gaugeEnabled(() -> (double) enabledGauge.get(), "Idempotency GC enabled");
    taskMetrics.gaugeLastTickStart(
        () -> (double) lastTickStartMs.get(), "Idempotency GC last tick start millis");
    taskMetrics.gaugeLastTickEnd(
        () -> (double) lastTickEndMs.get(), "Idempotency GC last tick end millis");
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

    final OwnedAccounts accountRepo;
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
    gcMetrics.recordCollection(1, Tag.of(TagKey.RESULT, "tick"));

    final long maxTickMillis =
        cfg.getOptionalValue("floecat.gc.idempotency.max-tick-millis", Long.class).orElse(4000L);
    final int accountsPageSize =
        cfg.getOptionalValue("floecat.gc.idempotency.accounts-page-size", Integer.class)
            .orElse(200);
    final long deadline = now + maxTickMillis;

    long tickStart = System.nanoTime();
    try {
      var allAccounts = accountRepo.listAll(accountsPageSize);
      Collections.shuffle(allAccounts);

      for (var account : allAccounts) {
        if (System.currentTimeMillis() >= deadline || stopping) {
          break;
        }

        String accountId = account.getResourceId().getId();
        String token = tokenByAccount.getOrDefault(accountId, "");

        long sliceStart = System.nanoTime();
        IdempotencyGc.Result result = gc.runSliceForAccount(accountId, token);
        gcMetrics.recordCollection(result.scanned(), Tag.of(TagKey.RESULT, "scanned"));
        gcMetrics.recordCollection(result.expired(), Tag.of(TagKey.RESULT, "expired"));
        gcMetrics.recordCollection(result.ptrDeleted(), Tag.of(TagKey.RESULT, "ptr-deleted"));
        gcMetrics.recordCollection(result.blobDeleted(), Tag.of(TagKey.RESULT, "blob-deleted"));
        gcMetrics.recordPause(
            Duration.ofNanos(System.nanoTime() - sliceStart), Tag.of(TagKey.RESULT, "slice"));

        if (result.nextToken() == null || result.nextToken().isBlank()) {
          tokenByAccount.remove(accountId);
        } else {
          tokenByAccount.put(accountId, result.nextToken());
        }
      }
    } finally {
      gcMetrics.recordPause(
          Duration.ofNanos(System.nanoTime() - tickStart), Tag.of(TagKey.RESULT, "tick"));
      lastTickEndMs.set(System.currentTimeMillis());
      running.set(0);
    }
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
      return !enabled || stopping || DynamoDbBootstrapReadiness.shouldWaitForBootstrap();
    }
  }
}
