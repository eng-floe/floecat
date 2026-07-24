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

package ai.floedb.floecat.service.telemetry;

import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.ObservationScope;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.helpers.StoreMetrics;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
public class StorageUsageMetrics {
  @Inject AccountRepository accounts;
  @Inject PointerStore pointerStore;
  @Inject Observability observability;
  @Inject Tracer tracer;

  private final Map<String, AtomicLong> accountBytes = new ConcurrentHashMap<>();
  private final Map<String, AtomicLong> accountPointers = new ConcurrentHashMap<>();
  private StoreMetrics storeMetrics;

  @PostConstruct
  void init() {
    storeMetrics = new StoreMetrics(observability, "service", "storage.refresh");
  }

  @Scheduled(
      every = "${floecat.metrics.storage.refresh:30s}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
  void refresh() {
    long startedNanos = System.nanoTime();
    Span refreshSpan =
        tracer
            .spanBuilder("service.storage.refresh")
            .setAttribute("floecat.component", "service")
            .setAttribute("floecat.operation", "storage.refresh")
            .startSpan();
    try (Scope ignored = refreshSpan.makeCurrent()) {
      ObservationScope refreshScope = storeMetrics.observe();
      boolean error = false;
      try {
        String token = "";
        StringBuilder next = new StringBuilder();
        do {
          var page = accounts.list(200, token, next);
          token = next.toString();
          next.setLength(0);
          for (var account : page) {
            String accountId = account.getResourceId().getId();
            try {
              var usage =
                  StorageAccountingPointerStore.decodeUsage(
                      pointerStore.get(Keys.accountStorageUsagePointer(accountId)).orElse(null));
              updateGauge(
                  accountPointers,
                  ServiceMetrics.Storage.ACCOUNT_POINTERS,
                  accountId,
                  usage.pointers());
              updateGauge(
                  accountBytes, ServiceMetrics.Storage.ACCOUNT_BYTES, accountId, usage.bytes());
              storeMetrics.recordBytes(usage.bytes(), "success", Tag.of(TagKey.ACCOUNT, accountId));
              storeMetrics.recordRequest("success", Tag.of(TagKey.ACCOUNT, accountId));
            } catch (RuntimeException e) {
              error = true;
              observability.counter(
                  ServiceMetrics.Storage.FAILURES,
                  1.0,
                  Tag.of(TagKey.OPERATION, "refresh"),
                  Tag.of(TagKey.ACCOUNT, accountId));
            }
          }
        } while (!token.isBlank());
      } finally {
        if (error) {
          refreshScope.error(new IllegalStateException("storage refresh encountered errors"));
        } else {
          refreshScope.success();
        }
        refreshScope.close();
      }
    } finally {
      refreshSpan.end();
      observability.timer(
          ServiceMetrics.Storage.REFRESH_DURATION,
          Duration.ofNanos(System.nanoTime() - startedNanos),
          Tag.of(TagKey.OPERATION, "refresh"));
    }
  }

  private void updateGauge(
      Map<String, AtomicLong> map, MetricId metric, String accountId, long value) {
    map.computeIfAbsent(
            accountId,
            tid -> {
              AtomicLong holder = new AtomicLong();
              observability.gauge(
                  metric, holder::get, "Storage account metric", Tag.of(TagKey.ACCOUNT, tid));
              return holder;
            })
        .set(value);
  }
}
