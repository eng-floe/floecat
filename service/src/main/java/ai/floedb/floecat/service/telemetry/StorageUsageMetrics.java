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

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.helpers.StoreMetrics;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class StorageUsageMetrics {

  @Inject AccountRepository accounts;
  @Inject PointerStore pointerStore;
  @Inject BlobStore blobStore;
  @Inject Observability observability;
  private final Map<String, AtomicLong> accountBytes = new ConcurrentHashMap<>();
  private final Map<String, AtomicLong> accountPointers = new ConcurrentHashMap<>();
  private StoreMetrics storeMetrics;

  @ConfigProperty(name = "floecat.metrics.storage.refresh", defaultValue = "30s")
  String refreshEvery;

  @ConfigProperty(name = "floecat.metrics.storage.page-size", defaultValue = "200")
  int pageSize;

  @ConfigProperty(name = "floecat.metrics.storage.sample-max", defaultValue = "200")
  int sampleMax;

  @ConfigProperty(name = "floecat.metrics.storage.default-avg-bytes", defaultValue = "0")
  long defaultAvgBytes;

  @PostConstruct
  void init() {
    storeMetrics = new StoreMetrics(observability, "service", "storage.refresh");
  }

  @Scheduled(every = "${floecat.metrics.storage.refresh:30s}")
  void refresh() {
    long refreshStart = System.nanoTime();
    boolean error = false;

    try {
      String token = "";
      StringBuilder next = new StringBuilder();
      do {
        var page = accounts.list(200, token, next);
        token = next.toString();
        next.setLength(0);

        for (var t : page) {
          final String accountId = t.getResourceId().getId();
          long accountStart = System.nanoTime();
          try {
            int ptrCount = pointerStore.countByPrefix(Keys.accountRootPointer(accountId));
            updateGauge(
                accountPointers, ServiceMetrics.Storage.ACCOUNT_POINTERS, accountId, ptrCount);

            long bytes = estimateBytesForAccount(accountId, ptrCount);
            updateGauge(accountBytes, ServiceMetrics.Storage.ACCOUNT_BYTES, accountId, bytes);

            storeMetrics.recordRequest("success", Tag.of(TagKey.ACCOUNT, accountId));
            storeMetrics.recordBytes(bytes, "success", Tag.of(TagKey.ACCOUNT, accountId));
            storeMetrics.recordLatency(
                Duration.ofNanos(System.nanoTime() - accountStart),
                "success",
                Tag.of(TagKey.ACCOUNT, accountId));

          } catch (Throwable e) {
            error = true;
            storeMetrics.recordRequest("error", Tag.of(TagKey.ACCOUNT, accountId));
            storeMetrics.recordLatency(
                Duration.ofNanos(System.nanoTime() - accountStart),
                "error",
                Tag.of(TagKey.ACCOUNT, accountId));
          }
        }
      } while (!token.isBlank());
    } finally {
      Duration refreshDuration = Duration.ofNanos(System.nanoTime() - refreshStart);
      storeMetrics.recordLatency(
          refreshDuration,
          error ? "error" : "success",
          Tag.of(TagKey.RESULT, error ? "error" : "success"));
    }
  }

  private void updateGauge(
      Map<String, AtomicLong> map, MetricId metric, String accountId, long value) {
    accountObservers(map, metric, accountId).set(value);
  }

  private AtomicLong accountObservers(
      Map<String, AtomicLong> map, MetricId metric, String accountId) {
    return map.computeIfAbsent(
        accountId,
        tid -> {
          AtomicLong holder = new AtomicLong(0L);
          observability.gauge(
              metric, holder::get, "Storage account metric", Tag.of(TagKey.ACCOUNT, tid));
          return holder;
        });
  }

  public long estimateBytesForAccount(String accountId, int knownTotalObjects) {
    final String accountPrefix = Keys.accountRootPointer(accountId);
    final int totalObjects =
        knownTotalObjects >= 0 ? knownTotalObjects : pointerStore.countByPrefix(accountPrefix);

    if (totalObjects == 0) {
      return 0L;
    }

    long sampleBytes = 0L;
    int sampleCount = 0;

    String token = "";
    while (sampleCount < sampleMax) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(accountPrefix, pageSize, token, next);
      if (pointers.isEmpty()) {
        break;
      }

      for (Pointer pointer : pointers) {
        if (sampleCount >= sampleMax) {
          break;
        }

        try {
          var hdrOpt = blobStore.head(pointer.getBlobUri());
          if (hdrOpt.isPresent()) {
            long len = hdrOpt.get().getContentLength();
            if (len > 0) {
              sampleBytes += len;
              sampleCount++;
            }
          }
        } catch (Throwable ignore) {
        }
      }
      token = next.toString();
      if (token.isBlank()) {
        break;
      }
    }

    if (sampleCount == 0) {
      return defaultAvgBytes * (long) totalObjects;
    }

    double avg = (double) sampleBytes / (double) sampleCount;
    return (long) Math.round(avg * (double) totalObjects);
  }
}
