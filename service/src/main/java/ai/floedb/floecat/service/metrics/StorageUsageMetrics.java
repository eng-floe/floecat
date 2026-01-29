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

package ai.floedb.floecat.service.metrics;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
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
  @Inject MeterRegistry registry;

  @ConfigProperty(name = "floecat.metrics.storage.refresh", defaultValue = "30s")
  String refreshEvery;

  @ConfigProperty(name = "floecat.metrics.storage.page-size", defaultValue = "200")
  int pageSize;

  @ConfigProperty(name = "floecat.metrics.storage.sample-max", defaultValue = "200")
  int sampleMax;

  @ConfigProperty(name = "floecat.metrics.storage.default-avg-bytes", defaultValue = "0")
  long defaultAvgBytes;

  private final Map<String, AtomicLong> accountBytes = new ConcurrentHashMap<>();
  private final Map<String, AtomicLong> accountPointers = new ConcurrentHashMap<>();

  private Counter refreshErrors;
  private Timer refreshTimer;

  @PostConstruct
  void init() {
    refreshErrors =
        Counter.builder("account.storage.refresh.errors")
            .description("Errors during storage metrics refresh")
            .register(registry);

    refreshTimer =
        Timer.builder("account.storage.refresh.ms")
            .description("Refresh duration (ms)")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);
  }

  @Scheduled(every = "${floecat.metrics.storage.refresh:30s}")
  void refresh() {
    var sample = Timer.start(registry);

    try {
      String token = "";
      StringBuilder next = new StringBuilder();
      do {
        var page = accounts.list(200, token, next);
        token = next.toString();
        next.setLength(0);

        for (var t : page) {
          final String accountId = t.getResourceId().getId();
          try {
            int ptrCount = pointerStore.countByPrefix(Keys.accountRootPointer(accountId));
            setGauge(
                accountPointers,
                "account.pointers.count",
                "Pointer count per account",
                accountId,
                ptrCount);

            long bytes = estimateBytesForAccount(accountId, ptrCount);
            setGauge(
                accountBytes,
                "account.storage.bytes",
                "Approximate blob bytes per account",
                accountId,
                bytes);

          } catch (Throwable e) {
            refreshErrors.increment();
          }
        }
      } while (!token.isBlank());
    } finally {
      sample.stop(refreshTimer);
    }
  }

  private void setGauge(
      Map<String, AtomicLong> map, String meterName, String desc, String accountId, long value) {
    map.computeIfAbsent(
            accountId,
            tid -> {
              var holder = new AtomicLong(0L);
              Gauge.builder(meterName, holder, AtomicLong::get)
                  .description(desc)
                  .tag("account", tid)
                  .register(registry);
              return holder;
            })
        .set(value);
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
