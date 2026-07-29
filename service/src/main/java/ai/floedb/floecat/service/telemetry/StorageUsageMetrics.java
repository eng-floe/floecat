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

import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Publishes the latest per-account storage estimate observed by CAS GC.
 *
 * <p>This class deliberately performs no storage reads. CAS GC supplies values from pointer rows it
 * already traverses, so storage telemetry adds no pointer-store or object-store pressure.
 */
@ApplicationScoped
public class StorageUsageMetrics {
  @Inject Observability observability;

  private final Map<String, AtomicLong> accountBytes = new ConcurrentHashMap<>();
  private final Map<String, AtomicLong> accountPointers = new ConcurrentHashMap<>();
  private final Map<String, AtomicLong> accountSizeCoveragePpm = new ConcurrentHashMap<>();

  public StorageUsageMetrics() {}

  /** Test/embedded constructor. */
  public StorageUsageMetrics(Observability observability) {
    this.observability = observability;
  }

  /**
   * Records one completed CAS-GC mark estimate. Byte coverage is parts per million so the gauge can
   * use an atomic integral supplier while exporting a stable 0..1 ratio.
   */
  public void recordGcEstimate(
      String accountId,
      long pointersScanned,
      long referencedBytes,
      long sizedBlobPointers,
      long blobPointers) {
    if (accountId == null || accountId.isBlank()) {
      return;
    }
    updateGauge(
        accountPointers,
        ServiceMetrics.Storage.ACCOUNT_GC_ESTIMATED_POINTERS,
        accountId,
        Math.max(0L, pointersScanned));
    updateGauge(
        accountBytes,
        ServiceMetrics.Storage.ACCOUNT_GC_ESTIMATED_BYTES,
        accountId,
        Math.max(0L, referencedBytes));
    long coveragePpm =
        blobPointers <= 0L
            ? 0L
            : Math.min(
                1_000_000L,
                Math.round(1_000_000.0d * Math.max(0L, sizedBlobPointers) / (double) blobPointers));
    AtomicLong coverage =
        accountSizeCoveragePpm.computeIfAbsent(
            accountId,
            tid -> {
              AtomicLong holder = new AtomicLong();
              observability.gauge(
                  ServiceMetrics.Storage.ACCOUNT_GC_SIZE_COVERAGE,
                  () -> holder.get() / 1_000_000.0d,
                  "Fraction of CAS-GC-scanned blob pointers carrying size metadata",
                  Tag.of(TagKey.ACCOUNT, tid));
              return holder;
            });
    coverage.set(coveragePpm);
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
