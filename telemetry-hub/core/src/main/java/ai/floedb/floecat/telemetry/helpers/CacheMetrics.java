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

package ai.floedb.floecat.telemetry.helpers;

import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/** Helper that emits cache-related metrics with canonical tags. */
public final class CacheMetrics extends BaseMetrics {
  private final Observability observability;

  public CacheMetrics(
      Observability observability,
      String component,
      String operation,
      String cacheName,
      Tag... tags) {
    super(component, operation, tagged(cacheName, tags));
    this.observability = Objects.requireNonNull(observability, "observability");
  }

  private static Tag[] tagged(String cacheName, Tag... tags) {
    Tag[] result = new Tag[(tags == null ? 0 : tags.length) + 1];
    result[0] = Tag.of(TagKey.CACHE_NAME, cacheName);
    if (tags != null && tags.length > 0) {
      System.arraycopy(tags, 0, result, 1, tags.length);
    }
    return result;
  }

  public void recordHit(Tag... extraTags) {
    observability.counter(Telemetry.Metrics.CACHE_HITS, 1, metricTags(extraTags));
  }

  public void recordMiss(Tag... extraTags) {
    observability.counter(Telemetry.Metrics.CACHE_MISSES, 1, metricTags(extraTags));
  }

  public void trackSize(Supplier<? extends Number> supplier, String description, Tag... extraTags) {
    registerGauge(Telemetry.Metrics.CACHE_SIZE, supplier, description, metricTags(extraTags));
  }

  public void trackEnabled(
      Supplier<? extends Number> supplier, String description, Tag... extraTags) {
    registerGauge(Telemetry.Metrics.CACHE_ENABLED, supplier, description, metricTags(extraTags));
  }

  public void trackMaxEntries(
      Supplier<? extends Number> supplier, String description, Tag... extraTags) {
    registerGauge(
        Telemetry.Metrics.CACHE_MAX_ENTRIES, supplier, description, metricTags(extraTags));
  }

  public void trackMaxWeight(
      Supplier<? extends Number> supplier, String description, Tag... extraTags) {
    registerGauge(Telemetry.Metrics.CACHE_MAX_WEIGHT, supplier, description, metricTags(extraTags));
  }

  public void trackAccounts(
      Supplier<? extends Number> supplier, String description, Tag... extraTags) {
    registerGauge(Telemetry.Metrics.CACHE_ACCOUNTS, supplier, description, metricTags(extraTags));
  }

  public void trackWeightedSize(
      Supplier<? extends Number> supplier, String description, Tag... extraTags) {
    registerGauge(
        Telemetry.Metrics.CACHE_WEIGHTED_SIZE, supplier, description, metricTags(extraTags));
  }

  private void registerGauge(
      MetricId metric, Supplier<? extends Number> supplier, String description, Tag... tags) {
    observability.gauge(metric, safeSupplier(supplier), description, tags);
  }

  public void recordLoad(Duration duration, boolean hit, Tag... extraTags) {
    List<Tag> dynamic = new ArrayList<>();
    dynamic.add(Tag.of(TagKey.RESULT, hit ? "hit" : "miss"));
    addExtra(dynamic, extraTags);
    Tag[] tags = metricTags(dynamic);
    observability.timer(Telemetry.Metrics.CACHE_LATENCY, duration, tags);
  }

  public void recordLoadFailure(Duration duration, Throwable error, Tag... extraTags) {
    List<Tag> dynamic = new ArrayList<>();
    dynamic.add(Tag.of(TagKey.RESULT, "error"));
    if (error != null) {
      dynamic.add(Tag.of(TagKey.EXCEPTION, error.getClass().getSimpleName()));
    }
    addExtra(dynamic, extraTags);
    Tag[] tags = metricTags(dynamic);
    observability.timer(Telemetry.Metrics.CACHE_LATENCY, duration, tags);
    observability.counter(Telemetry.Metrics.CACHE_ERRORS, 1, tags);
  }
}
