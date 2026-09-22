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

package ai.floedb.floecat.service.cache;

import ai.floedb.floecat.service.repo.cache.PlanningPointerIndex;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import java.time.Duration;
import java.util.Arrays;
import org.jboss.logging.Logger;

/**
 * Warm observations for the planner pointer index.
 *
 * <p>Account IDs stay out of the metric tags and go to the log instead: one time series per account
 * is the shape that makes a per-tenant metric unusable, and an operator chasing one account needs
 * the ID rather than a series.
 */
final class PlanningPointerWarmTelemetry implements PlanningPointerIndex.WarmObserver {
  private static final Logger LOG = Logger.getLogger(PlanningPointerWarmTelemetry.class);

  private final Observability observability;
  private final Tag[] baseTags;
  private final long maxBytesPerAccount;

  PlanningPointerWarmTelemetry(
      Observability observability, Tag[] baseTags, long maxBytesPerAccount) {
    this.observability = observability;
    this.baseTags = baseTags;
    this.maxBytesPerAccount = maxBytesPerAccount;
  }

  @Override
  public void started(String accountId) {
    observability.counter(ServiceMetrics.PlanningPointer.WARM_STARTS, 1, baseTags);
  }

  @Override
  public void completed(String accountId, Duration duration) {
    observability.timer(
        ServiceMetrics.PlanningPointer.WARM_LATENCY,
        duration,
        append(Tag.of(TagKey.RESULT, "success")));
  }

  @Override
  public void failed(String accountId, Duration duration, Throwable failure) {
    Tag[] tags =
        append(
            Tag.of(TagKey.RESULT, "error"),
            Tag.of(TagKey.EXCEPTION, failure.getClass().getSimpleName()));
    observability.timer(ServiceMetrics.PlanningPointer.WARM_LATENCY, duration, tags);
    observability.counter(ServiceMetrics.PlanningPointer.WARM_ERRORS, 1, tags);
    LOG.warnf(
        failure, "planner_pointer_warm_failed account_id=%s duration=%s", accountId, duration);
  }

  @Override
  public void refused(String accountId, Duration duration) {
    observability.timer(
        ServiceMetrics.PlanningPointer.WARM_LATENCY,
        duration,
        append(Tag.of(TagKey.RESULT, "refused")));
    LOG.warnf(
        "planner_pointer_warm_refused_for_size account_id=%s duration=%s max_bytes_per_account=%d",
        accountId, duration, maxBytesPerAccount);
  }

  private Tag[] append(Tag... extra) {
    Tag[] merged = Arrays.copyOf(baseTags, baseTags.length + extra.length);
    System.arraycopy(extra, 0, merged, baseTags.length, extra.length);
    return merged;
  }
}
