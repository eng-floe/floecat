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

import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Helper that emits GC-related metrics with canonical tags. */
public final class GcMetrics extends BaseMetrics {
  private final Observability observability;

  public GcMetrics(Observability observability, String component, String operation, String gcName) {
    super(component, operation, Tag.of(TagKey.GC_NAME, gcName));
    this.observability = observability;
  }

  public void recordCollection(double count, Tag... extraTags) {
    List<Tag> dynamic = new ArrayList<>();
    dynamic.add(Tag.of(TagKey.RESULT, "success"));
    addExtra(dynamic, extraTags);
    observability.counter(Telemetry.Metrics.GC_COLLECTIONS, count, metricTags(dynamic));
  }

  public void recordPause(Duration duration, Tag... extraTags) {
    List<Tag> dynamic = new ArrayList<>();
    dynamic.add(Tag.of(TagKey.RESULT, "success"));
    addExtra(dynamic, extraTags);
    observability.timer(Telemetry.Metrics.GC_PAUSE, duration, metricTags(dynamic));
  }
}
