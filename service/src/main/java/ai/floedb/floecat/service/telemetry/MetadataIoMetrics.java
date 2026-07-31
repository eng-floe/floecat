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

import ai.floedb.floecat.service.concurrent.MetadataIoRunner;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

/**
 * Publishes the process-wide metadata-I/O admission ceiling as metrics.
 *
 * <p>Staging note: no caller routes store I/O through the admission tier yet, so these read zero
 * against a configured ceiling until the wiring lands. Kept out of the metric descriptions, which
 * are a versioned contract.
 *
 * <p>A permit is held until the downstream call exits, so a stalled store keeps the ceiling full
 * after its callers have given up. {@code in_use} against {@code capacity} shows whether the
 * ceiling is the constraint, {@code waiters} the queue behind it, and {@code saturated_waits} how
 * often calls arrive to find it full.
 */
@ApplicationScoped
public class MetadataIoMetrics {

  @Inject Observability observability;
  @Inject MetadataIoRunner admission;

  /**
   * Observes {@link StartupEvent} so Arc retains this bean and instantiates it eagerly; nothing
   * injects it, and an unobserved {@code @PostConstruct} bean is a removal candidate.
   */
  // After MetadataIoLifecycle's observer, which clears the previous lifecycle's shutdown sentinel.
  // Injecting the runner here would otherwise be able to construct it while that sentinel is still
  // installed and fail startup.
  void registerAdmissionGauges(@Observes @Priority(100) StartupEvent startup) {
    Tag component = Tag.of(TagKey.COMPONENT, "service");
    Tag operation = Tag.of(TagKey.OPERATION, "metadata_io");

    observability.gauge(
        ServiceMetrics.MetadataIo.PERMITS_CAPACITY,
        admission::capacity,
        ServiceMetrics.MetadataIo.CAPACITY_DESC,
        component,
        operation);
    observability.gauge(
        ServiceMetrics.MetadataIo.PERMITS_IN_USE,
        admission::permitsInUse,
        ServiceMetrics.MetadataIo.IN_USE_DESC,
        component,
        operation);
    observability.gauge(
        ServiceMetrics.MetadataIo.ADMISSION_WAITERS,
        admission::admissionWaiters,
        ServiceMetrics.MetadataIo.WAITERS_DESC,
        component,
        operation);
    // A counter, incremented where the wait happens, so rate() and reset detection behave. The
    // sink runs on the waiting thread and only forwards to Observability.
    MetadataIoRunner.setSaturationSink(
        () ->
            observability.counter(
                ServiceMetrics.MetadataIo.ADMISSION_SATURATED_WAITS, 1, component, operation));
  }
}
