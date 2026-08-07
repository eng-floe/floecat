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
 * <p>Scope: these count the annotated repository families only (catalog, namespace, table, view).
 * Reads through an unadmitted family do not appear here, so {@code in_use} below {@code capacity}
 * does not mean the process is idle on store I/O. Kept out of the metric descriptions, which are a
 * versioned contract.
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
   * Uses a {@link StartupEvent} observer as Arc's retention root and eager-instantiation trigger
   * for the gauge publisher. The observer invokes the runner before registering lazy gauges so an
   * invalid admission capacity aborts application startup.
   */
  void registerAdmissionGauges(@Observes @Priority(100) StartupEvent startup) {
    Tag component = Tag.of(TagKey.COMPONENT, "service");
    Tag operation = Tag.of(TagKey.OPERATION, "metadata_io");
    // Arc injects an application-scoped client proxy. Invoke it now so invalid admission
    // configuration fails startup instead of being deferred until the first gauge scrape.
    int capacity = admission.capacity();

    observability.gauge(
        ServiceMetrics.MetadataIo.PERMITS_CAPACITY,
        () -> capacity,
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
    // MetadataIoRunner records saturated arrivals through this same Observability instance.
  }
}
