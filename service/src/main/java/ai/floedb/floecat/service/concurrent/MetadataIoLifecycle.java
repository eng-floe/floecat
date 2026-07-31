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
package ai.floedb.floecat.service.concurrent;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

/**
 * Owns the one process-lifecycle hook the metadata-I/O tier needs: closing the shared admission
 * runtime at application shutdown.
 *
 * <p>Deliberately a separate bean rather than an observer on {@link MetadataIoRunner}. The shared
 * runtime can be started with no CDI instance of that bean in existence — {@link
 * MetadataIoRunner#shared()} and its default constructor are supported outside CDI — so an {@code
 * IF_EXISTS} observer there would silently never fire and leak the platform-worker pool (and the
 * context class loader its threads pin) across every dev-mode reload and {@code @QuarkusTest}
 * restart. This bean is always instantiated to receive the event, and costs nothing when the
 * runtime was never started: {@link MetadataIoRunner#closeSharedRuntimeIfStarted()} finds no
 * installed runtime and closes nothing, so an unused runner is not started just to be closed.
 */
@ApplicationScoped
public class MetadataIoLifecycle {

  /**
   * Reject a malformed concurrency value before the service accepts traffic, so a typo fails the
   * deployment instead of silently running on the default ceiling.
   */
  // Ordered ahead of every other StartupEvent observer: the previous lifecycle's shutdown left a
  // sentinel installed, and any observer that constructs a MetadataIoRunner before it is cleared
  // gets RejectedExecutionException. MetadataIoMetrics is one such observer.
  void validateMetadataIoConfig(@Observes @Priority(0) StartupEvent event) {
    // Re-arm first: the previous lifecycle's shutdown left a sentinel installed so no pool could be
    // built after its ShutdownEvent. Without this the restarted application refuses every call.
    MetadataIoRunner.reopenSharedRuntime();
    MetadataIoRunner.validateConfiguredCapacity();
  }

  /**
   * Closes the shared runtime at shutdown.
   *
   * <p>Ordering caveat, unresolved on purpose: {@code ShutdownEvent} fires at the start of
   * shutdown, and whether the HTTP/gRPC layer has finished draining in-flight calls by then is a
   * Quarkus-version detail. If it has not, a request still doing store I/O sees a rejected
   * admission rather than completing. Nothing routes store I/O through this tier yet, so the
   * question cannot be settled by test here; settle it in the change that wires the first caller,
   * against a live request, rather than by guessing at container ordering.
   */
  void closeSharedMetadataIoRuntime(@Observes ShutdownEvent event) {
    // Drop the telemetry sink with the runtime: it closes over this container's beans, and the
    // static that holds it outlives a dev-mode reload.
    MetadataIoRunner.clearSaturationSink();
    MetadataIoRunner.closeSharedRuntimeIfStarted();
  }
}
