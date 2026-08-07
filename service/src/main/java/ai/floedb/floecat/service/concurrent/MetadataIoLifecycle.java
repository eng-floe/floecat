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
 * Owns the process-lifecycle hooks the metadata-I/O tier needs: validating startup configuration
 * and dropping container-owned telemetry references at shutdown.
 *
 * <p>Deliberately a separate bean rather than an observer on {@link MetadataIoRunner}. The shared
 * runtime can be started with no CDI instance of that bean in existence — {@link
 * MetadataIoRunner#shared()} and its default constructor are supported outside CDI — so no bean
 * owns its lifetime. The runtime stays available through CDI teardown; its daemon pool releases
 * idle workers independently and JVM exit does not wait for a stuck downstream client.
 */
@ApplicationScoped
public class MetadataIoLifecycle {

  /**
   * Reject a malformed concurrency value before the service accepts traffic, so a typo fails the
   * deployment instead of silently running on the default ceiling.
   */
  // Ordered ahead of any observer that reads through a MetadataIoRunner: the previous lifecycle's
  // shutdown left the latch up, so the runtime resolves to the closed instance until this lowers
  // it. MetadataIoMetrics is one such observer. Deliberately behind
  // KvStoreProducer.BOOTSTRAP_PRIORITY, which reserves 1 to run first.
  void validateMetadataIoConfig(@Observes @Priority(2) StartupEvent event) {
    // Re-arm first: the previous lifecycle's shutdown left the latch up so no pool could be built
    // after an explicit close. Without this the restarted application never replaces the closed
    // runtime and refuses every call.
    MetadataIoRunner.reopenSharedRuntime();
    MetadataIoRunner.validateConfiguredCapacity();
  }

  /**
   * Drop the telemetry sink before this container's beans are destroyed.
   *
   * <p>The runtime deliberately remains open: neither {@code ShutdownEvent} nor any CDI destruction
   * phase is after every potential consumer, including {@code @Singleton} beans. Closing there
   * would reject their teardown metadata I/O. Daemon workers and idle-timeout pool reclamation make
   * retaining the runtime safe across in-JVM restarts while process-wide admission remains shared.
   */
  void clearSaturationSinkAtShutdown(@Observes ShutdownEvent event) {
    // The sink closes over CDI beans, while the static that holds it survives a dev-mode reload.
    MetadataIoRunner.clearSaturationSink();
  }
}
