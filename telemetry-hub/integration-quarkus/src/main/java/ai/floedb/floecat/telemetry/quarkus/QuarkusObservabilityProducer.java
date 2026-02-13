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

package ai.floedb.floecat.telemetry.quarkus;

import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.TelemetryPolicy;
import ai.floedb.floecat.telemetry.TelemetryRegistry;
import ai.floedb.floecat.telemetry.micrometer.MicrometerObservability;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** Quarkus CDI producer that exposes the telemetry hub components to the application. */
@ApplicationScoped
public class QuarkusObservabilityProducer {

  @Inject MeterRegistry meterRegistry;

  @ConfigProperty(name = "telemetry.strict", defaultValue = "false")
  boolean strict;

  @ConfigProperty(name = "telemetry.contract.version", defaultValue = "v1")
  String contractVersion;

  private final TelemetryRegistry telemetryRegistry = Telemetry.newRegistryWithCore();

  @Produces
  @ApplicationScoped
  public Observability observability() {
    TelemetryPolicy policy = strict ? TelemetryPolicy.STRICT : TelemetryPolicy.LENIENT;
    meterRegistry.config().commonTags("telemetry.contract.version", contractVersion);
    return new MicrometerObservability(meterRegistry, telemetryRegistry, policy);
  }

  @Produces
  @ApplicationScoped
  public TelemetryRegistry telemetryRegistry() {
    return telemetryRegistry;
  }
}
