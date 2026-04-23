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

package ai.floedb.floecat.service.statistics;

import ai.floedb.floecat.stats.spi.StatsCaptureBatchRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchResult;
import ai.floedb.floecat.stats.spi.StatsCaptureControlPlane;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Service-layer adapter exposing {@link StatsOrchestrator} through reconciler's control-plane SPI.
 */
@ApplicationScoped
public class StatsCaptureControlPlaneAdapter implements StatsCaptureControlPlane {

  private final StatsOrchestrator orchestrator;

  @Inject
  public StatsCaptureControlPlaneAdapter(StatsOrchestrator orchestrator) {
    this.orchestrator = orchestrator;
  }

  @Override
  public StatsCaptureBatchResult triggerBatch(StatsCaptureBatchRequest batchRequest) {
    return orchestrator.triggerBatch(batchRequest);
  }
}
