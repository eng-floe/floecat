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

package ai.floedb.floecat.service.statistics.engine.impl;

import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.stats.spi.StatsCapabilities;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsKind;
import ai.floedb.floecat.stats.spi.StatsSamplingSupport;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Set;
import org.jboss.logging.Logger;

/** Native connector-backed capture engine for Delta and Unity sources. */
@Dependent
public class DeltaNativeStatsCaptureEngine extends AbstractNativeStatsCaptureEngine {

  public static final String ENGINE_ID = "delta_native";
  private static final int PRIORITY = 1_000;

  private static final Set<String> DELTA_CONNECTOR_TYPES =
      Set.of("delta", "delta-unity", "delta-filesystem", "unity");

  private static final StatsCapabilities CAPABILITIES =
      StatsCapabilities.builder()
          .connectors(DELTA_CONNECTOR_TYPES)
          .targetTypes(Set.of(StatsTargetType.TABLE, StatsTargetType.COLUMN, StatsTargetType.FILE))
          .statisticKindsByTarget(
              Map.of(
                  StatsTargetType.TABLE,
                  Set.of(StatsKind.ROW_COUNT, StatsKind.FILE_COUNT, StatsKind.TOTAL_BYTES),
                  StatsTargetType.COLUMN,
                  Set.of(StatsKind.NULL_COUNT, StatsKind.MIN_MAX),
                  StatsTargetType.FILE,
                  Set.of(StatsKind.ROW_COUNT, StatsKind.NULL_COUNT, StatsKind.MIN_MAX)))
          .executionModes(Set.of(StatsExecutionMode.SYNC, StatsExecutionMode.ASYNC))
          .samplingSupport(Set.of(StatsSamplingSupport.NONE))
          .snapshotAware(true)
          .build();

  @Inject
  public DeltaNativeStatsCaptureEngine(
      TableRepository tableRepository,
      ConnectorRepository connectorRepository,
      CredentialResolver credentialResolver,
      StatsStore statsStore) {
    super(
        Logger.getLogger(DeltaNativeStatsCaptureEngine.class),
        tableRepository,
        connectorRepository,
        credentialResolver,
        statsStore);
  }

  @Override
  public String id() {
    return ENGINE_ID;
  }

  @Override
  public int priority() {
    return PRIORITY;
  }

  @Override
  public StatsCapabilities capabilities() {
    return CAPABILITIES;
  }

  @Override
  protected boolean supportsKind(ConnectorConfig.Kind kind) {
    return kind == ConnectorConfig.Kind.DELTA || kind == ConnectorConfig.Kind.UNITY;
  }
}
