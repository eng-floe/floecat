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

package ai.floedb.floecat.reconciler.impl;

import ai.floedb.floecat.connector.spi.ConnectorConfigMapper;
import ai.floedb.floecat.connector.spi.ConnectorFactory;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngine;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineCapabilities;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineRequest;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineResult;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.EnumSet;
import java.util.Optional;

/** Default OSS Java capture engine backed by the current connector SPI. */
@ApplicationScoped
public class JavaConnectorCaptureEngine implements CaptureEngine {
  @FunctionalInterface
  interface ConnectorOpener {
    FloecatConnector open(ai.floedb.floecat.connector.rpc.Connector connector);
  }

  final JavaConnectorFileGroupCaptureAdapter adapter = new JavaConnectorFileGroupCaptureAdapter();

  ConnectorOpener connectorOpener =
      connector -> ConnectorFactory.create(ConnectorConfigMapper.fromProto(connector));

  @Override
  public String id() {
    return "java_connector_capture";
  }

  @Override
  public int priority() {
    return 10_000;
  }

  @Override
  public CaptureEngineCapabilities capabilities() {
    return CaptureEngineCapabilities.of(
        EnumSet.of(
            FloecatConnector.StatsTargetKind.TABLE,
            FloecatConnector.StatsTargetKind.COLUMN,
            FloecatConnector.StatsTargetKind.FILE),
        true,
        false,
        true,
        CaptureEngineCapabilities.ExecutionScope.FILE_GROUP_ONLY,
        CaptureEngineCapabilities.ResultContract.COMPLETE_FILE_GROUP_OUTPUTS,
        CaptureEngineCapabilities.ExecutionRuntime.LOCAL_ONLY);
  }

  @Override
  public Optional<CaptureEngineResult> capture(CaptureEngineRequest request) {
    if (!supports(request)) {
      return Optional.empty();
    }
    try (var source = connectorOpener.open(request.sourceConnector())) {
      return Optional.of(adapter.capture(source, request));
    }
  }
}
