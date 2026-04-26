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

import ai.floedb.floecat.connector.common.auth.CredentialResolverSupport;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.spi.AuthResolutionContext;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorConfigMapper;
import ai.floedb.floecat.connector.spi.ConnectorFactory;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngine;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineCapabilities;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineRequest;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.EnumSet;
import java.util.Optional;

/** Default OSS Java capture engine backed by the current connector SPI. */
@ApplicationScoped
public class JavaConnectorCaptureEngine implements CaptureEngine {
  @FunctionalInterface
  interface ConnectorOpener {
    FloecatConnector open(ConnectorConfig connector);
  }

  final JavaConnectorFileGroupCaptureAdapter adapter = new JavaConnectorFileGroupCaptureAdapter();
  @Inject CredentialResolver credentialResolver;

  ConnectorOpener connectorOpener = ConnectorFactory::create;

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
    try (var source = connectorOpener.open(resolveCredentials(request.sourceConnector()))) {
      return Optional.of(adapter.capture(source, request));
    } catch (RuntimeException e) {
      if (isMissingObjectFailure(e)) {
        throw new ReconcileFailureException(
            ai.floedb.floecat.reconciler.spi.ReconcileExecutor.ExecutionResult.FailureKind
                .TABLE_MISSING,
            "source object missing",
            e);
      }
      throw e;
    }
  }

  private ConnectorConfig resolveCredentials(Connector connector) {
    ConnectorConfig base = ConnectorConfigMapper.fromProto(connector);
    AuthConfig auth = connector == null ? AuthConfig.getDefaultInstance() : connector.getAuth();
    if (auth.hasCredentials()
        && auth.getCredentials().getCredentialCase()
            != AuthCredentials.CredentialCase.CREDENTIAL_NOT_SET) {
      return CredentialResolverSupport.apply(base, auth.getCredentials());
    }
    if (connector == null
        || !connector.hasResourceId()
        || auth.getScheme().isBlank()
        || "none".equalsIgnoreCase(auth.getScheme())) {
      return base;
    }
    return credentialResolver
        .resolve(connector.getResourceId().getAccountId(), connector.getResourceId().getId())
        .map(c -> CredentialResolverSupport.apply(base, c, AuthResolutionContext.empty()))
        .orElse(base);
  }

  private static boolean isMissingObjectFailure(Throwable t) {
    if (t == null) {
      return false;
    }
    String className = t.getClass().getName();
    if (className.endsWith("NoSuchTableException")
        || className.endsWith("NoSuchViewException")
        || className.endsWith("NoSuchObjectException")
        || className.endsWith("NotFoundException")) {
      return true;
    }
    String message = t.getMessage();
    if (message == null || message.isBlank()) {
      return false;
    }
    String normalized = message.toLowerCase();
    return normalized.contains("http 404")
        || normalized.contains("status 404")
        || normalized.contains("not found")
        || normalized.contains("does not exist");
  }
}
