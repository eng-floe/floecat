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

package ai.floedb.floecat.service.connector.impl;

import ai.floedb.floecat.connector.common.auth.CredentialResolverSupport;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.spi.AuthResolutionContext;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorConfig.Kind;
import ai.floedb.floecat.service.credentials.AuthResolutionContexts;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import java.util.Map;

/** Shared helpers for building {@link ConnectorConfig} instances from connector RPC messages. */
final class ConnectorConfigSupport {
  private ConnectorConfigSupport() {}

  static Kind resolveKind(ConnectorKind kind, String corr) {
    return switch (kind) {
      case CK_ICEBERG -> Kind.ICEBERG;
      case CK_DELTA -> Kind.DELTA;
      case CK_GLUE -> Kind.GLUE;
      case CK_UNITY -> Kind.UNITY;
      default -> throw GrpcErrors.invalidArgument(corr, null, Map.of("field", "kind"));
    };
  }

  static ConnectorConfig.Auth toConnectorAuth(AuthConfig auth) {
    return new ConnectorConfig.Auth(
        auth.getScheme(), auth.getPropertiesMap(), auth.getHeaderHintsMap());
  }

  static boolean hasAuthCredentials(AuthConfig auth) {
    if (auth == null || !auth.hasCredentials()) {
      return false;
    }
    return auth.getCredentials().getCredentialCase()
        != AuthCredentials.CredentialCase.CREDENTIAL_NOT_SET;
  }

  static ConnectorConfig resolveCredentials(ConnectorConfig base, AuthConfig auth) {
    if (hasAuthCredentials(auth)) {
      AuthResolutionContext context = AuthResolutionContexts.fromInboundContext();
      return CredentialResolverSupport.apply(base, auth.getCredentials(), context);
    }
    return base;
  }
}
