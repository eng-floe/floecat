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

package ai.floedb.floecat.connector.iceberg.impl;

import ai.floedb.floecat.connector.common.auth.CredentialResolverSupport;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.spi.AuthResolutionContext;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorProvider;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import java.util.HashMap;
import java.util.Map;

public final class IcebergConnectorProvider implements ConnectorProvider {
  @Override
  public String kind() {
    return "iceberg";
  }

  @Override
  public FloecatConnector create(ConnectorConfig cfg) {
    ConnectorConfig resolved = resolveCredentials(cfg);
    Map<String, String> options = new HashMap<>(resolved.options());
    var auth = resolved.auth();

    return IcebergConnectorFactory.create(
        resolved.uri(),
        options,
        auth.scheme(),
        new HashMap<>(auth.props()),
        new HashMap<>(auth.headerHints()));
  }

  static ConnectorConfig resolveCredentials(ConnectorConfig cfg) {
    if (cfg == null) {
      return cfg;
    }
    AuthCredentials credentials = cfg.auth() != null ? cfg.auth().credentials() : null;
    if (credentials == null
        || credentials.getCredentialCase() == AuthCredentials.CredentialCase.CREDENTIAL_NOT_SET) {
      return cfg;
    }
    return CredentialResolverSupport.apply(cfg, credentials, AuthResolutionContext.empty());
  }
}
