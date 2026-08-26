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

package ai.floedb.floecat.connector.spi;

import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.Connector;

public final class ConnectorConfigMapper {
  public static ConnectorConfig fromProto(Connector c) {
    var kind =
        switch (c.getKind()) {
          case CK_ICEBERG -> ConnectorConfig.Kind.ICEBERG;
          case CK_DELTA -> ConnectorConfig.Kind.DELTA;
          case CK_GLUE -> ConnectorConfig.Kind.GLUE;
          // Includes the reserved wire value 4 (once CK_UNITY), which arrives as UNRECOGNIZED. A
          // persisted connector carrying it never had a provider to build from, so rejecting it
          // here reports the configuration error rather than failing later inside the factory.
          default -> throw new IllegalArgumentException("unsupported kind: " + c.getKind());
        };

    var auth = toAuth(c.getAuth());

    return new ConnectorConfig(kind, c.getDisplayName(), c.getUri(), c.getPropertiesMap(), auth);
  }

  private static ConnectorConfig.Auth toAuth(AuthConfig auth) {
    return new ConnectorConfig.Auth(
        auth.getScheme(), auth.getPropertiesMap(), auth.getHeaderHintsMap());
  }
}
