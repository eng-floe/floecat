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

package ai.floedb.floecat.gateway.iceberg.rest.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.Map;
import java.util.Optional;

@ConfigMapping(prefix = "floecat.connector.integration")
public interface ConnectorIntegrationConfig {
  @WithDefault("true")
  boolean enabled();

  Optional<String> metadataFileIo();

  Optional<String> metadataFileIoRoot();

  Optional<String> defaultRegion();

  Map<String, String> storageCredentialProperties();

  Map<String, RegisterConnectorTemplate> registerConnectors();

  interface RegisterConnectorTemplate {
    Optional<String> displayName();

    Optional<String> description();

    Map<String, String> properties();

    @WithDefault("true")
    boolean captureStatistics();
  }
}
