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

package ai.floedb.floecat.gateway.iceberg.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

@ConfigMapping(prefix = "floecat.gateway")
public interface IcebergGatewayConfig {
  @WithDefault("oidc")
  String authMode();

  @WithDefault("localhost:9000")
  String upstreamTarget();

  @WithDefault("true")
  boolean upstreamPlaintext();

  @WithDefault("authorization")
  String authHeader();

  Optional<String> defaultAccountId();

  @WithDefault("account_id")
  String accountClaim();

  Optional<String> defaultAuthorization();

  Map<String, String> catalogMapping();

  @WithDefault("false")
  boolean devAllowMissingAuth();

  Optional<String> defaultPrefix();

  Optional<String> defaultWarehousePath();

  Optional<String> defaultRegion();

  Optional<StorageCredentialConfig> storageCredential();

  Optional<String> metadataFileIo();

  Optional<String> metadataFileIoRoot();

  interface StorageCredentialConfig {
    Optional<String> scope();

    Map<String, String> properties();
  }

  Map<String, RegisterConnectorTemplate> registerConnectors();

  @WithDefault("true")
  boolean connectorIntegrationEnabled();

  @WithDefault("PT10M")
  Duration planTaskTtl();

  @WithDefault("128")
  int planTaskFilesPerTask();

  @WithDefault("PT30M")
  Duration idempotencyKeyLifetime();

  interface RegisterConnectorTemplate {
    String uri();

    Optional<String> displayName();

    Optional<String> description();

    Map<String, String> properties();

    Optional<AuthTemplate> auth();

    @WithDefault("true")
    boolean captureStatistics();
  }

  interface AuthTemplate {
    @WithDefault("none")
    String scheme();

    Map<String, String> properties();

    Map<String, String> headerHints();

    Optional<String> secretRef();
  }
}
