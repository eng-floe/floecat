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

package ai.floedb.floecat.gateway.iceberg.minimal.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

@ConfigMapping(prefix = "floecat.gateway.minimal")
public interface MinimalGatewayConfig {
  @WithDefault("localhost:9100")
  String upstreamTarget();

  @WithDefault("true")
  boolean upstreamPlaintext();

  Optional<String> defaultAccountId();

  Optional<String> defaultAuthorization();

  Optional<String> defaultPrefix();

  Map<String, String> catalogMapping();

  @WithDefault("PT30M")
  Duration idempotencyKeyLifetime();

  @WithDefault("PT5M")
  Duration planTaskTtl();

  @WithDefault("128")
  int planTaskFilesPerTask();

  Optional<StorageCredentialConfig> storageCredential();

  Optional<String> metadataFileIo();

  Optional<String> metadataFileIoRoot();

  Optional<String> metadataS3Endpoint();

  @WithDefault("true")
  boolean metadataS3PathStyleAccess();

  Optional<String> metadataS3Region();

  Optional<String> metadataClientRegion();

  Optional<String> metadataS3AccessKeyId();

  Optional<String> metadataS3SecretAccessKey();

  Optional<String> defaultWarehousePath();

  Optional<DeltaCompatConfig> deltaCompat();

  @WithDefault("false")
  boolean logRequestBodies();

  @WithDefault("8192")
  int logRequestBodyMaxChars();

  interface DeltaCompatConfig {
    @WithDefault("false")
    boolean enabled();

    @WithDefault("true")
    boolean readOnly();
  }

  interface StorageCredentialConfig {
    Optional<String> scope();

    Map<String, String> properties();
  }
}
