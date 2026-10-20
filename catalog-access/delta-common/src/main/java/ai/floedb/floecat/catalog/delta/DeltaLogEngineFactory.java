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

package ai.floedb.floecat.catalog.delta;

import ai.floedb.floecat.aws.RefreshingAwsClient;
import ai.floedb.floecat.catalog.access.VendedStorageCredentials;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

/** Opens a read-only Delta Kernel engine from catalog-vended S3 credentials. */
public final class DeltaLogEngineFactory {
  private DeltaLogEngineFactory() {}

  public static OpenedEngine open(VendedStorageCredentials credentials) {
    Objects.requireNonNull(credentials, "credentials");
    Map<String, String> properties = credentials.properties();
    String accessKey = required(properties, "s3.access-key-id");
    String secretKey = required(properties, "s3.secret-access-key");
    String sessionToken = nonBlank(properties.get("s3.session-token"));
    AwsCredentials awsCredentials =
        sessionToken == null
            ? AwsBasicCredentials.create(accessKey, secretKey)
            : AwsSessionCredentials.create(accessKey, secretKey, sessionToken);
    var provider = StaticCredentialsProvider.create(awsCredentials);
    var clients =
        RefreshingAwsClient.withResourceFactory(
            "Delta overlay S3",
            () -> {
              var builder =
                  S3Client.builder()
                      .credentialsProvider(provider)
                      .region(Region.of(properties.getOrDefault("s3.region", "us-east-1")))
                      .serviceConfiguration(
                          S3Configuration.builder()
                              .pathStyleAccessEnabled(
                                  Boolean.parseBoolean(properties.get("s3.path-style-access")))
                              .build());
              String endpoint = nonBlank(properties.get("s3.endpoint"));
              if (endpoint != null) {
                builder.endpointOverride(URI.create(endpoint));
              }
              return RefreshingAwsClient.clientResource(builder.build());
            });
    return new OpenedEngine(DefaultEngine.create(new S3V2FileSystemClient(clients)), clients);
  }

  private static String required(Map<String, String> properties, String key) {
    String value = nonBlank(properties.get(key));
    if (value == null) {
      throw new IllegalArgumentException("Vended Delta storage credentials omitted " + key);
    }
    return value;
  }

  private static String nonBlank(String value) {
    return value == null || value.isBlank() ? null : value;
  }

  public record OpenedEngine(Engine engine, AutoCloseable resources) implements AutoCloseable {
    public OpenedEngine {
      Objects.requireNonNull(engine, "engine");
      Objects.requireNonNull(resources, "resources");
    }

    @Override
    public void close() throws Exception {
      resources.close();
    }
  }
}
