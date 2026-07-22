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

package ai.floedb.floecat.connector.common.auth;

import ai.floedb.floecat.aws.RefreshingAwsClient;
import java.util.Map;
import java.util.function.Supplier;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;

public final class AwsGlueClientFactory {
  private AwsGlueClientFactory() {}

  public static RefreshingAwsClient<GlueClient> createRefreshing(
      Map<String, String> options, Map<String, String> authProps) {
    String region = resolveRegion(options, "us-east-1");
    Supplier<AwsCredentialsProvider> credentials = credentialsProviderFactory(options, authProps);
    return RefreshingAwsClient.withResourceFactory(
        () -> {
          AwsCredentialsProvider provider = credentials.get();
          try {
            GlueClient client =
                GlueClient.builder()
                    .region(Region.of(region))
                    .credentialsProvider(provider)
                    .build();
            return RefreshingAwsClient.clientResource(
                client, RefreshingAwsClient.closeableResource(provider));
          } catch (RuntimeException | Error e) {
            RefreshingAwsClient.closeQuietly(RefreshingAwsClient.closeableResource(provider));
            throw e;
          }
        });
  }

  public static String resolveRegion(Map<String, String> options, String defaultRegion) {
    String region = option(options, "client.region");
    if (region == null) {
      region = option(options, "s3.region");
    }
    if (region == null) {
      region = option(options, "aws.region");
    }
    return region == null ? defaultRegion : region;
  }

  private static String option(Map<String, String> options, String key) {
    if (options == null) {
      return null;
    }
    String value = options.get(key);
    if (value == null || value.isBlank()) {
      return null;
    }
    return value;
  }

  static Supplier<AwsCredentialsProvider> credentialsProviderFactory(
      Map<String, String> options, Map<String, String> authProps) {
    return () -> resolveCredentials(options, authProps);
  }

  private static AwsCredentialsProvider resolveCredentials(
      Map<String, String> options, Map<String, String> authProps) {
    String providerId =
        option(options, RefreshingAwsCredentialsProviderRegistry.CATALOG_OPTION_PROVIDER_ID);
    if (providerId != null) {
      return new RegistryBackedAwsCredentialsProvider(providerId, "catalog");
    }
    String access = option(options, "rest.access-key-id");
    String secret = option(options, "rest.secret-access-key");
    String token = option(options, "rest.session-token");
    if (access != null && secret != null) {
      AwsCredentials credentials =
          token == null
              ? AwsBasicCredentials.create(access, secret)
              : AwsSessionCredentials.create(access, secret, token);
      return StaticCredentialsProvider.create(credentials);
    }
    return AwsProfileSupport.resolveProfileProvider(authProps)
        .<AwsCredentialsProvider>map(provider -> provider)
        .orElseGet(() -> DefaultCredentialsProvider.builder().build());
  }
}
