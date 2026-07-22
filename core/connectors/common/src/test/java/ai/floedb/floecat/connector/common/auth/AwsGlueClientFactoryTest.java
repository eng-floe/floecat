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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

class AwsGlueClientFactoryTest {

  @Test
  void resolveRegionPrefersClientRegion() {
    String region =
        AwsGlueClientFactory.resolveRegion(
            Map.of("client.region", "us-west-2", "s3.region", "us-east-1"), "us-east-1");
    assertEquals("us-west-2", region);
  }

  @Test
  void resolveRegionFallsBackToS3Region() {
    String region =
        AwsGlueClientFactory.resolveRegion(Map.of("s3.region", "eu-west-1"), "us-east-1");
    assertEquals("eu-west-1", region);
  }

  @Test
  void resolveRegionFallsBackToAwsRegion() {
    String region =
        AwsGlueClientFactory.resolveRegion(Map.of("aws.region", "ap-southeast-1"), "us-east-1");
    assertEquals("ap-southeast-1", region);
  }

  @Test
  void resolveRegionUsesDefaultWhenMissing() {
    String region = AwsGlueClientFactory.resolveRegion(Map.of(), "us-east-1");
    assertEquals("us-east-1", region);
  }

  @Test
  void credentialsProviderFactoryBuildsFreshProviderForEachClientRefresh() {
    var factory = AwsGlueClientFactory.credentialsProviderFactory(Map.of(), Map.of());

    AwsCredentialsProvider first = factory.get();
    AwsCredentialsProvider second = factory.get();

    assertNotSame(first, second);
  }

  @Test
  void credentialsProviderFactoryDoesNotUseStorageRegistryProviderForGlue() {
    var factory =
        AwsGlueClientFactory.credentialsProviderFactory(
            Map.of(RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID, "provider-1"),
            Map.of());

    AwsCredentialsProvider first = factory.get();

    assertInstanceOf(DefaultCredentialsProvider.class, first);
  }

  @Test
  void credentialsProviderFactoryUsesCatalogRegistryProviderForGlue() {
    var factory =
        AwsGlueClientFactory.credentialsProviderFactory(
            Map.of(
                RefreshingAwsCredentialsProviderRegistry.CATALOG_OPTION_PROVIDER_ID, "provider-1"),
            Map.of());

    AwsCredentialsProvider provider = factory.get();

    assertInstanceOf(RegistryBackedAwsCredentialsProvider.class, provider);
  }

  @Test
  void credentialsProviderFactoryUsesCatalogStaticCredentialsForGlue() {
    var factory =
        AwsGlueClientFactory.credentialsProviderFactory(
            Map.of(
                "rest.access-key-id", "catalog-access",
                "rest.secret-access-key", "catalog-secret",
                "rest.session-token", "catalog-session",
                "s3.access-key-id", "storage-access",
                "s3.secret-access-key", "storage-secret"),
            Map.of());

    AwsCredentialsProvider provider = factory.get();

    assertInstanceOf(StaticCredentialsProvider.class, provider);
    assertEquals("catalog-access", provider.resolveCredentials().accessKeyId());
  }

  @Test
  void catalogRegistryProviderRenewsExpiredCredentials() {
    AtomicInteger refreshes = new AtomicInteger();
    String providerId =
        RefreshingAwsCredentialsProviderRegistry.register(
            new ResolvedStorageCredentials(
                "expired-access",
                "expired-secret",
                "expired-session",
                Instant.now().minusSeconds(1)),
            () -> {
              refreshes.incrementAndGet();
              return new ResolvedStorageCredentials(
                  "renewed-access",
                  "renewed-secret",
                  "renewed-session",
                  Instant.now().plusSeconds(900));
            });
    try {
      AwsCredentialsProvider provider =
          AwsGlueClientFactory.credentialsProviderFactory(
                  Map.of(
                      RefreshingAwsCredentialsProviderRegistry.CATALOG_OPTION_PROVIDER_ID,
                      providerId),
                  Map.of())
              .get();

      assertEquals("renewed-access", provider.resolveCredentials().accessKeyId());
      assertEquals(1, refreshes.get());
    } finally {
      RefreshingAwsCredentialsProviderRegistry.unregister(providerId);
    }
  }
}
