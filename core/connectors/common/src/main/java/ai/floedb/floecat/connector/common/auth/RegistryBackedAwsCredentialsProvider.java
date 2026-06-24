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

import java.util.Map;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public final class RegistryBackedAwsCredentialsProvider implements AwsCredentialsProvider {
  private final String providerId;

  public RegistryBackedAwsCredentialsProvider(String providerId) {
    if (providerId == null || providerId.isBlank()) {
      throw new IllegalArgumentException("providerId must be non-blank");
    }
    this.providerId = providerId.trim();
  }

  public static AwsCredentialsProvider create(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      throw new IllegalArgumentException("provider properties must include a provider id");
    }
    String providerId =
        properties.get(RefreshingAwsCredentialsProviderRegistry.PROPERTY_PROVIDER_ID);
    if (providerId == null || providerId.isBlank()) {
      providerId = properties.get(RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID);
    }
    return new RegistryBackedAwsCredentialsProvider(providerId);
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return RefreshingAwsCredentialsProviderRegistry.resolve(providerId);
  }
}
