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

package ai.floedb.floecat.catalog.iceberg.rest.auth;

import java.util.Locale;
import java.util.Map;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/** AWS SDK reflection entry point backed by {@link RefreshingAwsCredentialsRegistry}. */
public final class RegistryBackedAwsCredentialsProvider implements AwsCredentialsProvider {
  private final String providerId;
  private final AwsCredentialScope scope;

  public RegistryBackedAwsCredentialsProvider(String providerId, AwsCredentialScope scope) {
    if (providerId == null || providerId.isBlank()) {
      throw new IllegalArgumentException("providerId must be non-blank");
    }
    this.providerId = providerId.trim();
    this.scope = scope;
  }

  public static AwsCredentialsProvider create(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      throw new IllegalArgumentException("provider properties must include a provider id");
    }
    String providerId = properties.get(RefreshingAwsCredentialsRegistry.PROVIDER_ID_PROPERTY);
    String rawScope =
        properties.getOrDefault(
            RefreshingAwsCredentialsRegistry.CREDENTIAL_SCOPE_PROPERTY,
            AwsCredentialScope.STORAGE.name());
    AwsCredentialScope scope = AwsCredentialScope.valueOf(rawScope.trim().toUpperCase(Locale.ROOT));
    return new RegistryBackedAwsCredentialsProvider(providerId, scope);
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return RefreshingAwsCredentialsRegistry.resolve(providerId, scope);
  }
}
