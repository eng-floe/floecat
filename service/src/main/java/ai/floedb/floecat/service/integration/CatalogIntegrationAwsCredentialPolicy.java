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

package ai.floedb.floecat.service.integration;

import ai.floedb.floecat.integration.rpc.AwsSigV4Authentication;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** Deployment-owned gate for ambient AWS credentials used by Catalog Integrations. */
@ApplicationScoped
class CatalogIntegrationAwsCredentialPolicy {
  @ConfigProperty(
      name = "floecat.catalog-integrations.aws.default-credentials-enabled",
      defaultValue = "false")
  boolean defaultCredentialsEnabled;

  void requireAllowed(AwsSigV4Authentication authentication) {
    switch (authentication.getCredentialsCase()) {
      case AWS_DEFAULT -> {
        if (!defaultCredentialsEnabled) {
          throw new IllegalArgumentException(
              "AWS default credentials are disabled for Catalog Integrations");
        }
      }
      case AWS_ASSUME_ROLE, AWS_ACCESS_KEY, CREDENTIALS_NOT_SET -> {}
    }
  }
}
