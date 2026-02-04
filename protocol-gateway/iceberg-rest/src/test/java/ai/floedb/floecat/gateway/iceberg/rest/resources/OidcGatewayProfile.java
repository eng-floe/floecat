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

package ai.floedb.floecat.gateway.iceberg.rest.resources;

import ai.floedb.floecat.gateway.iceberg.rest.util.TestKeyPair;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class OidcGatewayProfile implements QuarkusTestProfile {
  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.ofEntries(
        Map.entry("floecat.gateway.auth-mode", "oidc"),
        Map.entry("floecat.gateway.account-claim", "account_id"),
        Map.entry("quarkus.oidc.enabled", "true"),
        Map.entry("quarkus.oidc.tenant-enabled", "true"),
        Map.entry("quarkus.oidc.discovery-enabled", "false"),
        Map.entry("quarkus.oidc.public-key", TestKeyPair.publicKeyBase64()),
        Map.entry("quarkus.oidc.token.audience", "floecat-client"));
  }

  @Override
  public String getConfigProfile() {
    return "gateway-oidc";
  }
}
