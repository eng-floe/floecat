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

package ai.floedb.floecat.service.it.profiles;

import ai.floedb.floecat.service.it.util.TestKeyPair;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class OidcSessionHeaderValidateAccountProfile implements QuarkusTestProfile {
  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.of(
        "floecat.auth.mode", "oidc",
        "quarkus.oidc.enabled", "true",
        "quarkus.oidc.discovery-enabled", "false",
        "quarkus.oidc.tenant-enabled", "true",
        "quarkus.oidc.token.audience", "floecat-client",
        "floecat.interceptor.validate.account", "true",
        "floecat.interceptor.session.header", "x-floe-session",
        "quarkus.oidc.public-key", TestKeyPair.publicKeyBase64());
  }

  @Override
  public String getConfigProfile() {
    return "oidc-session-header-validate-account";
  }
}
