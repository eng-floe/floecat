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

package ai.floedb.floecat.service.it;

import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Alternative
@Priority(1)
@ApplicationScoped
public class TestCredentialResolver implements CredentialResolver {
  private final ConcurrentHashMap<String, AuthCredentials> secrets = new ConcurrentHashMap<>();

  @Override
  public Optional<AuthCredentials> resolve(String accountId, String credentialId) {
    if (credentialId == null || credentialId.isBlank()) {
      return Optional.empty();
    }
    if ("secret-1".equals(credentialId)) {
      return Optional.of(
          AuthCredentials.newBuilder()
              .setBearer(AuthCredentials.BearerToken.newBuilder().setToken("resolved-token"))
              .build());
    }
    return Optional.ofNullable(secrets.get(credentialId));
  }

  @Override
  public void store(String accountId, String credentialId, AuthCredentials credentials) {
    if (credentials == null || credentialId == null || credentialId.isBlank()) {
      return;
    }
    secrets.put(credentialId, credentials);
  }

  @Override
  public void delete(String accountId, String credentialId) {
    if (credentialId == null || credentialId.isBlank()) {
      return;
    }
    if (!"secret-1".equals(credentialId)) {
      secrets.remove(credentialId);
    }
  }
}
