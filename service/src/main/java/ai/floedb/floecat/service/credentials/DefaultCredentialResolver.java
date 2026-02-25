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

package ai.floedb.floecat.service.credentials;

import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;

@ApplicationScoped
public class DefaultCredentialResolver implements CredentialResolver {
  private static final String SECRET_TYPE = "connectors";

  @Inject SecretsManager secretsManager;

  @Override
  public Optional<AuthCredentials> resolve(String accountId, String credentialId) {
    if (credentialId == null || credentialId.isBlank()) {
      return Optional.empty();
    }
    return secretsManager
        .get(accountId, SECRET_TYPE, credentialId)
        .map(
            payload -> {
              try {
                return AuthCredentials.parseFrom(payload);
              } catch (Exception e) {
                throw new IllegalStateException("Failed to parse auth credentials secret", e);
              }
            });
  }

  @Override
  public void store(String accountId, String credentialId, AuthCredentials credentials) {
    if (credentials == null || credentialId == null || credentialId.isBlank()) {
      return;
    }
    byte[] payload = credentials.toByteArray();
    boolean exists = secretsManager.get(accountId, SECRET_TYPE, credentialId).isPresent();
    if (exists) {
      secretsManager.update(accountId, SECRET_TYPE, credentialId, payload);
    } else {
      secretsManager.put(accountId, SECRET_TYPE, credentialId, payload);
    }
  }

  @Override
  public void delete(String accountId, String credentialId) {
    if (credentialId == null || credentialId.isBlank()) {
      return;
    }
    secretsManager.delete(accountId, SECRET_TYPE, credentialId);
  }
}
