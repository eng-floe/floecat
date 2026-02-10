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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class DefaultCredentialResolverTest {

  @Test
  void uses_connector_secret_type() {
    RecordingSecretsManager secrets = new RecordingSecretsManager();
    DefaultCredentialResolver resolver = new DefaultCredentialResolver();
    resolver.secretsManager = secrets;

    resolver.resolve("acct", "conn-1");
    assertEquals("connectors", secrets.lastSecretType);

    resolver.store("acct", "conn-2", AuthCredentials.getDefaultInstance());
    assertEquals("connectors", secrets.lastSecretType);

    resolver.delete("acct", "conn-3");
    assertEquals("connectors", secrets.lastSecretType);
  }

  @Test
  void stores_payload_with_put_when_absent() {
    RecordingSecretsManager secrets = new RecordingSecretsManager();
    DefaultCredentialResolver resolver = new DefaultCredentialResolver();
    resolver.secretsManager = secrets;

    AuthCredentials creds =
        AuthCredentials.newBuilder()
            .setBearer(AuthCredentials.BearerToken.newBuilder().setToken("t"))
            .build();
    resolver.store("acct", "conn-1", creds);

    assertTrue(secrets.putCalled);
    assertEquals("conn-1", secrets.lastSecretId);
  }

  private static final class RecordingSecretsManager implements SecretsManager {
    boolean putCalled;
    String lastSecretType;
    String lastSecretId;

    @Override
    public void put(String accountId, String secretType, String secretId, byte[] payload) {
      putCalled = true;
      lastSecretType = secretType;
      lastSecretId = secretId;
    }

    @Override
    public Optional<byte[]> get(String accountId, String secretType, String secretId) {
      lastSecretType = secretType;
      lastSecretId = secretId;
      return Optional.empty();
    }

    @Override
    public void update(String accountId, String secretType, String secretId, byte[] payload) {
      lastSecretType = secretType;
      lastSecretId = secretId;
    }

    @Override
    public void delete(String accountId, String secretType, String secretId) {
      lastSecretType = secretType;
      lastSecretId = secretId;
    }
  }
}
