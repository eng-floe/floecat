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

import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.Optional;

@Alternative
@Priority(1)
@ApplicationScoped
public class TestCredentialResolver implements CredentialResolver {
  @Override
  public Optional<AuthCredentials> resolve(AuthConfig auth) {
    if (auth == null) {
      return Optional.empty();
    }
    if (auth.getAuthCredentialsCase() == AuthConfig.AuthCredentialsCase.SECRET_REF
        && "secret-1".equals(auth.getSecretRef())) {
      return Optional.of(
          AuthCredentials.newBuilder()
              .setBearer(AuthCredentials.BearerToken.newBuilder().setToken("resolved-token"))
              .build());
    }
    return Optional.empty();
  }
}
