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

package ai.floedb.floecat.service.storage.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.StorageAuthority;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StorageAuthorityResolverTest {
  private StorageAuthorityResolver resolver;

  @BeforeEach
  void setUp() {
    resolver = new StorageAuthorityResolver();
    resolver.secretsManager = new StaticSecretsManager();
  }

  @Test
  void buildResponseAllowsStaticAwsSecretsForServerSideAccess() {
    ResolveStorageAuthorityResponse response =
        resolver.buildResponse(authority(), "s3://warehouse/orders", "acct", true, true, true);

    assertEquals("us-east-1", response.getClientSafeConfigMap().get("s3.region"));
    assertEquals(1, response.getStorageCredentialsCount());
    assertEquals("s3://warehouse/orders", response.getStorageCredentials(0).getPrefix());
    assertEquals("akid", response.getStorageCredentials(0).getConfigMap().get("s3.access-key-id"));
    assertEquals(
        "secret", response.getStorageCredentials(0).getConfigMap().get("s3.secret-access-key"));
    assertFalse(response.getStorageCredentials(0).hasExpiresAt());
  }

  @Test
  void buildResponseRejectsStaticAwsSecretsForClientVending() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            resolver.buildResponse(
                authority(), "s3://warehouse/orders", "acct", true, true, false));
  }

  private static StorageAuthority authority() {
    return StorageAuthority.newBuilder()
        .setResourceId(
            ResourceId.newBuilder()
                .setAccountId("acct")
                .setKind(ResourceKind.RK_STORAGE_AUTHORITY)
                .setId("sa-1")
                .build())
        .setDisplayName("orders")
        .setEnabled(true)
        .setType("s3")
        .setLocationPrefix("s3://warehouse/orders")
        .setRegion("us-east-1")
        .setEndpoint("http://localhost:4566")
        .setPathStyleAccess(true)
        .build();
  }

  private static final class StaticSecretsManager implements SecretsManager {
    @Override
    public void put(String accountId, String secretType, String secretId, byte[] payload) {}

    @Override
    public Optional<byte[]> get(String accountId, String secretType, String secretId) {
      return Optional.of(
          AuthCredentials.newBuilder()
              .setAws(
                  AuthCredentials.AwsCredentials.newBuilder()
                      .setAccessKeyId("akid")
                      .setSecretAccessKey("secret"))
              .build()
              .toByteArray());
    }

    @Override
    public void update(String accountId, String secretType, String secretId, byte[] payload) {}

    @Override
    public void delete(String accountId, String secretType, String secretId) {}
  }
}
