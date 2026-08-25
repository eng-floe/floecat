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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.integration.rpc.BearerAuthentication;
import ai.floedb.floecat.integration.rpc.CatalogAuthentication;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationCredentials;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationSpec;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationType;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationsGrpc;
import ai.floedb.floecat.integration.rpc.CreateCatalogIntegrationRequest;
import ai.floedb.floecat.integration.rpc.DeleteCatalogIntegrationRequest;
import ai.floedb.floecat.integration.rpc.GetCatalogIntegrationRequest;
import ai.floedb.floecat.integration.rpc.ListCatalogIntegrationsRequest;
import ai.floedb.floecat.integration.rpc.OAuthClientCredentialsAuthentication;
import ai.floedb.floecat.integration.rpc.SecretValue;
import ai.floedb.floecat.integration.rpc.UpdateCatalogIntegrationAuthenticationRequest;
import ai.floedb.floecat.integration.rpc.UpdateCatalogIntegrationRequest;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.integration.CatalogIntegrationCredentialStore;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class CatalogIntegrationIT {
  @GrpcClient("floecat")
  CatalogIntegrationsGrpc.CatalogIntegrationsBlockingStub integrations;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;
  @Inject CatalogIntegrationCredentialStore credentialStore;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void catalogIntegrationLifecycle() throws Exception {
    var created =
        integrations.createCatalogIntegration(
            CreateCatalogIntegrationRequest.newBuilder()
                .setSpec(
                    CatalogIntegrationSpec.newBuilder()
                        .setDisplayName("warehouse")
                        .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
                        .setCatalogUri("https://catalog.example/v1")
                        .putProperties("warehouse", "analytics")
                        .setAuthentication(oauthAuthentication("catalog-client")))
                .setCredentials(oauthCredentials("initial-secret"))
                .build());

    CatalogIntegration integration = created.getIntegration();
    var integrationId = integration.getResourceId();
    assertEquals(ResourceKind.RK_CATALOG_INTEGRATION, integrationId.getKind());
    assertTrue(integration.getAuthentication().getCredentialsConfigured());
    assertEquals(1L, integration.getAuthentication().getCredentialGeneration());
    assertEquals(
        "initial-secret",
        credentialStore.resolve(integration).orElseThrow().getOauthClientSecret().getValue());

    var fetchedById =
        integrations
            .getCatalogIntegration(
                GetCatalogIntegrationRequest.newBuilder().setIntegrationId(integrationId).build())
            .getIntegration();
    var fetchedByName =
        integrations
            .getCatalogIntegration(
                GetCatalogIntegrationRequest.newBuilder().setDisplayName("warehouse").build())
            .getIntegration();
    assertEquals(integration, fetchedById);
    assertEquals(integrationId, fetchedByName.getResourceId());

    var listed =
        integrations.listCatalogIntegrations(ListCatalogIntegrationsRequest.newBuilder().build());
    assertEquals(1, listed.getEntriesCount());
    assertEquals(integrationId, listed.getEntries(0).getIntegration().getResourceId());

    var updated =
        integrations.updateCatalogIntegration(
            UpdateCatalogIntegrationRequest.newBuilder()
                .setIntegrationId(integrationId)
                .setSpec(
                    CatalogIntegrationSpec.newBuilder()
                        .setDisplayName("warehouse-renamed")
                        .setCatalogUri("https://catalog.example/v2")
                        .putProperties("warehouse", "finance"))
                .setUpdateMask(
                    FieldMask.newBuilder()
                        .addPaths("display_name")
                        .addPaths("catalog_uri")
                        .addPaths("properties"))
                .setPrecondition(precondition(created.getMeta()))
                .build());
    assertEquals("warehouse-renamed", updated.getIntegration().getDisplayName());
    assertEquals("https://catalog.example/v2", updated.getIntegration().getCatalogUri());
    assertEquals("finance", updated.getIntegration().getPropertiesMap().get("warehouse"));
    assertEquals(integrationId, updated.getIntegration().getResourceId());

    var rotated =
        integrations.updateCatalogIntegrationAuthentication(
            UpdateCatalogIntegrationAuthenticationRequest.newBuilder()
                .setIntegrationId(integrationId)
                .setAuthentication(oauthAuthentication("catalog-client-v2"))
                .setCredentials(oauthCredentials("rotated-secret"))
                .setPrecondition(precondition(updated.getMeta()))
                .build());
    assertEquals(2L, rotated.getIntegration().getAuthentication().getCredentialGeneration());
    assertEquals(
        "rotated-secret",
        credentialStore
            .resolve(rotated.getIntegration())
            .orElseThrow()
            .getOauthClientSecret()
            .getValue());

    integrations.deleteCatalogIntegration(
        DeleteCatalogIntegrationRequest.newBuilder()
            .setIntegrationId(integrationId)
            .setPrecondition(precondition(rotated.getMeta()))
            .build());

    var missing =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                integrations.getCatalogIntegration(
                    GetCatalogIntegrationRequest.newBuilder()
                        .setIntegrationId(integrationId)
                        .build()));
    TestSupport.assertGrpcAndMc(
        missing, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "Catalog integration not found");
    assertFalse(credentialStore.resolve(rotated.getIntegration()).isPresent());
    assertEquals(
        0,
        integrations
            .listCatalogIntegrations(ListCatalogIntegrationsRequest.newBuilder().build())
            .getEntriesCount());
  }

  @Test
  void createIsIdempotentThroughGrpc() {
    var request =
        CreateCatalogIntegrationRequest.newBuilder()
            .setSpec(
                CatalogIntegrationSpec.newBuilder()
                    .setDisplayName("idempotent-warehouse")
                    .setType(CatalogIntegrationType.CIT_UNITY)
                    .setCatalogUri("https://unity.example/api/2.1/unity-catalog")
                    .setAuthentication(
                        CatalogAuthentication.newBuilder()
                            .setBearer(BearerAuthentication.getDefaultInstance())))
            .setCredentials(
                CatalogIntegrationCredentials.newBuilder()
                    .setBearerToken(SecretValue.newBuilder().setValue("token")))
            .setIdempotency(IdempotencyKey.newBuilder().setKey("catalog-integration-it"))
            .build();

    var first = integrations.createCatalogIntegration(request);
    var retry = integrations.createCatalogIntegration(request);

    assertEquals(first.getIntegration(), retry.getIntegration());
    assertEquals(first.getMeta(), retry.getMeta());
    assertEquals(
        1,
        integrations
            .listCatalogIntegrations(ListCatalogIntegrationsRequest.newBuilder().build())
            .getEntriesCount());
  }

  private static CatalogAuthentication oauthAuthentication(String clientId) {
    return CatalogAuthentication.newBuilder()
        .setOauthClientCredentials(
            OAuthClientCredentialsAuthentication.newBuilder().setClientId(clientId))
        .build();
  }

  private static CatalogIntegrationCredentials oauthCredentials(String secret) {
    return CatalogIntegrationCredentials.newBuilder()
        .setOauthClientSecret(SecretValue.newBuilder().setValue(secret))
        .build();
  }

  private static Precondition precondition(ai.floedb.floecat.common.rpc.MutationMeta meta) {
    return Precondition.newBuilder()
        .setExpectedVersion(meta.getPointerVersion())
        .setExpectedEtag(meta.getEtag())
        .build();
  }
}
