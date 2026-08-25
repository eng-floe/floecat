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
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetCatalogRequest;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.integration.rpc.BearerAuthentication;
import ai.floedb.floecat.integration.rpc.CatalogAuthentication;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationCredentials;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationSpec;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationType;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationsGrpc;
import ai.floedb.floecat.integration.rpc.CatalogOverlay;
import ai.floedb.floecat.integration.rpc.CatalogOverlaySpec;
import ai.floedb.floecat.integration.rpc.CatalogOverlaysGrpc;
import ai.floedb.floecat.integration.rpc.CreateCatalogIntegrationRequest;
import ai.floedb.floecat.integration.rpc.CreateCatalogOverlayRequest;
import ai.floedb.floecat.integration.rpc.DeleteCatalogOverlayRequest;
import ai.floedb.floecat.integration.rpc.GetCatalogIntegrationRequest;
import ai.floedb.floecat.integration.rpc.GetCatalogOverlayRequest;
import ai.floedb.floecat.integration.rpc.ListCatalogOverlaysRequest;
import ai.floedb.floecat.integration.rpc.NamespacePath;
import ai.floedb.floecat.integration.rpc.SecretValue;
import ai.floedb.floecat.integration.rpc.UpdateCatalogOverlayRequest;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class CatalogOverlayIT {
  @GrpcClient("floecat")
  CatalogOverlaysGrpc.CatalogOverlaysBlockingStub overlays;

  @GrpcClient("floecat")
  CatalogIntegrationsGrpc.CatalogIntegrationsBlockingStub integrations;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalogs;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void catalogOverlayLifecycle() throws Exception {
    var parents = createParents("lifecycle");
    var created =
        overlays.createCatalogOverlay(
            CreateCatalogOverlayRequest.newBuilder()
                .setSpec(
                    CatalogOverlaySpec.newBuilder()
                        .setDisplayName("warehouse-overlay")
                        .setIntegrationId(parents.integrationId())
                        .setCatalogId(parents.catalogId())
                        .addIncludeNamespaces(path("sales"))
                        .addExcludeNamespaces(path("sales", "private")))
                .build());

    CatalogOverlay overlay = created.getOverlay();
    var overlayId = overlay.getResourceId();
    assertEquals(ResourceKind.RK_CATALOG_OVERLAY, overlayId.getKind());
    assertEquals(parents.integrationId(), overlay.getIntegrationId());
    assertEquals(parents.catalogId(), overlay.getCatalogId());
    assertEquals(List.of(path("sales")), overlay.getIncludeNamespacesList());
    assertEquals(List.of(path("sales", "private")), overlay.getExcludeNamespacesList());

    var fetchedById =
        overlays
            .getCatalogOverlay(
                GetCatalogOverlayRequest.newBuilder().setOverlayId(overlayId).build())
            .getOverlay();
    var fetchedByName =
        overlays
            .getCatalogOverlay(
                GetCatalogOverlayRequest.newBuilder().setDisplayName("warehouse-overlay").build())
            .getOverlay();
    assertEquals(overlay, fetchedById);
    assertEquals(overlayId, fetchedByName.getResourceId());

    assertEquals(
        1,
        overlays
            .listCatalogOverlays(ListCatalogOverlaysRequest.newBuilder().build())
            .getEntriesCount());
    var filtered =
        overlays.listCatalogOverlays(
            ListCatalogOverlaysRequest.newBuilder()
                .setIntegrationId(parents.integrationId())
                .build());
    assertEquals(1, filtered.getEntriesCount());
    assertEquals(overlayId, filtered.getEntries(0).getOverlay().getResourceId());

    var updated =
        overlays.updateCatalogOverlay(
            UpdateCatalogOverlayRequest.newBuilder()
                .setOverlayId(overlayId)
                .setSpec(
                    CatalogOverlaySpec.newBuilder()
                        .setDisplayName("warehouse-overlay-renamed")
                        .addIncludeNamespaces(path("finance"))
                        .addExcludeNamespaces(path("finance", "staging")))
                .setUpdateMask(
                    FieldMask.newBuilder()
                        .addPaths("display_name")
                        .addPaths("include_namespaces")
                        .addPaths("exclude_namespaces"))
                .setPrecondition(precondition(created.getMeta()))
                .build());
    assertEquals("warehouse-overlay-renamed", updated.getOverlay().getDisplayName());
    assertEquals(List.of(path("finance")), updated.getOverlay().getIncludeNamespacesList());
    assertEquals(
        List.of(path("finance", "staging")), updated.getOverlay().getExcludeNamespacesList());
    assertEquals(parents.integrationId(), updated.getOverlay().getIntegrationId());
    assertEquals(parents.catalogId(), updated.getOverlay().getCatalogId());

    overlays.deleteCatalogOverlay(
        DeleteCatalogOverlayRequest.newBuilder()
            .setOverlayId(overlayId)
            .setPrecondition(precondition(updated.getMeta()))
            .build());

    var missing =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                overlays.getCatalogOverlay(
                    GetCatalogOverlayRequest.newBuilder().setOverlayId(overlayId).build()));
    TestSupport.assertGrpcAndMc(
        missing, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "Catalog overlay not found");
    assertEquals(
        0,
        overlays
            .listCatalogOverlays(ListCatalogOverlaysRequest.newBuilder().build())
            .getEntriesCount());

    assertEquals(
        parents.catalogId(),
        catalogs
            .getCatalog(GetCatalogRequest.newBuilder().setCatalogId(parents.catalogId()).build())
            .getCatalog()
            .getResourceId());
    assertEquals(
        parents.integrationId(),
        integrations
            .getCatalogIntegration(
                GetCatalogIntegrationRequest.newBuilder()
                    .setIntegrationId(parents.integrationId())
                    .build())
            .getIntegration()
            .getResourceId());
  }

  @Test
  void createIsIdempotentThroughGrpc() {
    var parents = createParents("idempotent");
    var request =
        CreateCatalogOverlayRequest.newBuilder()
            .setSpec(
                CatalogOverlaySpec.newBuilder()
                    .setDisplayName("idempotent-overlay")
                    .setIntegrationId(parents.integrationId())
                    .setCatalogId(parents.catalogId()))
            .setIdempotency(IdempotencyKey.newBuilder().setKey("catalog-overlay-it"))
            .build();

    var first = overlays.createCatalogOverlay(request);
    var retry = overlays.createCatalogOverlay(request);

    assertEquals(first.getOverlay(), retry.getOverlay());
    assertEquals(first.getMeta(), retry.getMeta());
    assertEquals(
        1,
        overlays
            .listCatalogOverlays(ListCatalogOverlaysRequest.newBuilder().build())
            .getEntriesCount());
  }

  private Parents createParents(String suffix) {
    var catalog = TestSupport.createCatalog(catalogs, "overlay-it-catalog-" + suffix, "");
    var integration =
        integrations
            .createCatalogIntegration(
                CreateCatalogIntegrationRequest.newBuilder()
                    .setSpec(
                        CatalogIntegrationSpec.newBuilder()
                            .setDisplayName("overlay-it-integration-" + suffix)
                            .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
                            .setCatalogUri("https://catalog.example/" + suffix)
                            .setAuthentication(
                                CatalogAuthentication.newBuilder()
                                    .setBearer(BearerAuthentication.getDefaultInstance())))
                    .setCredentials(
                        CatalogIntegrationCredentials.newBuilder()
                            .setBearerToken(SecretValue.newBuilder().setValue("token-" + suffix)))
                    .build())
            .getIntegration();
    return new Parents(integration.getResourceId(), catalog.getResourceId());
  }

  private static NamespacePath path(String... segments) {
    return NamespacePath.newBuilder().addAllSegments(List.of(segments)).build();
  }

  private static Precondition precondition(MutationMeta meta) {
    return Precondition.newBuilder()
        .setExpectedVersion(meta.getPointerVersion())
        .setExpectedEtag(meta.getEtag())
        .build();
  }

  private record Parents(
      ai.floedb.floecat.common.rpc.ResourceId integrationId,
      ai.floedb.floecat.common.rpc.ResourceId catalogId) {}
}
