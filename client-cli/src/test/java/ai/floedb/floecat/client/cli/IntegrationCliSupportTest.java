/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package ai.floedb.floecat.client.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationEntry;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationType;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationsGrpc;
import ai.floedb.floecat.integration.rpc.CatalogOverlay;
import ai.floedb.floecat.integration.rpc.CatalogOverlayEntry;
import ai.floedb.floecat.integration.rpc.CatalogOverlaysGrpc;
import ai.floedb.floecat.integration.rpc.CreateCatalogIntegrationRequest;
import ai.floedb.floecat.integration.rpc.CreateCatalogIntegrationResponse;
import ai.floedb.floecat.integration.rpc.CreateCatalogOverlayRequest;
import ai.floedb.floecat.integration.rpc.CreateCatalogOverlayResponse;
import ai.floedb.floecat.integration.rpc.DeleteCatalogIntegrationRequest;
import ai.floedb.floecat.integration.rpc.DeleteCatalogIntegrationResponse;
import ai.floedb.floecat.integration.rpc.DeleteCatalogOverlayRequest;
import ai.floedb.floecat.integration.rpc.DeleteCatalogOverlayResponse;
import ai.floedb.floecat.integration.rpc.GetCatalogIntegrationRequest;
import ai.floedb.floecat.integration.rpc.GetCatalogIntegrationResponse;
import ai.floedb.floecat.integration.rpc.GetCatalogOverlayRequest;
import ai.floedb.floecat.integration.rpc.GetCatalogOverlayResponse;
import ai.floedb.floecat.integration.rpc.ListCatalogIntegrationsRequest;
import ai.floedb.floecat.integration.rpc.ListCatalogIntegrationsResponse;
import ai.floedb.floecat.integration.rpc.ListCatalogOverlaysRequest;
import ai.floedb.floecat.integration.rpc.ListCatalogOverlaysResponse;
import ai.floedb.floecat.integration.rpc.UpdateCatalogIntegrationAuthenticationRequest;
import ai.floedb.floecat.integration.rpc.UpdateCatalogIntegrationAuthenticationResponse;
import ai.floedb.floecat.integration.rpc.UpdateCatalogIntegrationRequest;
import ai.floedb.floecat.integration.rpc.UpdateCatalogIntegrationResponse;
import ai.floedb.floecat.integration.rpc.UpdateCatalogOverlayRequest;
import ai.floedb.floecat.integration.rpc.UpdateCatalogOverlayResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import org.junit.jupiter.api.Test;

class IntegrationCliSupportTest {
  private static final String INTEGRATION_ID = "00000000-0000-0000-0000-000000000011";
  private static final String OVERLAY_ID = "00000000-0000-0000-0000-000000000012";

  @Test
  void createsIntegrationWithOAuthCredentials() throws Exception {
    try (Harness h = new Harness()) {
      h.run(
          "integration",
          List.of(
              "create",
              "lakehouse",
              "iceberg-rest",
              "https://catalog.example/v1",
              "--auth-type",
              "oauth-client-credentials",
              "--auth",
              "client_id=floecat",
              "token_uri=https://identity.example/token",
              "scopes=catalog.read,catalog.write",
              "--cred",
              "client_secret=secret"));

      var spec = h.integrations.lastCreate.getSpec();
      assertEquals("lakehouse", spec.getDisplayName());
      assertEquals(CatalogIntegrationType.CIT_ICEBERG_REST, spec.getType());
      assertEquals("https://catalog.example/v1", spec.getCatalogUri());
      assertEquals("floecat", spec.getAuthentication().getOauthClientCredentials().getClientId());
      assertEquals(
          List.of("catalog.read", "catalog.write"),
          spec.getAuthentication().getOauthClientCredentials().getScopesList());
      assertEquals(
          "secret", h.integrations.lastCreate.getCredentials().getOauthClientSecret().getValue());
    }
  }

  @Test
  void createsIntegrationWithSigV4AccessKeyCredentials() throws Exception {
    try (Harness h = new Harness()) {
      h.run(
          "integration",
          List.of(
              "create",
              "lakehouse",
              "iceberg-rest",
              "https://catalog.example/v1",
              "--auth-type",
              "aws-sigv4",
              "--auth",
              "region=us-east-1",
              "credential_source=access-key",
              "access_key_id=AKIAEXAMPLE",
              "--cred",
              "secret_access_key=secret",
              "session_token=session"));

      var auth = h.integrations.lastCreate.getSpec().getAuthentication().getAwsSigv4();
      assertEquals("us-east-1", auth.getRegion());
      assertEquals("AKIAEXAMPLE", auth.getAwsAccessKey().getAccessKeyId());
      assertEquals(
          "session",
          h.integrations.lastCreate.getCredentials().getAwsAccessKey().getSessionToken());
    }
  }

  @Test
  void listsAndGetsIntegrationByName() throws Exception {
    try (Harness h = new Harness()) {
      assertTrue(h.run("integrations", List.of()).contains("lakehouse"));
      assertTrue(h.run("integration", List.of("get", "lakehouse")).contains("lakehouse"));
    }
  }

  @Test
  void updatesAndDeletesIntegrationById() throws Exception {
    try (Harness h = new Harness()) {
      h.run("integration", List.of("update", INTEGRATION_ID, "--display", "renamed"));
      assertEquals(
          List.of("display_name"), h.integrations.lastUpdate.getUpdateMask().getPathsList());
      assertEquals("renamed", h.integrations.lastUpdate.getSpec().getDisplayName());

      h.run("integration", List.of("delete", INTEGRATION_ID, "--cascade"));
      assertEquals(INTEGRATION_ID, h.integrations.lastDelete.getIntegrationId().getId());
      assertTrue(h.integrations.lastDelete.getCascade());
    }
  }

  @Test
  void rotatesIntegrationAuthentication() throws Exception {
    try (Harness h = new Harness()) {
      h.run(
          "integration",
          List.of(
              "update-auth",
              INTEGRATION_ID,
              "--auth-type",
              "bearer",
              "--cred",
              "token=replacement",
              "--etag",
              "etag-1"));

      assertEquals(INTEGRATION_ID, h.integrations.lastAuthUpdate.getIntegrationId().getId());
      assertTrue(h.integrations.lastAuthUpdate.getAuthentication().hasBearer());
      assertEquals(
          "replacement",
          h.integrations.lastAuthUpdate.getCredentials().getBearerToken().getValue());
      assertEquals("etag-1", h.integrations.lastAuthUpdate.getPrecondition().getExpectedEtag());
    }
  }

  @Test
  void createsOverlayForIntegration() throws Exception {
    try (Harness h = new Harness()) {
      h.run(
          "overlay",
          List.of("create", "sales", "lakehouse", "--include", "prod.sales,prod.reference"));

      var spec = h.overlays.lastCreate.getSpec();
      assertEquals(INTEGRATION_ID, spec.getIntegrationId().getId());
      assertEquals(2, spec.getIncludeNamespacesCount());
      assertEquals(List.of("prod", "sales"), spec.getIncludeNamespaces(0).getSegmentsList());
    }
  }

  @Test
  void listsAndGetsOverlayByName() throws Exception {
    try (Harness h = new Harness()) {
      assertTrue(h.run("overlays", List.of()).contains("sales"));
      assertTrue(h.run("overlay", List.of("get", "sales")).contains("sales"));
    }
  }

  @Test
  void updatesAndDeletesOverlayById() throws Exception {
    try (Harness h = new Harness()) {
      h.run("overlay", List.of("update", OVERLAY_ID, "--display", "renamed", "--exclude", "tmp"));
      assertEquals(
          List.of("display_name", "exclude_namespaces"),
          h.overlays.lastUpdate.getUpdateMask().getPathsList());
      assertEquals("renamed", h.overlays.lastUpdate.getSpec().getDisplayName());

      h.run("overlay", List.of("delete", OVERLAY_ID));
      assertEquals(OVERLAY_ID, h.overlays.lastDelete.getOverlayId().getId());
    }
  }

  private static ResourceId id(String id, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId("account").setId(id).setKind(kind).build();
  }

  private static final class Harness implements AutoCloseable {
    final IntegrationService integrations = new IntegrationService();
    final OverlayService overlays = new OverlayService();
    final Server server;
    final ManagedChannel channel;
    final CatalogIntegrationsGrpc.CatalogIntegrationsBlockingStub integrationStub;
    final CatalogOverlaysGrpc.CatalogOverlaysBlockingStub overlayStub;

    Harness() throws Exception {
      String name = InProcessServerBuilder.generateName();
      server =
          InProcessServerBuilder.forName(name)
              .directExecutor()
              .addService(integrations)
              .addService(overlays)
              .build()
              .start();
      channel = InProcessChannelBuilder.forName(name).directExecutor().build();
      integrationStub = CatalogIntegrationsGrpc.newBlockingStub(channel);
      overlayStub = CatalogOverlaysGrpc.newBlockingStub(channel);
    }

    String run(String command, List<String> args) {
      var bytes = new ByteArrayOutputStream();
      IntegrationCliSupport.handle(
          command, args, new PrintStream(bytes), integrationStub, overlayStub, () -> "account");
      return bytes.toString();
    }

    @Override
    public void close() {
      channel.shutdownNow();
      server.shutdownNow();
    }
  }

  private static final class IntegrationService
      extends CatalogIntegrationsGrpc.CatalogIntegrationsImplBase {
    final CatalogIntegration integration =
        CatalogIntegration.newBuilder()
            .setResourceId(id(INTEGRATION_ID, ResourceKind.RK_CATALOG_INTEGRATION))
            .setDisplayName("lakehouse")
            .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
            .setCatalogUri("https://catalog.example/v1")
            .build();
    CreateCatalogIntegrationRequest lastCreate;
    UpdateCatalogIntegrationRequest lastUpdate;
    UpdateCatalogIntegrationAuthenticationRequest lastAuthUpdate;
    DeleteCatalogIntegrationRequest lastDelete;

    @Override
    public void listCatalogIntegrations(
        ListCatalogIntegrationsRequest request,
        StreamObserver<ListCatalogIntegrationsResponse> observer) {
      respond(
          observer,
          ListCatalogIntegrationsResponse.newBuilder()
              .addEntries(CatalogIntegrationEntry.newBuilder().setIntegration(integration))
              .build());
    }

    @Override
    public void getCatalogIntegration(
        GetCatalogIntegrationRequest request,
        StreamObserver<GetCatalogIntegrationResponse> observer) {
      respond(
          observer, GetCatalogIntegrationResponse.newBuilder().setIntegration(integration).build());
    }

    @Override
    public void createCatalogIntegration(
        CreateCatalogIntegrationRequest request,
        StreamObserver<CreateCatalogIntegrationResponse> observer) {
      lastCreate = request;
      respond(
          observer,
          CreateCatalogIntegrationResponse.newBuilder()
              .setIntegration(
                  integration.toBuilder().setDisplayName(request.getSpec().getDisplayName()))
              .build());
    }

    @Override
    public void updateCatalogIntegration(
        UpdateCatalogIntegrationRequest request,
        StreamObserver<UpdateCatalogIntegrationResponse> observer) {
      lastUpdate = request;
      respond(
          observer,
          UpdateCatalogIntegrationResponse.newBuilder().setIntegration(integration).build());
    }

    @Override
    public void updateCatalogIntegrationAuthentication(
        UpdateCatalogIntegrationAuthenticationRequest request,
        StreamObserver<UpdateCatalogIntegrationAuthenticationResponse> observer) {
      lastAuthUpdate = request;
      respond(
          observer,
          UpdateCatalogIntegrationAuthenticationResponse.newBuilder()
              .setIntegration(integration)
              .build());
    }

    @Override
    public void deleteCatalogIntegration(
        DeleteCatalogIntegrationRequest request,
        StreamObserver<DeleteCatalogIntegrationResponse> observer) {
      lastDelete = request;
      respond(observer, DeleteCatalogIntegrationResponse.getDefaultInstance());
    }
  }

  private static final class OverlayService extends CatalogOverlaysGrpc.CatalogOverlaysImplBase {
    final CatalogOverlay overlay =
        CatalogOverlay.newBuilder()
            .setResourceId(id(OVERLAY_ID, ResourceKind.RK_CATALOG_OVERLAY))
            .setDisplayName("sales")
            .setIntegrationId(id(INTEGRATION_ID, ResourceKind.RK_CATALOG_INTEGRATION))
            .build();
    CreateCatalogOverlayRequest lastCreate;
    UpdateCatalogOverlayRequest lastUpdate;
    DeleteCatalogOverlayRequest lastDelete;

    @Override
    public void listCatalogOverlays(
        ListCatalogOverlaysRequest request, StreamObserver<ListCatalogOverlaysResponse> observer) {
      respond(
          observer,
          ListCatalogOverlaysResponse.newBuilder()
              .addEntries(CatalogOverlayEntry.newBuilder().setOverlay(overlay))
              .build());
    }

    @Override
    public void getCatalogOverlay(
        GetCatalogOverlayRequest request, StreamObserver<GetCatalogOverlayResponse> observer) {
      respond(observer, GetCatalogOverlayResponse.newBuilder().setOverlay(overlay).build());
    }

    @Override
    public void createCatalogOverlay(
        CreateCatalogOverlayRequest request,
        StreamObserver<CreateCatalogOverlayResponse> observer) {
      lastCreate = request;
      respond(
          observer,
          CreateCatalogOverlayResponse.newBuilder()
              .setOverlay(overlay.toBuilder().setDisplayName(request.getSpec().getDisplayName()))
              .build());
    }

    @Override
    public void updateCatalogOverlay(
        UpdateCatalogOverlayRequest request,
        StreamObserver<UpdateCatalogOverlayResponse> observer) {
      lastUpdate = request;
      respond(observer, UpdateCatalogOverlayResponse.newBuilder().setOverlay(overlay).build());
    }

    @Override
    public void deleteCatalogOverlay(
        DeleteCatalogOverlayRequest request,
        StreamObserver<DeleteCatalogOverlayResponse> observer) {
      lastDelete = request;
      respond(observer, DeleteCatalogOverlayResponse.getDefaultInstance());
    }
  }

  private static <T> void respond(StreamObserver<T> observer, T value) {
    observer.onNext(value);
    observer.onCompleted();
  }
}
