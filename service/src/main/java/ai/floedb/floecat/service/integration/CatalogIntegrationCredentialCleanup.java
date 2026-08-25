/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.integration;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationCredentials;
import ai.floedb.floecat.service.repo.impl.CatalogIntegrationRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.jboss.logging.Logger;

@ApplicationScoped
public class CatalogIntegrationCredentialCleanup {
  private static final Logger LOG = Logger.getLogger(CatalogIntegrationCredentialCleanup.class);

  @Inject PointerStore pointerStore;
  @Inject CatalogIntegrationRepository integrations;
  @Inject CatalogIntegrationCredentialStore credentials;

  public record Result(int scanned, int deleted) {}

  public boolean schedule(CatalogIntegration integration) {
    if (!CatalogIntegrationCredentialStore.hasStoredCredentials(integration)) return false;
    schedule(
        integration.getResourceId(), integration.getAuthentication().getCredentialGeneration());
    return true;
  }

  public void cleanIfSuperseded(CatalogIntegration integration) {
    if (!CatalogIntegrationCredentialStore.hasStoredCredentials(integration)) return;
    cleanIfSuperseded(
        integration.getResourceId(), integration.getAuthentication().getCredentialGeneration());
  }

  public void cleanPrepared(
      ResourceId integrationId,
      long generation,
      CatalogIntegrationCredentials preparedCredentials) {
    if (preparedCredentials.getCredentialCase()
        == CatalogIntegrationCredentials.CredentialCase.CREDENTIAL_NOT_SET) return;
    schedule(integrationId, generation);
    cleanIfSuperseded(integrationId, generation);
  }

  public boolean cancelIfResourceUnchanged(
      CatalogIntegration integration, long expectedPointerVersion) {
    if (!CatalogIntegrationCredentialStore.hasStoredCredentials(integration)
        || expectedPointerVersion <= 0L) return false;
    ResourceId integrationId = integration.getResourceId();
    String markerKey =
        key(integrationId, integration.getAuthentication().getCredentialGeneration());
    Pointer marker = pointerStore.get(markerKey).orElse(null);
    if (marker == null) return false;
    String canonicalKey =
        Keys.catalogIntegrationPointerById(integrationId.getAccountId(), integrationId.getId());
    return pointerStore.compareAndSetBatch(
        List.of(
            new PointerStore.CasCheck(canonicalKey, expectedPointerVersion),
            new PointerStore.CasDelete(markerKey, marker.getVersion())));
  }

  public Result drain(long deadlineMs, int pageSize) {
    int scanned = 0;
    int deleted = 0;
    String token = "";
    while (System.currentTimeMillis() < deadlineMs) {
      var next = new StringBuilder();
      var markers =
          pointerStore.listPointersByPrefix(
              Keys.catalogIntegrationCredentialCleanupPrefix(), Math.max(1, pageSize), token, next);
      for (Pointer marker : markers) {
        if (System.currentTimeMillis() >= deadlineMs) break;
        scanned++;
        CleanupTarget target = parse(marker.getKey());
        if (target == null) {
          LOG.errorf(
              "invalid catalog integration credential cleanup marker key=%s", marker.getKey());
          if (pointerStore.compareAndDelete(marker.getKey(), marker.getVersion())) deleted++;
          continue;
        }
        if (cleanIfSuperseded(target.integrationId(), target.generation())) deleted++;
      }
      token = next.toString();
      if (token.isEmpty()) break;
    }
    return new Result(scanned, deleted);
  }

  private void schedule(ResourceId integrationId, long generation) {
    String key = key(integrationId, generation);
    if (pointerStore.get(key).isPresent()) return;
    pointerStore.compareAndSet(key, 0L, PointerReferences.opaqueMarkerPointer(key, key, 1L));
  }

  private boolean cleanIfSuperseded(ResourceId integrationId, long generation) {
    String key = key(integrationId, generation);
    Pointer marker = pointerStore.get(key).orElse(null);
    if (marker == null) return false;
    try {
      var current = integrations.getByIdForMutation(integrationId);
      if (current.isPresent()
          && CatalogIntegrationCredentialStore.hasStoredCredentials(current.get())
          && current.get().getAuthentication().getCredentialGeneration() == generation) {
        return false;
      }
      credentials.deleteImmediately(integrationId, generation);
      return pointerStore.compareAndDelete(key, marker.getVersion());
    } catch (RuntimeException failure) {
      LOG.warnf(
          failure,
          "catalog integration credential cleanup deferred account=%s integration=%s generation=%s",
          integrationId.getAccountId(),
          integrationId.getId(),
          Long.toUnsignedString(generation));
      return false;
    }
  }

  private static String key(ResourceId integrationId, long generation) {
    return Keys.catalogIntegrationCredentialCleanupPointer(
        integrationId.getAccountId(), integrationId.getId(), generation);
  }

  private static CleanupTarget parse(String key) {
    String prefix = Keys.catalogIntegrationCredentialCleanupPrefix();
    if (key == null || !key.startsWith(prefix)) return null;
    String[] parts = key.substring(prefix.length()).split("/", -1);
    if (parts.length != 3) return null;
    try {
      String accountId = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
      String integrationId = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
      long generation = Long.parseUnsignedLong(parts[2]);
      if (accountId.isBlank() || integrationId.isBlank() || generation == 0L) return null;
      return new CleanupTarget(
          ResourceId.newBuilder()
              .setAccountId(accountId)
              .setId(integrationId)
              .setKind(ResourceKind.RK_CATALOG_INTEGRATION)
              .build(),
          generation);
    } catch (IllegalArgumentException failure) {
      return null;
    }
  }

  private record CleanupTarget(ResourceId integrationId, long generation) {}
}
