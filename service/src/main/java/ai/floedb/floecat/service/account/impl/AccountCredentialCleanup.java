/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.account.impl;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.credentials.DefaultCredentialResolver;
import ai.floedb.floecat.service.integration.CatalogIntegrationCredentialCleanup;
import ai.floedb.floecat.service.repo.impl.CatalogIntegrationRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.storage.impl.StorageAuthorityResolver;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.jboss.logging.Logger;

/** Idempotently revokes every credential owned by an account before its resource rows disappear. */
@ApplicationScoped
public class AccountCredentialCleanup {
  private static final Logger LOG = Logger.getLogger(AccountCredentialCleanup.class);

  @Inject PointerStore pointerStore;
  @Inject DefaultCredentialResolver credentialResolver;
  @Inject SecretsManager secretsManager;
  @Inject CatalogIntegrationRepository catalogIntegrationRepo;
  @Inject CatalogIntegrationCredentialCleanup catalogIntegrationCredentialCleanup;

  public void revoke(ResourceId accountId) {
    revoke(accountId.getId());
  }

  public void revoke(String accountId) {
    var summary = new AccountCleanupSummary(accountId);
    LOG.infof("account_delete_credential_revocation_start account_id=%s", accountId);
    try {
      List<ResourceId> storageAuthorities =
          listCanonicalResourceIds(
              Keys.storageAuthorityPointerByIdPrefix(accountId),
              accountId,
              ResourceKind.RK_STORAGE_AUTHORITY);
      List<ResourceId> catalogIntegrations =
          listCanonicalResourceIds(
              Keys.catalogIntegrationPointerByIdPrefix(accountId),
              accountId,
              ResourceKind.RK_CATALOG_INTEGRATION);
      List<ResourceId> connectors =
          listCanonicalResourceIds(
              Keys.connectorPointerByIdPrefix(accountId), accountId, ResourceKind.RK_CONNECTOR);
      revokeCatalogIntegrationCredentials(catalogIntegrations, summary);
      cleanupStorageAuthorityCredentials(accountId, storageAuthorities, summary);
      cleanupConnectorCredentials(accountId, connectors, summary);
      LOG.infof(
          "account_delete_credential_revocation_complete account_id=%s catalog_integrations=%d storage_authorities=%d connectors=%d credential_deletes=%d",
          summary.accountId,
          summary.catalogIntegrationsDeleted,
          summary.storageAuthoritiesDeleted,
          summary.connectorsDeleted,
          summary.credentialsDeleted);
    } catch (RuntimeException e) {
      LOG.errorf(e, "account_delete_credential_revocation_failed account_id=%s", accountId);
      throw e;
    }
  }

  private void revokeCatalogIntegrationCredentials(
      List<ResourceId> integrationIds, AccountCleanupSummary summary) {
    for (ResourceId integrationId : integrationIds) {
      try {
        catalogIntegrationRepo
            .getById(integrationId)
            .ifPresent(
                integration -> {
                  catalogIntegrationCredentialCleanup.revokeForDeletedAccount(integration);
                  summary.catalogIntegrationsDeleted++;
                  summary.credentialsDeleted++;
                });
      } catch (BaseResourceRepository.CorruptionException corruption) {
        LOG.warnf(
            corruption,
            "account_delete_cleanup_skipping_corrupt_catalog_integration account_id=%s integration_id=%s",
            integrationId.getAccountId(),
            integrationId.getId());
      }
    }
  }

  private void cleanupStorageAuthorityCredentials(
      String accountId, List<ResourceId> authorities, AccountCleanupSummary summary) {
    for (ResourceId authorityId : authorities) {
      LOG.infof(
          "account_delete_cleanup_storage_authority account_id=%s authority_id=%s",
          accountId, authorityId.getId());
      secretsManager.delete(
          accountId, StorageAuthorityResolver.STORAGE_AUTHORITY_SECRET_TYPE, authorityId.getId());
      summary.storageAuthoritiesDeleted++;
      summary.credentialsDeleted++;
    }
  }

  private void cleanupConnectorCredentials(
      String accountId, List<ResourceId> connectors, AccountCleanupSummary summary) {
    for (ResourceId connectorId : connectors) {
      LOG.infof(
          "account_delete_cleanup_connector account_id=%s connector_id=%s",
          accountId, connectorId.getId());
      credentialResolver.delete(accountId, connectorId.getId());
      summary.connectorsDeleted++;
      summary.credentialsDeleted++;
    }
  }

  private List<ResourceId> listCanonicalResourceIds(
      String prefix, String accountId, ResourceKind kind) {
    var ids = new ArrayList<ResourceId>();
    var seenTokens = new HashSet<String>();
    String token = "";
    while (true) {
      var next = new StringBuilder();
      for (Pointer pointer : pointerStore.listPointersByPrefix(prefix, 200, token, next)) {
        String id;
        try {
          id = Keys.extractLastSegment(pointer.getKey());
        } catch (RuntimeException malformedKey) {
          LOG.warnf(
              malformedKey,
              "account_delete_cleanup_skipping_malformed_canonical_pointer account_id=%s pointer_key=%s",
              accountId,
              pointer.getKey());
          continue;
        }
        if (id == null || id.isBlank()) {
          LOG.warnf(
              "account_delete_cleanup_skipping_malformed_canonical_pointer account_id=%s pointer_key=%s",
              accountId, pointer.getKey());
          continue;
        }
        ids.add(ResourceId.newBuilder().setAccountId(accountId).setId(id).setKind(kind).build());
      }
      token = next.toString();
      if (token.isBlank()) return List.copyOf(ids);
      if (!seenTokens.add(token)) {
        throw new IllegalStateException("stagnant page token during account cleanup: " + token);
      }
    }
  }

  private static final class AccountCleanupSummary {
    private final String accountId;
    private int catalogIntegrationsDeleted;
    private int storageAuthoritiesDeleted;
    private int connectorsDeleted;
    private int credentialsDeleted;

    private AccountCleanupSummary(String accountId) {
      this.accountId = accountId;
    }
  }
}
