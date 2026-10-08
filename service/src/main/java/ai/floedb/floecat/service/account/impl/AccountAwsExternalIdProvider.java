/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.account.impl;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.UUID;
import java.util.function.Supplier;

/** Supplies the stable, Floecat-issued AWS external ID belonging to an account. */
@ApplicationScoped
public class AccountAwsExternalIdProvider {
  private static final String EXTERNAL_ID_PREFIX = "floecat-";
  private static final String SESSION_NAME_PREFIX = "floecat-";

  @Inject AccountRepository accounts;
  Supplier<String> externalIds = AccountAwsExternalIdProvider::newExternalId;

  public static String newExternalId() {
    return EXTERNAL_ID_PREFIX + UUID.randomUUID();
  }

  public String getOrCreate(String accountId) {
    String tenant = requireAccountId(accountId);
    String candidate = null;
    ResourceId id =
        ResourceId.newBuilder()
            .setAccountId(tenant)
            .setId(tenant)
            .setKind(ResourceKind.RK_ACCOUNT)
            .build();
    var cached = accounts.getById(id).orElseThrow(() -> new AccountMissingException(tenant));
    if (!cached.getAwsExternalId().isBlank()) {
      return cached.getAwsExternalId();
    }
    for (; ; ) {
      var current =
          accounts
              .getByIdWithMetaForMutation(id)
              .orElseThrow(() -> new AccountMissingException(tenant));
      if (!current.value().getAwsExternalId().isBlank()) {
        return current.value().getAwsExternalId();
      }
      if (candidate == null) candidate = externalIds.get();
      var updated = current.value().toBuilder().setAwsExternalId(candidate).build();
      try {
        if (accounts.update(updated, current.meta().getPointerVersion())) {
          return candidate;
        }
      } catch (BaseResourceRepository.AccountDeletionInProgressException deleting) {
        throw new AccountMissingException(tenant, deleting);
      }
    }
  }

  public static String roleSessionName(String accountId) {
    String tenant = requireAccountId(accountId);
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(tenant.getBytes(StandardCharsets.UTF_8));
      return SESSION_NAME_PREFIX + HexFormat.of().formatHex(digest, 0, 12);
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }

  private static String requireAccountId(String accountId) {
    if (accountId == null || accountId.isBlank()) {
      throw new IllegalArgumentException("account_id must be non-blank");
    }
    return accountId.trim();
  }

  public static final class AccountMissingException extends RuntimeException {
    public AccountMissingException(String accountId) {
      super("Floecat account does not exist: " + accountId);
    }

    private AccountMissingException(String accountId, Throwable cause) {
      super("Floecat account does not exist: " + accountId, cause);
    }
  }
}
