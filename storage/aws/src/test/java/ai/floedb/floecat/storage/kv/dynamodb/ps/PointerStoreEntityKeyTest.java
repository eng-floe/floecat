/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.storage.kv.dynamodb.ps;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class PointerStoreEntityKeyTest {

  @Test
  void credentialCleanupPointerUsesDedicatedPartition() {
    var key =
        PointerStoreEntity._testKey(
            "/catalog-integration-credential-cleanup/account/integration/3");

    assertEquals(PointerStoreEntity.CREDENTIAL_CLEANUP_PK, key.partitionKey());
    assertEquals("catalog-integration-credential-cleanup/account/integration/3", key.sortKey());
  }

  @Test
  void credentialCleanupPrefixUsesDedicatedPartition() {
    var key = PointerStoreEntity.prefixKey("/catalog-integration-credential-cleanup/");

    assertEquals(PointerStoreEntity.CREDENTIAL_CLEANUP_PK, key.partitionKey());
    assertEquals("catalog-integration-credential-cleanup/", key.sortKey());
  }
}
