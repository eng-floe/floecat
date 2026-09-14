/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.storage.kv.dynamodb.ps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
  void assignmentFenceUsesOnePartitionPerAccount() {
    var key = PointerStoreEntity._testKey("/assignment-fence/acct-a");

    assertEquals("_ASSIGNMENT_FENCE/acct-a", key.partitionKey());
    assertEquals("fence", key.sortKey());
  }

  @Test
  void memberAssignmentIndexUsesOnePartitionPerMember() {
    var key = PointerStoreEntity._testKey("/assignments/floecat-0");

    assertEquals("_MEMBER_ASSIGNMENT/floecat-0", key.partitionKey());
    assertEquals("index", key.sortKey());
  }

  @Test
  void assignmentRecordsAreRejectedWhenTheyCarryMoreThanOneSegment() {
    assertThrows(
        IllegalArgumentException.class, () -> PointerStoreEntity._testKey("/assignment-fence/a/b"));
    assertThrows(
        IllegalArgumentException.class, () -> PointerStoreEntity._testKey("/assignment-fence/"));
  }

  @Test
  void assignmentRecordsRoundTripThroughTheReverseMapping() {
    for (String key : new String[] {"/assignment-fence/acct-a", "/assignments/floecat-0"}) {
      assertEquals(key, PointerStoreEntity._testKeyOf(PointerStoreEntity._testKey(key)));
    }
  }

  @Test
  void credentialCleanupPrefixUsesDedicatedPartition() {
    var key = PointerStoreEntity.prefixKey("/catalog-integration-credential-cleanup/");

    assertEquals(PointerStoreEntity.CREDENTIAL_CLEANUP_PK, key.partitionKey());
    assertEquals("catalog-integration-credential-cleanup/", key.sortKey());
  }
}
