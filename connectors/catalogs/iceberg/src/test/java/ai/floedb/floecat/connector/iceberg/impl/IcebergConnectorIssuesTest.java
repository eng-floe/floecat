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

package ai.floedb.floecat.connector.iceberg.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.gateway.iceberg.rest.common.TestS3Fixtures;
import java.util.HashMap;
import java.util.Set;
import org.junit.jupiter.api.Test;

class IcebergConnectorIssuesTest {

  @Test
  void skipsMalformedBoundsFromTpcdsSfoneFixture() {
    TestS3Fixtures.seedTpcdsSfoneFixtureOnce();

    var props = new HashMap<String, String>();
    props.putAll(
        TestS3Fixtures.fileIoProperties(
            TestS3Fixtures.bucketPath().getParent().toAbsolutePath().toString()));
    props.put("external.namespace", "tpcds_sfone");
    props.put("external.table-name", "catalog_returns");
    props.put("stats.ndv.enabled", "false");
    props.put("iceberg.source", "filesystem");
    String metadataLocation =
        TestS3Fixtures.tpcdsSfoneUri(
            "floe_test.db/tpcds_sfone/catalog_returns/metadata/00002-fb845f92-5b90-4bd1-8670-3cab11eb68b1.metadata.json");

    try (FloecatConnector connector =
        IcebergConnectorFactory.create(
            metadataLocation, props, "none", new HashMap<>(), new HashMap<>())) {
      var snapshots =
          assertDoesNotThrow(
              () ->
                  connector.enumerateSnapshotsWithStats(
                      "tpcds_sfone",
                      "catalog_returns",
                      ResourceId.newBuilder()
                          .setAccountId("test-account")
                          .setId("test-table")
                          .setKind(ResourceKind.RK_TABLE)
                          .build(),
                      Set.of()));

      assertNotNull(snapshots);
      assertFalse(snapshots.isEmpty(), "expected snapshots from tpcds_sfone fixture");
      assertNotNull(snapshots.get(0).tableStats(), "expected table stats to still be produced");
      assertTrue(
          snapshots.get(0).fileStats().stream().anyMatch(f -> !f.columns().isEmpty()),
          "expected file-level stats to still be produced");
    }
  }
}
