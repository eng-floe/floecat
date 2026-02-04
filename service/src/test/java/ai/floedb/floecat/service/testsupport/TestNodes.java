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

package ai.floedb.floecat.service.testsupport;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Helper factories for graph tests. */
public final class TestNodes {

  private TestNodes() {}

  public static UserTableNode tableNode(ResourceId tableId, String schemaJson) {
    return new UserTableNode(
        tableId,
        1L,
        Instant.EPOCH,
        rid(tableId.getAccountId(), "cat-" + tableId.getId()),
        rid(tableId.getAccountId(), "ns-" + tableId.getId()),
        tableId.getId(),
        TableFormat.TF_ICEBERG,
        ColumnIdAlgorithm.CID_FIELD_ID,
        schemaJson,
        Map.of(),
        List.of(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        List.of(),
        Map.of(),
        Map.of());
  }

  static ResourceId rid(String accountId, String id) {
    return ResourceId.newBuilder()
        .setAccountId(accountId)
        .setId(id)
        .setKind(ResourceKind.RK_TABLE)
        .build();
  }
}
