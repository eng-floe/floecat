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

package ai.floedb.floecat.tools.transfer;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorSpec;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.ConnectorTransferBundle;
import ai.floedb.floecat.connector.rpc.ConnectorTransferEntry;
import org.junit.jupiter.api.Test;

class ConnectorTransferCliTest {
  @Test
  void selectsCreateWhenNoConnectorExists() {
    for (var mode : ConnectorTransferCli.ConflictMode.values()) {
      assertThat(ConnectorTransferCli.importAction(false, mode))
          .isEqualTo(ConnectorTransferCli.ImportAction.CREATE);
    }
  }

  @Test
  void appliesSelectedConflictModeWhenConnectorExists() {
    assertThat(ConnectorTransferCli.importAction(true, ConnectorTransferCli.ConflictMode.FAIL))
        .isEqualTo(ConnectorTransferCli.ImportAction.FAIL);
    assertThat(ConnectorTransferCli.importAction(true, ConnectorTransferCli.ConflictMode.SKIP))
        .isEqualTo(ConnectorTransferCli.ImportAction.SKIP);
    assertThat(ConnectorTransferCli.importAction(true, ConnectorTransferCli.ConflictMode.REPLACE))
        .isEqualTo(ConnectorTransferCli.ImportAction.REPLACE);
  }

  @Test
  void idempotencyKeyIsStableWithinRunAndUniqueAcrossRuns() throws Exception {
    var entry =
        ConnectorTransferEntry.newBuilder()
            .setConnector(
                Connector.newBuilder()
                    .setResourceId(ResourceId.newBuilder().setId("source-connector")))
            .setPortableSpec(ConnectorSpec.newBuilder().setDisplayName("connector"))
            .build();
    var bundle = ConnectorTransferBundle.newBuilder().setSourceAccountId("source-account").build();

    String first = ConnectorTransferCli.idempotencyKey("run-1", bundle, entry);

    assertThat(ConnectorTransferCli.idempotencyKey("run-1", bundle, entry)).isEqualTo(first);
    assertThat(ConnectorTransferCli.idempotencyKey("run-2", bundle, entry)).isNotEqualTo(first);
  }

  @Test
  void replacementUpdatesInPlaceAndCoversEveryPortableField() {
    var current =
        Connector.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("target-connector"))
            .build();
    var request =
        ConnectorTransferCli.replacementRequest(
            current, ConnectorSpec.newBuilder().setDisplayName("connector").build());

    assertThat(request.getConnectorId()).isEqualTo(current.getResourceId());
    assertThat(request.getSpec().getState()).isEqualTo(ConnectorState.CS_ACTIVE);
    assertThat(request.getUpdateMask().getPathsList())
        .containsExactlyInAnyOrder(
            "display_name",
            "description",
            "kind",
            "source",
            "destination",
            "uri",
            "auth",
            "policy",
            "state",
            "properties");
  }
}
