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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class ConnectorTransferCliTest {
  @Test
  void usesTlsByDefaultAndRequiresExplicitPlaintextOptIn() {
    var defaults = new ConnectorTransferCli();
    var plaintext = new ConnectorTransferCli();

    new CommandLine(plaintext).parseArgs("--plaintext");

    assertThat(defaults.plaintext).isFalse();
    assertThat(plaintext.plaintext).isTrue();
  }

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

  @Test
  void connectorIdSelectorWinsOverAnotherConnectorsDisplayName() {
    var namedAfterId = connector("connector-a", "connector-b");
    var idMatch = connector("connector-b", "other-name");

    assertThat(ConnectorTransferCli.select(List.of(namedAfterId, idMatch), List.of("connector-b")))
        .containsExactly(idMatch);
  }

  @Test
  void skippedAndFailedImportsDoNotRunMutationValidation() {
    var validations = new AtomicInteger();
    var entry =
        ConnectorTransferEntry.newBuilder()
            .setPortableSpec(ConnectorSpec.newBuilder().setDisplayName("connector"))
            .build();
    var current = connector("connector-id", "connector");

    var skipped =
        ConnectorTransferCli.prepareImport(
            entry,
            current,
            ConnectorTransferCli.ConflictMode.SKIP,
            spec -> {
              validations.incrementAndGet();
              return "valid";
            });
    var failed =
        ConnectorTransferCli.prepareImport(
            entry,
            current,
            ConnectorTransferCli.ConflictMode.FAIL,
            spec -> {
              validations.incrementAndGet();
              return "valid";
            });

    assertThat(skipped.action()).isEqualTo(ConnectorTransferCli.ImportAction.SKIP);
    assertThat(failed.action()).isEqualTo(ConnectorTransferCli.ImportAction.FAIL);
    assertThat(validations).hasValue(0);
  }

  @Test
  void createdAndReplacedImportsRunMutationValidation() {
    var validations = new AtomicInteger();
    var entry =
        ConnectorTransferEntry.newBuilder()
            .setPortableSpec(ConnectorSpec.newBuilder().setDisplayName("connector"))
            .build();
    ConnectorTransferCli.ImportValidator validator =
        spec -> {
          validations.incrementAndGet();
          return "valid";
        };

    var created =
        ConnectorTransferCli.prepareImport(
            entry, null, ConnectorTransferCli.ConflictMode.FAIL, validator);
    var replaced =
        ConnectorTransferCli.prepareImport(
            entry,
            connector("connector-id", "connector"),
            ConnectorTransferCli.ConflictMode.REPLACE,
            validator);

    assertThat(created.validationSummary()).isEqualTo("valid");
    assertThat(replaced.validationSummary()).isEqualTo("valid");
    assertThat(validations).hasValue(2);
  }

  private static Connector connector(String id, String displayName) {
    return Connector.newBuilder()
        .setResourceId(ResourceId.newBuilder().setId(id))
        .setDisplayName(displayName)
        .build();
  }
}
