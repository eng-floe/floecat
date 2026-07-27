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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorSpec;
import ai.floedb.floecat.connector.rpc.ConnectorTransferBundle;
import ai.floedb.floecat.connector.rpc.ConnectorTransferEntry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.zip.ZipFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConnectorArchiveTest {
  @TempDir Path temporaryDirectory;

  @Test
  void roundTripsBundleAndKeepsSecretsOutOfReadableMetadata() throws Exception {
    Path archive = temporaryDirectory.resolve("connectors.zip");
    var credentials =
        AuthCredentials.newBuilder()
            .setClient(
                AuthCredentials.ClientCredentials.newBuilder()
                    .setClientId("client")
                    .setClientSecret("highly-secret"))
            .build();
    var connector =
        Connector.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setId("connector-1")
                    .setKind(ResourceKind.RK_CONNECTOR))
            .setDisplayName("upstream")
            .setKind(ConnectorKind.CK_ICEBERG)
            .build();
    var spec =
        ConnectorSpec.newBuilder()
            .setDisplayName("upstream")
            .setKind(ConnectorKind.CK_ICEBERG)
            .build();
    var bundle =
        ConnectorTransferBundle.newBuilder()
            .setFormatVersion(ConnectorArchive.FORMAT_VERSION)
            .setSourceAccountId("acct")
            .addEntries(
                ConnectorTransferEntry.newBuilder()
                    .setConnector(connector)
                    .setPortableSpec(spec)
                    .setCredentials(credentials))
            .build();

    ConnectorArchive.write(archive, bundle, false);

    assertThat(ConnectorArchive.read(archive)).isEqualTo(bundle);
    try (var zip = new ZipFile(archive.toFile())) {
      String manifest = read(zip, "manifest.json");
      String portableSpec = read(zip, "connectors/connector-1/portable-spec.json");
      assertThat(manifest).contains("\"hasCredentials\": true").doesNotContain("highly-secret");
      assertThat(portableSpec).contains("upstream").doesNotContain("highly-secret");
      assertThat(zip.getEntry("connectors/connector-1/credentials.pb")).isNotNull();
    }
  }

  @Test
  void refusesToOverwriteWithoutForce() throws Exception {
    Path archive = temporaryDirectory.resolve("connectors.zip");
    var bundle =
        ConnectorTransferBundle.newBuilder()
            .setFormatVersion(ConnectorArchive.FORMAT_VERSION)
            .build();
    ConnectorArchive.write(archive, bundle, false);

    assertThatThrownBy(() -> ConnectorArchive.write(archive, bundle, false))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("--force");
  }

  private static String read(ZipFile zip, String entry) throws IOException {
    try (var in = zip.getInputStream(zip.getEntry(entry))) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
