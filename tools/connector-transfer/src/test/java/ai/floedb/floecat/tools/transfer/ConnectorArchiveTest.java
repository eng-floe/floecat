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
import com.google.protobuf.Timestamp;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
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

  @Test
  void rejectsDuplicateConnectorNamesBeforeImport() throws Exception {
    Path archive = temporaryDirectory.resolve("duplicates.zip");
    var first =
        ConnectorTransferEntry.newBuilder()
            .setConnector(
                Connector.newBuilder()
                    .setResourceId(ResourceId.newBuilder().setId("connector-1"))
                    .setDisplayName("duplicate"))
            .setPortableSpec(ConnectorSpec.newBuilder().setDisplayName("duplicate"))
            .build();
    var second =
        first.toBuilder()
            .setConnector(
                first.getConnector().toBuilder()
                    .setResourceId(ResourceId.newBuilder().setId("connector-2")))
            .build();
    var bundle =
        ConnectorTransferBundle.newBuilder()
            .setFormatVersion(ConnectorArchive.FORMAT_VERSION)
            .addEntries(first)
            .addEntries(second)
            .build();
    ConnectorArchive.write(archive, bundle, false);

    assertThatThrownBy(() -> ConnectorArchive.read(archive))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("duplicate connector display name");
  }

  @Test
  void rejectsCredentialsEmbeddedInPortableSpecification() throws Exception {
    Path archive = temporaryDirectory.resolve("embedded-credentials.zip");
    var credentials =
        AuthCredentials.newBuilder()
            .setBearer(AuthCredentials.BearerToken.newBuilder().setToken("secret"))
            .build();
    var bundle =
        ConnectorTransferBundle.newBuilder()
            .setFormatVersion(ConnectorArchive.FORMAT_VERSION)
            .addEntries(
                ConnectorTransferEntry.newBuilder()
                    .setConnector(
                        Connector.newBuilder()
                            .setResourceId(ResourceId.newBuilder().setId("connector-1"))
                            .setDisplayName("connector"))
                    .setPortableSpec(
                        ConnectorSpec.newBuilder()
                            .setDisplayName("connector")
                            .setAuth(
                                ai.floedb.floecat.connector.rpc.AuthConfig.newBuilder()
                                    .setCredentials(credentials))))
            .build();
    ConnectorArchive.write(archive, bundle, false);

    assertThatThrownBy(() -> ConnectorArchive.read(archive))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("embeds credentials");
  }

  @Test
  void rejectsBundlesLargerThanTheReaderLimit() {
    assertThatThrownBy(() -> ConnectorArchive.validateBundleSize(64 * 1024 * 1024 + 1))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("exceeds size limit");
  }

  @Test
  void writesRfc3339ManifestTimestampWithNanosecondPrecision() throws Exception {
    Path archive = temporaryDirectory.resolve("timestamp.zip");
    var bundle =
        ConnectorTransferBundle.newBuilder()
            .setFormatVersion(ConnectorArchive.FORMAT_VERSION)
            .setExportedAt(Timestamp.newBuilder().setSeconds(123).setNanos(5))
            .build();

    ConnectorArchive.write(archive, bundle, false);

    try (var zip = new ZipFile(archive.toFile())) {
      assertThat(read(zip, "manifest.json"))
          .contains("\"exportedAt\": \"1970-01-01T00:02:03.000000005Z\"");
    }
  }

  @Test
  void protectsArchiveWithOwnerOnlyPermissions() throws Exception {
    Path archive = temporaryDirectory.resolve("permissions.zip");
    var bundle =
        ConnectorTransferBundle.newBuilder()
            .setFormatVersion(ConnectorArchive.FORMAT_VERSION)
            .build();

    ConnectorArchive.write(archive, bundle, false);

    assertThat(Files.getPosixFilePermissions(archive))
        .isEqualTo(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
  }

  private static String read(ZipFile zip, String entry) throws IOException {
    try (var in = zip.getInputStream(zip.getEntry(entry))) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
