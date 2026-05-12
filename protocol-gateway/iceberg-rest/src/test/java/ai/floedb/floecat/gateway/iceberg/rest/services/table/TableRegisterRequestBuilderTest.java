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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService.ImportedMetadata;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService.ImportedSnapshot;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TableRegisterRequestBuilderTest {

  @Test
  void mergeImportedPropertiesDoesNotPersistSecretBearingRegisterProps() {
    TableRegisterRequestBuilder builder = new TableRegisterRequestBuilder();

    Map<String, String> merged =
        builder.mergeImportedProperties(
            Map.of("existing", "value"),
            null,
            Map.of(
                "s3.endpoint", "http://localhost:4566",
                "s3.access-key-id", "akid",
                "s3.secret-access-key", "secret",
                "s3.session-token", "session",
                "format-version", "2"));

    assertEquals("value", merged.get("existing"));
    assertEquals("http://localhost:4566", merged.get("s3.endpoint"));
    assertEquals("2", merged.get("format-version"));
    assertFalse(merged.containsKey("metadata-location"));
    assertFalse(merged.containsKey("s3.access-key-id"));
    assertFalse(merged.containsKey("s3.secret-access-key"));
    assertFalse(merged.containsKey("s3.session-token"));
  }

  @Test
  void buildRegisterTransactionRequestSkipsAddSnapshotForExistingSnapshotIds() {
    TableRegisterRequestBuilder builder = new TableRegisterRequestBuilder();
    ImportedSnapshot currentSnapshot =
        new ImportedSnapshot(11L, null, 3L, 1000L, "s3://manifest.avro", Map.of(), 1);
    ImportedMetadata importedMetadata =
        new ImportedMetadata(
            "{\"schema-id\":1,\"type\":\"struct\",\"fields\":[],\"last-column-id\":1}",
            Map.of("format-version", "2"),
            "s3://warehouse/orders",
            null,
            currentSnapshot,
            List.of(currentSnapshot));

    TransactionCommitRequest request =
        builder.buildRegisterTransactionRequest(
            List.of("db"),
            "orders",
            Map.of("location", "s3://warehouse/orders"),
            "s3://warehouse/orders/metadata/00003.metadata.json",
            importedMetadata,
            List.of(11L),
            false);

    assertTrue(
        request.tableChanges().get(0).updates().stream()
            .noneMatch(
                update ->
                    "add-snapshot".equals(update.get("action"))
                        && update.get("snapshot") instanceof Map<?, ?> snapshot
                        && Long.valueOf(11L).equals(snapshot.get("snapshot-id"))));
  }
}
