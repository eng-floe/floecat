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

package ai.floedb.floecat.gateway.iceberg.minimal.services.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TableStorageCleanupServiceTest {
  private final MinimalGatewayConfig config = Mockito.mock(MinimalGatewayConfig.class);

  @Test
  void purgeDeletesStorageLocationPrefix() {
    TrackingPrefixFileIo fileIo = Mockito.spy(new TrackingPrefixFileIo());
    TableStorageCleanupService service =
        new TableStorageCleanupService(config) {
          @Override
          protected FileIO newFileIo(java.util.Map<String, String> properties) {
            return fileIo;
          }
        };
    Table table =
        Table.newBuilder().putProperties("storage_location", "s3://bucket/db/orders").build();

    service.purgeTableData(table);

    assertEquals("s3://bucket/db/orders/", fileIo.deletedPrefix);
    verify(fileIo).close();
  }

  @Test
  void derivesTableLocationFromMetadataLocation() {
    Table table =
        Table.newBuilder()
            .putProperties(
                "metadata-location", "s3://bucket/db/orders/metadata/00001.metadata.json")
            .build();

    assertEquals("s3://bucket/db/orders", TableStorageCleanupService.tableLocation(table));
  }

  static class TrackingPrefixFileIo implements SupportsPrefixOperations {
    String deletedPrefix;

    @Override
    public Iterable<org.apache.iceberg.io.FileInfo> listPrefix(String prefix) {
      return java.util.List.of();
    }

    @Override
    public void deletePrefix(String prefix) {
      this.deletedPrefix = prefix;
    }

    @Override
    public org.apache.iceberg.io.InputFile newInputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public org.apache.iceberg.io.OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String path) {
      throw new UnsupportedOperationException();
    }
  }
}
