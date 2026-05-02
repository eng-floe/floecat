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

package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableMetadataImportService {
  private static final Logger LOG = Logger.getLogger(TableMetadataImportService.class);

  @Inject MetadataFileIoSupport metadataFileIoSupport;
  @Inject TableMetadataImportMapperSupport tableMetadataImportMapperSupport;

  public record ImportedMetadata(
      String schemaJson,
      Map<String, String> properties,
      String tableLocation,
      IcebergMetadata icebergMetadata,
      ImportedSnapshot currentSnapshot,
      List<ImportedSnapshot> snapshots) {}

  public record ImportedSnapshot(
      Long snapshotId,
      Long parentSnapshotId,
      Long sequenceNumber,
      Long timestampMs,
      String manifestList,
      Map<String, String> summary,
      Integer schemaId) {}

  public ImportedMetadata importMetadata(
      String metadataLocation, Map<String, String> ioProperties) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      throw new IllegalArgumentException("metadata-location is required");
    }
    LOG.infof(
        "Importing Iceberg metadata location=%s fileIO=%s ioProps=%s",
        metadataLocation,
        ioProperties == null ? "<null>" : ioProperties.getOrDefault("io-impl", "<default>"),
        metadataFileIoSupport.redactIoProperties(ioProperties));
    FileIO fileIO = null;
    try {
      fileIO = metadataFileIoSupport.newImportFileIo(ioProperties);
      TableMetadata metadata = TableMetadataParser.read(fileIO, metadataLocation);
      return tableMetadataImportMapperSupport.toImportedMetadata(metadata, metadataLocation);
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      LOG.debugf(e, "Failed to read Iceberg metadata from %s", metadataLocation);
      throw new IllegalArgumentException(
          "Unable to read Iceberg metadata from " + metadataLocation, e);
    } finally {
      metadataFileIoSupport.closeQuietly(fileIO);
    }
  }
}
