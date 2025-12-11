package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableDropCleanupService {
  private static final Logger LOG = Logger.getLogger(TableDropCleanupService.class);

  @Inject IcebergGatewayConfig config;
  @Inject ObjectMapper mapper;

  public void purgeTableData(String catalogName, String namespace, String tableName, Table table) {
    if (table == null) {
      LOG.debugf(
          "Skipping purge for %s.%s in catalog %s because table metadata was unavailable",
          namespace, tableName, catalogName);
      return;
    }
    Map<String, String> props = table.getPropertiesMap();
    if (props == null || props.isEmpty()) {
      LOG.debugf(
          "Skipping purge for %s.%s in catalog %s because table properties were empty",
          namespace, tableName, catalogName);
      return;
    }
    String metadataLocation = MetadataLocationUtil.metadataLocation(props);
    if (metadataLocation == null || metadataLocation.isBlank()) {
      LOG.debugf(
          "Skipping purge for %s.%s in catalog %s because metadata-location was missing",
          namespace, tableName, catalogName);
      return;
    }
    FileIO fileIO = null;
    try {
      fileIO = FileIoFactory.createFileIo(props, config, true);
      String resolvedLocation = resolveMetadataLocation(fileIO, metadataLocation);
      if (resolvedLocation == null || resolvedLocation.isBlank()) {
        LOG.debugf(
            "Skipping purge for %s.%s in catalog %s because metadata-location could not be"
                + " resolved",
            namespace, tableName, catalogName);
        return;
      }
      TableMetadata metadata = TableMetadataParser.read(fileIO, resolvedLocation);
      CatalogUtil.dropTableData(fileIO, metadata);
      if (!resolvedLocation.equals(metadataLocation)) {
        deleteQuietly(fileIO, metadataLocation, "metadata pointer");
      }
      LOG.infof(
          "Purged Iceberg metadata and data for %s.%s in catalog %s (metadata=%s)",
          namespace, tableName, catalogName, resolvedLocation);
    } catch (Exception e) {
      LOG.warnf(
          e,
          "Failed to purge Iceberg metadata/data for %s.%s in catalog %s (metadata=%s)",
          namespace,
          tableName,
          catalogName,
          metadataLocation);
    } finally {
      closeQuietly(fileIO);
    }
  }

  private String resolveMetadataLocation(FileIO fileIO, String metadataLocation)
      throws IOException {
    if (fileIO == null || metadataLocation == null || metadataLocation.isBlank()) {
      return metadataLocation;
    }
    if (!MetadataLocationUtil.isPointer(metadataLocation)) {
      return metadataLocation;
    }
    InputFile pointerFile = fileIO.newInputFile(metadataLocation);
    if (!pointerFile.exists()) {
      return metadataLocation;
    }
    try (SeekableInputStream in = pointerFile.newStream()) {
      byte[] payload = in.readAllBytes();
      JsonNode node = mapper.readTree(payload);
      String resolved = node.path("metadata-location").asText(null);
      if (resolved == null || resolved.isBlank()) {
        resolved = node.path("metadata_location").asText(null);
      }
      return (resolved == null || resolved.isBlank()) ? metadataLocation : resolved;
    }
  }

  private void deleteQuietly(FileIO fileIO, String location, String description) {
    if (fileIO == null || location == null || location.isBlank()) {
      return;
    }
    try {
      fileIO.deleteFile(location);
    } catch (RuntimeException e) {
      LOG.debugf(e, "Failed to delete %s file %s", description, location);
    }
  }

  private void closeQuietly(FileIO fileIO) {
    if (fileIO instanceof AutoCloseable closable) {
      try {
        closable.close();
      } catch (Exception e) {
        LOG.debugf(e, "Failed to close FileIO %s", fileIO.getClass().getName());
      }
    }
  }
}
