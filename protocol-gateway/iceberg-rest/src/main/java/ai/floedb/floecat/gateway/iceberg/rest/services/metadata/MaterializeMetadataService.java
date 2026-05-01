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

import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.firstNonBlank;

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.jboss.logging.Logger;

@ApplicationScoped
public class MaterializeMetadataService {
  private static final Logger LOG = Logger.getLogger(MaterializeMetadataService.class);
  private static final Set<String> SKIPPED_SCHEMES = Set.of("floecat");

  @Inject ObjectMapper mapper;
  @Inject MetadataFileIoSupport metadataFileIoSupport = new MetadataFileIoSupport();

  @Inject
  MetadataVersionedLocationSupport metadataVersionedLocationSupport =
      new MetadataVersionedLocationSupport();

  public record MaterializeResult(String metadataLocation, TableMetadataView metadata) {}

  public MaterializeResult materialize(
      String namespaceFq,
      String tableName,
      TableMetadataView metadata,
      String metadataLocationOverride) {
    if (metadata == null) {
      LOG.debugf(
          "Skipping metadata materialization for %s.%s because commit metadata was empty",
          namespaceFq, tableName);
      return new MaterializeResult(metadataLocationOverride, null);
    }
    String requestedLocation = firstNonBlank(metadataLocationOverride, metadata.metadataLocation());
    if (requestedLocation != null && !shouldMaterialize(requestedLocation)) {
      LOG.debugf(
          "Skipping metadata materialization for %s.%s because metadata-location was %s",
          namespaceFq, tableName, requestedLocation);
      return new MaterializeResult(requestedLocation, metadata);
    }

    String resolvedLocation = null;
    FileIO fileIO = null;
    try {
      fileIO = newFileIo(metadata.properties());
      resolvedLocation =
          metadataVersionedLocationSupport.resolveVersionedLocation(
              fileIO, requestedLocation, metadata);
      if (resolvedLocation == null || resolvedLocation.isBlank()) {
        LOG.debugf(
            "Skipping metadata materialization for %s.%s because metadata-location was unavailable",
            namespaceFq, tableName);
        return new MaterializeResult(requestedLocation, metadata);
      }

      TableMetadataView resolvedMetadata = metadata.withMetadataLocation(resolvedLocation);
      TableMetadata parsed = parseMetadata(resolvedMetadata, resolvedLocation);
      writeMetadata(fileIO, resolvedLocation, parsed);
      LOG.infof(
          "Materialized Iceberg metadata files for %s.%s to %s",
          namespaceFq, tableName, resolvedLocation);
      return new MaterializeResult(resolvedLocation, resolvedMetadata);
    } catch (MaterializeMetadataException e) {
      throw e;
    } catch (Exception e) {
      LOG.warnf(
          e,
          "Materialize metadata failure namespace=%s table=%s target=%s",
          namespaceFq,
          tableName,
          resolvedLocation == null ? requestedLocation : resolvedLocation);
      throw new MaterializeMetadataException(
          "Failed to materialize Iceberg metadata files to " + resolvedLocation, e);
    } finally {
      metadataFileIoSupport.closeQuietly(fileIO);
    }
  }

  protected FileIO newFileIo(Map<String, String> props) {
    return metadataFileIoSupport.newMaterializationFileIo(props);
  }

  private boolean shouldMaterialize(String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return false;
    }
    try {
      URI uri = URI.create(metadataLocation);
      String scheme = uri.getScheme();
      if (scheme == null) {
        return false;
      }
      return !SKIPPED_SCHEMES.contains(scheme.toLowerCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      LOG.infof(
          "Skipping metadata materialization because metadata-location %s is invalid",
          metadataLocation);
      return false;
    }
  }

  private TableMetadata parseMetadata(TableMetadataView metadata, String metadataLocation) {
    try {
      JsonNode node = mapper.valueToTree(metadata);
      return TableMetadataParser.fromJson(metadataLocation, node);
    } catch (RuntimeException e) {
      throw new MaterializeMetadataException("Unable to serialize Iceberg metadata", e);
    }
  }

  private void writeMetadata(FileIO fileIO, String location, TableMetadata metadata) {
    LOG.infof("Writing Iceberg metadata file %s", location == null ? "<null>" : location);
    TableMetadataParser.write(metadata, fileIO.newOutputFile(location));
    LOG.infof(
        "Successfully wrote Iceberg metadata file %s", location == null ? "<null>" : location);
  }
}
