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

package ai.floedb.floecat.gateway.iceberg.rest.services.table.materialization;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataException;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataResult;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableCommitMaterializationService {
  private static final Logger LOG = Logger.getLogger(TableCommitMaterializationService.class);

  @Inject MaterializeMetadataService materializeMetadataService;

  @Inject
  TableCommitMaterializationLocationSupport locationResolver =
      new TableCommitMaterializationLocationSupport();

  @Inject
  TableCommitMaterializationMetadataSupport metadataNormalizer =
      new TableCommitMaterializationMetadataSupport();

  public MaterializeMetadataResult materializeMetadata(
      String namespace,
      String table,
      Table tableRecord,
      TableMetadataView metadata,
      String metadataLocation) {
    if (metadata == null) {
      return MaterializeMetadataResult.failure(
          IcebergErrorResponses.validation("metadata is required for materialization"));
    }
    metadataLocation =
        locationResolver.resolveOutputMetadataLocation(
            namespace, table, tableRecord, metadata, metadataLocation);
    metadata =
        locationResolver.normalizeTableLocation(
            namespace, table, tableRecord, metadata, metadataLocation);
    metadata = metadataNormalizer.normalizeRequiredMetadata(metadata);
    boolean requestedLocation = hasText(metadataLocation) || hasText(metadata.metadataLocation());
    try {
      MaterializeMetadataService.MaterializeResult materializeResult =
          materializeMetadataService.materialize(namespace, table, metadata, metadataLocation);
      String resolvedLocation = materializeResult.metadataLocation();
      if (!hasText(resolvedLocation)) {
        if (!requestedLocation) {
          TableMetadataView resolvedMetadata =
              materializeResult.metadata() != null ? materializeResult.metadata() : metadata;
          return MaterializeMetadataResult.success(resolvedMetadata, resolvedLocation);
        }
        return MaterializeMetadataResult.failure(
            IcebergErrorResponses.failure(
                "metadata materialization returned empty metadata-location",
                "MaterializeMetadataException",
                Response.Status.INTERNAL_SERVER_ERROR));
      }
      TableMetadataView resolvedMetadata =
          materializeResult.metadata() != null ? materializeResult.metadata() : metadata;
      return MaterializeMetadataResult.success(resolvedMetadata, resolvedLocation);
    } catch (MaterializeMetadataException e) {
      LOG.warnf(
          e,
          "Failed to materialize Iceberg metadata for %s.%s to %s (serving original metadata)",
          namespace,
          table,
          metadataLocation);
      return MaterializeMetadataResult.failure(
          IcebergErrorResponses.failure(
              "metadata materialization failed",
              "MaterializeMetadataException",
              Response.Status.INTERNAL_SERVER_ERROR));
    }
  }

  private static boolean hasText(String value) {
    return value != null && !value.isBlank();
  }
}
