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

package ai.floedb.floecat.gateway.iceberg.rest.table.transaction;

import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.firstNonBlank;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.support.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.table.IcebergMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.table.TablePropertyService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.jboss.logging.Logger;

@ApplicationScoped
public class MetadataMaterializer {
  private static final Logger LOG = Logger.getLogger(MetadataMaterializer.class);

  @Inject IcebergMetadataService icebergMetadataService;
  @Inject TablePropertyService tablePropertyService;

  public record Result(
      ai.floedb.floecat.catalog.rpc.Table table, TableMetadata tableMetadata, Response error) {}

  public Result preMaterialize(
      String namespace,
      String tableName,
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.Table plannedTable,
      ParsedCommit commit,
      CurrentTableState currentState,
      TableGatewaySupport tableSupport,
      boolean preMaterializeAssertCreate,
      boolean hadCommittedSnapshot) {
    if (plannedTable == null) {
      return new Result(plannedTable, null, null);
    }
    boolean assertCreateRequested =
        CommitPlanningPredicates.requiresAssertCreate(commit.requirements());
    boolean firstWriteAssertCreate =
        assertCreateRequested
            && (commit.parsed().containsSnapshotUpdates()
                || commit.parsed().containsCreateInitializationActions());
    boolean firstWriteCreate =
        firstWriteAssertCreate
            || (commit.parsed().containsCreateInitializationActions() && !hadCommittedSnapshot);
    if (!preMaterializeAssertCreate && assertCreateRequested) {
      return new Result(plannedTable, null, null);
    }
    if (icebergMetadataService == null) {
      return new Result(plannedTable, null, null);
    }

    TableMetadata baseMetadata =
        resolveBaseMetadata(
            tableName, tableId, plannedTable, commit, tableSupport, firstWriteCreate);
    if (baseMetadata == null) {
      return new Result(
          plannedTable,
          null,
          IcebergErrorResponses.failure(
              "failed to load committed table metadata",
              "MetadataResolutionException",
              Response.Status.INTERNAL_SERVER_ERROR));
    }
    String baseMetadataLocation = MetadataLocationUtil.metadataLocation(baseMetadata);
    String authoritativeMetadataLocation = commit.authoritativeMetadataLocation(currentState);

    TableMetadata canonicalMetadata =
        icebergMetadataService.applyCommitUpdates(
            baseMetadata, plannedTable, commit.toCommitRequest());
    String canonicalMetadataLocation =
        firstNonBlank(
            authoritativeMetadataLocation,
            MetadataLocationUtil.metadataLocation(canonicalMetadata),
            baseMetadataLocation);
    if (canonicalMetadata != null
        && canonicalMetadataLocation != null
        && !canonicalMetadataLocation.isBlank()) {
      canonicalMetadata =
          TableMetadata.buildFrom(canonicalMetadata)
              .discardChanges()
              .withMetadataLocation(canonicalMetadataLocation)
              .build();
    }
    if (canonicalMetadata == null) {
      return new Result(plannedTable, null, null);
    }

    ai.floedb.floecat.catalog.rpc.Table canonicalizedTable =
        tablePropertyService.applyCanonicalMetadataProperties(plannedTable, canonicalMetadata);
    boolean importedRegisterCommit =
        commit.parsed().requestedMetadataLocation() != null
            && !commit.parsed().requestedMetadataLocation().isBlank()
            && commit.parsed().containsSnapshotUpdates()
            && !commit.parsed().containsCreateInitializationActions();
    if (importedRegisterCommit) {
      return new Result(canonicalizedTable, canonicalMetadata, null);
    }

    IcebergMetadataService.MaterializeResult materialized;
    Map<String, String> requestFileIoProperties =
        tableSupport == null ? Map.of() : tableSupport.defaultFileIoProperties();
    try {
      materialized =
          firstWriteCreate
              ? icebergMetadataService.materializeAtExactLocation(
                  namespace,
                  tableName,
                  canonicalMetadata,
                  canonicalMetadata.metadataFileLocation(),
                  requestFileIoProperties)
              : icebergMetadataService.materializeNextVersion(
                  namespace, tableName, canonicalMetadata, requestFileIoProperties);
    } catch (IllegalArgumentException e) {
      return new Result(
          plannedTable,
          null,
          IcebergErrorResponses.failure(
              "metadata materialization failed",
              "MaterializeMetadataException",
              Response.Status.INTERNAL_SERVER_ERROR));
    }

    TableMetadata committedMetadata =
        materialized == null || materialized.tableMetadata() == null
            ? canonicalMetadata
            : materialized.tableMetadata();
    ai.floedb.floecat.catalog.rpc.Table materializedTable =
        tablePropertyService.applyCanonicalMetadataProperties(
            canonicalizedTable, committedMetadata);
    String metadataLocation = materialized == null ? null : materialized.metadataLocation();
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      Map<String, String> props = new LinkedHashMap<>(materializedTable.getPropertiesMap());
      props.put("metadata-location", metadataLocation);
      materializedTable =
          materializedTable.toBuilder().clearProperties().putAllProperties(props).build();
    }
    return new Result(materializedTable, committedMetadata, null);
  }

  private TableMetadata resolveBaseMetadata(
      String tableName,
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.Table plannedTable,
      ParsedCommit commit,
      TableGatewaySupport tableSupport,
      boolean firstWriteCreate) {
    if (firstWriteCreate) {
      try {
        var resolved =
            icebergMetadataService.resolveMetadata(
                tableName,
                plannedTable,
                null,
                tableSupport == null ? Map.of() : tableSupport.defaultFileIoProperties(),
                List::of);
        if (resolved != null && resolved.tableMetadata() != null) {
          return resolved.tableMetadata();
        }
      } catch (RuntimeException e) {
        LOG.debugf(
            e,
            "Unable to load staged or imported base metadata during create commit tableId=%s table=%s",
            tableId == null ? "<missing>" : tableId.getId(),
            tableName);
      }
      return icebergMetadataService.bootstrapTableMetadataFromCommit(
          plannedTable, commit.toCommitRequest());
    }
    try {
      var resolved =
          icebergMetadataService.resolveMetadata(
              tableName,
              plannedTable,
              null,
              tableSupport == null ? Map.of() : tableSupport.defaultFileIoProperties(),
              List::of);
      if (resolved != null && resolved.tableMetadata() != null) {
        return resolved.tableMetadata();
      }
    } catch (RuntimeException e) {
      LOG.warnf(
          e,
          "Failed to load committed base metadata during pre-materialization tableId=%s table=%s",
          tableId == null ? "<missing>" : tableId.getId(),
          tableName);
      return null;
    }
    return null;
  }
}
