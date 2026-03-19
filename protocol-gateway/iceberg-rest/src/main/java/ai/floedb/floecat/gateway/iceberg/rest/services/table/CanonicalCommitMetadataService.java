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

import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asString;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.CanonicalTableMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataException;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService.ResolvedMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotRefParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadata.Builder;
import org.jboss.logging.Logger;

@ApplicationScoped
public class CanonicalCommitMetadataService {
  private static final Logger LOG = Logger.getLogger(CanonicalCommitMetadataService.class);

  @Inject TableMetadataImportService tableMetadataImportService;
  @Inject CanonicalTableMetadataService canonicalTableMetadataService;
  @Inject GrpcServiceFacade snapshotClient;
  @Inject ObjectMapper mapper;

  public record CanonicalCommitMetadata(TableMetadata tableMetadata) {}

  public CanonicalCommitMetadata applyCommitUpdates(
      String tableName,
      ResourceId tableId,
      Table table,
      IcebergMetadata metadata,
      TableRequests.Commit request,
      TableGatewaySupport tableSupport) {
    if (table == null || request == null || tableSupport == null) {
      return null;
    }
    BaseMetadata base = baseMetadata(tableName, tableId, table, metadata, tableSupport);
    if (base == null || base.tableMetadata() == null) {
      return null;
    }
    TableMetadata canonical = applyCommitUpdates(base.tableMetadata(), table, request);
    if (canonical == null) {
      return null;
    }
    String metadataLocation =
        metadataLocation(
            canonical.metadataFileLocation(),
            table.getPropertiesMap().get("metadata-location"),
            base.metadataLocation());
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      canonical =
          TableMetadata.buildFrom(canonical)
              .discardChanges()
              .withMetadataLocation(metadataLocation)
              .build();
    }
    return new CanonicalCommitMetadata(canonical);
  }

  private record BaseMetadata(TableMetadata tableMetadata, String metadataLocation) {}

  private BaseMetadata baseMetadata(
      String tableName,
      ResourceId tableId,
      Table table,
      IcebergMetadata metadata,
      TableGatewaySupport tableSupport) {
    ResolvedMetadata resolved =
        tableMetadataImportService.resolveMetadata(
            tableName,
            table,
            metadata,
            tableSupport.defaultFileIoProperties(),
            () ->
                metadata == null
                    ? List.of()
                    : SnapshotLister.fetchSnapshots(
                        snapshotClient, tableId, SnapshotLister.Mode.ALL, metadata));
    if (resolved.tableMetadata() != null) {
      return new BaseMetadata(
          resolved.tableMetadata(), resolved.tableMetadata().metadataFileLocation());
    }
    LOG.debugf(
        "Falling back to catalog metadata bootstrap tableId=%s table=%s",
        tableId == null ? "<missing>" : tableId.getId(), tableName);
    TableMetadataView baseView = resolved.metadataView();
    if (baseView == null) {
      return null;
    }
    String metadataLocation =
        metadataLocation(
            baseView.metadataLocation(), table.getPropertiesMap().get("metadata-location"), null);
    try {
      return new BaseMetadata(
          canonicalTableMetadataService.toTableMetadata(baseView, metadataLocation),
          metadataLocation);
    } catch (MaterializeMetadataException | IllegalArgumentException e) {
      LOG.debugf(
          e,
          "Skipping canonical bootstrap for tableId=%s table=%s due to incomplete bootstrap metadata",
          tableId == null ? "<missing>" : tableId.getId(),
          tableName);
      return null;
    }
  }

  TableMetadata applyCommitUpdates(
      TableMetadata metadata, Table table, TableRequests.Commit request) {
    if (metadata == null
        || request == null
        || request.updates() == null
        || request.updates().isEmpty()) {
      return metadata;
    }
    CommitUpdateInspector.Parsed parsed = CommitUpdateInspector.inspect(request);
    Builder builder = TableMetadata.buildFrom(metadata).discardChanges();
    String metadataLocation =
        metadataLocation(
            metadata.metadataFileLocation(),
            table.getPropertiesMap().get("metadata-location"),
            null);
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      builder.withMetadataLocation(metadataLocation);
    }
    Map<String, String> mergedProperties = new LinkedHashMap<>(metadata.properties());
    mergedProperties.putAll(table.getPropertiesMap());
    builder.setProperties(mergedProperties);

    Map<Integer, Schema> schemasById = new LinkedHashMap<>(metadata.schemasById());
    Map<Long, org.apache.iceberg.Snapshot> snapshotsById = new LinkedHashMap<>();
    for (org.apache.iceberg.Snapshot snapshot : metadata.snapshots()) {
      if (snapshot != null) {
        snapshotsById.put(snapshot.snapshotId(), snapshot);
      }
    }
    int currentFormatVersion = metadata.formatVersion();
    long lastSequenceNumber = Math.max(0L, metadata.lastSequenceNumber());
    Integer currentSchemaId = metadata.currentSchemaId();
    Integer lastAddedSchemaId = null;
    Integer lastAddedSpecId = null;
    Integer lastAddedSortOrderId = null;

    for (Map<String, Object> update : request.updates()) {
      if (update == null) {
        continue;
      }
      CommitUpdateInspector.UpdateAction action = CommitUpdateInspector.actionTypeOf(update);
      if (action == null) {
        continue;
      }
      switch (action) {
        case UPGRADE_FORMAT_VERSION -> {
          Integer requested = asInteger(update.get("format-version"));
          if (requested != null) {
            builder.upgradeFormatVersion(requested);
            currentFormatVersion = requested;
          }
        }
        case SET_LOCATION -> {
          String requestedLocation = asString(update.get("location"));
          if (requestedLocation != null && !requestedLocation.isBlank()) {
            builder.setLocation(requestedLocation);
          }
        }
        case ADD_SCHEMA -> {
          @SuppressWarnings("unchecked")
          Map<String, Object> schemaMap =
              update.get("schema") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
          Schema schema = parseSchema(schemaMap);
          if (schema != null) {
            builder.addSchema(schema);
            schemasById.put(schema.schemaId(), schema);
            if (schema.schemaId() >= 0) {
              lastAddedSchemaId = schema.schemaId();
            }
          }
        }
        case SET_CURRENT_SCHEMA -> {
          Integer schemaId =
              resolveLastAddedId(asInteger(update.get("schema-id")), lastAddedSchemaId);
          if (schemaId != null) {
            builder.setCurrentSchema(schemaId);
            currentSchemaId = schemaId;
          }
        }
        case ADD_SPEC -> {
          @SuppressWarnings("unchecked")
          Map<String, Object> specMap =
              update.get("spec") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
          PartitionSpec spec =
              parsePartitionSpec(
                  specMap, activeSchema(schemasById, currentSchemaId, lastAddedSchemaId));
          if (spec != null) {
            builder.addPartitionSpec(spec);
            if (spec.specId() >= 0) {
              lastAddedSpecId = spec.specId();
            }
          }
        }
        case SET_DEFAULT_SPEC -> {
          Integer specId = resolveLastAddedId(asInteger(update.get("spec-id")), lastAddedSpecId);
          if (specId != null) {
            builder.setDefaultPartitionSpec(specId);
          }
        }
        case ADD_SORT_ORDER -> {
          @SuppressWarnings("unchecked")
          Map<String, Object> sortOrderMap =
              update.get("sort-order") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
          SortOrder sortOrder =
              parseSortOrder(
                  sortOrderMap, activeSchema(schemasById, currentSchemaId, lastAddedSchemaId));
          if (sortOrder != null) {
            builder.addSortOrder(sortOrder);
            if (sortOrder.orderId() >= 0) {
              lastAddedSortOrderId = sortOrder.orderId();
            }
          }
        }
        case SET_DEFAULT_SORT_ORDER -> {
          Integer sortOrderId =
              resolveLastAddedId(asInteger(update.get("sort-order-id")), lastAddedSortOrderId);
          if (sortOrderId != null) {
            builder.setDefaultSortOrder(sortOrderId);
          }
        }
        case ADD_SNAPSHOT -> {
          @SuppressWarnings("unchecked")
          Map<String, Object> snapshotMap =
              update.get("snapshot") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
          org.apache.iceberg.Snapshot snapshot =
              parseSnapshot(snapshotMap, currentFormatVersion, lastSequenceNumber);
          if (snapshot != null) {
            if (snapshotsById.containsKey(snapshot.snapshotId())) {
              org.apache.iceberg.Snapshot existing = snapshotsById.get(snapshot.snapshotId());
              if (existing != null) {
                lastSequenceNumber = Math.max(lastSequenceNumber, existing.sequenceNumber());
              }
              break;
            }
            builder.addSnapshot(snapshot);
            snapshotsById.put(snapshot.snapshotId(), snapshot);
            lastSequenceNumber = Math.max(lastSequenceNumber, snapshot.sequenceNumber());
          }
        }
        default -> {
          // handled below or ignored
        }
      }
    }

    if (!parsed.removedSnapshotIds().isEmpty()) {
      builder.removeSnapshots(parsed.removedSnapshotIds());
      parsed.removedSnapshotIds().forEach(snapshotsById::remove);
    }
    for (CommitUpdateInspector.SnapshotRefMutation mutation : parsed.snapshotRefMutations()) {
      if (mutation == null || mutation.refName() == null || mutation.refName().isBlank()) {
        continue;
      }
      if (mutation.remove()) {
        builder.removeRef(mutation.refName());
        continue;
      }
      if (!hasKnownSnapshot(snapshotsById, mutation.snapshotId())) {
        LOG.debugf(
            "Skipping pre-commit snapshot ref mutation ref=%s snapshotId=%s because snapshot is not yet present in canonical metadata",
            mutation.refName(), mutation.snapshotId());
        continue;
      }
      SnapshotRef ref = parseSnapshotRef(mutation);
      if (ref != null) {
        builder.setRef(mutation.refName(), ref);
      }
    }
    return builder.build();
  }

  private String metadataLocation(String first, String second, String third) {
    if (first != null && !first.isBlank()) {
      return first;
    }
    if (second != null && !second.isBlank()) {
      return second;
    }
    if (third != null && !third.isBlank()) {
      return third;
    }
    return null;
  }

  private Schema activeSchema(
      Map<Integer, Schema> schemasById, Integer currentSchemaId, Integer lastAddedSchemaId) {
    if (currentSchemaId != null && schemasById.containsKey(currentSchemaId)) {
      return schemasById.get(currentSchemaId);
    }
    if (lastAddedSchemaId != null && schemasById.containsKey(lastAddedSchemaId)) {
      return schemasById.get(lastAddedSchemaId);
    }
    return schemasById.values().stream().findFirst().orElse(null);
  }

  private Schema parseSchema(Map<String, Object> schemaMap) {
    if (schemaMap == null || schemaMap.isEmpty()) {
      return null;
    }
    try {
      return SchemaParser.fromJson(mapper.writeValueAsString(schemaMap));
    } catch (Exception e) {
      LOG.debugf(e, "Failed to parse schema update");
      return null;
    }
  }

  private PartitionSpec parsePartitionSpec(Map<String, Object> specMap, Schema schema) {
    if (specMap == null || specMap.isEmpty() || schema == null) {
      return null;
    }
    try {
      return PartitionSpecParser.fromJson(schema, mapper.writeValueAsString(specMap));
    } catch (Exception e) {
      LOG.debugf(e, "Failed to parse partition spec update");
      return null;
    }
  }

  private SortOrder parseSortOrder(Map<String, Object> sortOrderMap, Schema schema) {
    if (sortOrderMap == null || sortOrderMap.isEmpty() || schema == null) {
      return null;
    }
    try {
      return SortOrderParser.fromJson(schema, mapper.writeValueAsString(sortOrderMap));
    } catch (Exception e) {
      LOG.debugf(e, "Failed to parse sort order update");
      return null;
    }
  }

  private SnapshotRef parseSnapshotRef(CommitUpdateInspector.SnapshotRefMutation mutation) {
    try {
      Map<String, Object> ref = new LinkedHashMap<>();
      ref.put("snapshot-id", mutation.snapshotId());
      ref.put("type", mutation.type());
      if (mutation.maxRefAgeMs() != null) {
        ref.put("max-ref-age-ms", mutation.maxRefAgeMs());
      }
      if (mutation.maxSnapshotAgeMs() != null) {
        ref.put("max-snapshot-age-ms", mutation.maxSnapshotAgeMs());
      }
      if (mutation.minSnapshotsToKeep() != null) {
        ref.put("min-snapshots-to-keep", mutation.minSnapshotsToKeep());
      }
      return SnapshotRefParser.fromJson(mapper.writeValueAsString(ref));
    } catch (Exception e) {
      LOG.debugf(e, "Failed to parse snapshot ref mutation");
      return null;
    }
  }

  private org.apache.iceberg.Snapshot parseSnapshot(
      Map<String, Object> snapshotMap, int formatVersion, long lastSequenceNumber) {
    if (snapshotMap == null || snapshotMap.isEmpty()) {
      return null;
    }
    try {
      Map<String, Object> normalized = new LinkedHashMap<>(snapshotMap);
      normalized.remove("schema-json");
      if (!normalized.containsKey("sequence-number")) {
        long nextSequence = formatVersion >= 2 ? Math.max(0L, lastSequenceNumber) + 1L : 0L;
        normalized.put("sequence-number", nextSequence);
      }
      return SnapshotParser.fromJson(mapper.writeValueAsString(normalized));
    } catch (Exception e) {
      LOG.debugf(e, "Failed to parse snapshot update");
      return null;
    }
  }

  private Integer resolveLastAddedId(Integer requested, Integer lastAdded) {
    if (requested == null) {
      return null;
    }
    if (requested == -1) {
      return lastAdded;
    }
    return requested;
  }

  private Integer asInteger(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number.intValue();
    }
    String text = value.toString();
    if (text.isBlank()) {
      return null;
    }
    try {
      return Integer.parseInt(text);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private boolean hasKnownSnapshot(
      Map<Long, org.apache.iceberg.Snapshot> snapshotsById, Long snapshotId) {
    if (snapshotsById == null || snapshotsById.isEmpty() || snapshotId == null || snapshotId <= 0) {
      return false;
    }
    return snapshotsById.containsKey(snapshotId);
  }
}
