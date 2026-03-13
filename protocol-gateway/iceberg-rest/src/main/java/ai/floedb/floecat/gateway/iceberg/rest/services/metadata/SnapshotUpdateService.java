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

import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asInteger;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asIntegerList;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asLong;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asLongList;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asObjectMap;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asObjectMapList;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asString;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asStringMap;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.firstNonNull;

import ai.floedb.floecat.catalog.rpc.DeleteSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.PartitionField;
import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergBlobMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergEncryptedKey;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergPartitionStatisticsFile;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSchema;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSortField;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSortOrder;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergStatisticsFile;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.Base64;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class SnapshotUpdateService {

  @Inject ObjectMapper mapper;
  @Inject GrpcServiceFacade snapshotClient;

  public Response validateSnapshotUpdates(List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return null;
    }
    for (Map<String, Object> update : updates) {
      if (update == null) {
        return validationError("commit update entry cannot be null");
      }
      String action = CommitUpdateInspector.actionOf(update);
      if (action == null || action.isBlank()) {
        return validationError("commit update missing action");
      }
      if (CommitUpdateInspector.ACTION_ADD_SNAPSHOT.equals(action)) {
        @SuppressWarnings("unchecked")
        Map<String, Object> snapshot =
            update.get("snapshot") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
        if (snapshot == null || snapshot.isEmpty()) {
          return validationError("add-snapshot requires snapshot");
        }
        Long snapshotId = asLong(snapshot.get("snapshot-id"));
        if (snapshotId == null) {
          return validationError("add-snapshot requires snapshot-id");
        }
      } else if (CommitUpdateInspector.ACTION_REMOVE_SNAPSHOTS.equals(action)) {
        List<Long> ids = asLongList(update.get("snapshot-ids"));
        if (ids.isEmpty()) {
          return validationError("remove-snapshots requires snapshot-ids");
        }
      } else if (CommitUpdateInspector.ACTION_SET_SNAPSHOT_REF.equals(action)) {
        String refName = asString(update.get("ref-name"));
        if (refName == null || refName.isBlank()) {
          return validationError("set-snapshot-ref requires ref-name");
        }
        String type = asString(update.get("type"));
        if (type == null || type.isBlank()) {
          return validationError("set-snapshot-ref requires type");
        }
        Long pointedSnapshot = asLong(update.get("snapshot-id"));
        if (pointedSnapshot == null) {
          return validationError("set-snapshot-ref requires snapshot-id");
        }
      } else if (CommitUpdateInspector.ACTION_REMOVE_SNAPSHOT_REF.equals(action)) {
        String ref = asString(update.get("ref-name"));
        if (ref == null || ref.isBlank()) {
          return validationError("remove-snapshot-ref requires ref-name");
        }
      } else if (CommitUpdateInspector.ACTION_ASSIGN_UUID.equals(action)) {
        String uuid = asString(update.get("uuid"));
        if (uuid == null || uuid.isBlank()) {
          return validationError("assign-uuid requires uuid");
        }
      } else if (CommitUpdateInspector.ACTION_UPGRADE_FORMAT_VERSION.equals(action)) {
        Integer version = asInteger(update.get("format-version"));
        if (version == null) {
          return validationError("upgrade-format-version requires format-version");
        }
      } else if (CommitUpdateInspector.ACTION_ADD_SCHEMA.equals(action)) {
        Map<String, Object> schemaMap = asObjectMap(update.get("schema"));
        if (schemaMap == null || schemaMap.isEmpty()) {
          return validationError("add-schema requires schema");
        }
        try {
          buildIcebergSchema(schemaMap, update);
        } catch (IllegalArgumentException | JsonProcessingException e) {
          return validationError(e.getMessage());
        }
      } else if (CommitUpdateInspector.ACTION_SET_CURRENT_SCHEMA.equals(action)) {
        Integer schemaId = asInteger(update.get("schema-id"));
        if (schemaId == null) {
          return validationError("set-current-schema requires schema-id");
        }
      } else if (CommitUpdateInspector.ACTION_ADD_SPEC.equals(action)) {
        Map<String, Object> specMap = asObjectMap(update.get("spec"));
        if (specMap == null || specMap.isEmpty()) {
          return validationError("add-spec requires spec");
        }
        try {
          buildPartitionSpec(specMap);
        } catch (IllegalArgumentException e) {
          return validationError(e.getMessage());
        }
      } else if (CommitUpdateInspector.ACTION_SET_DEFAULT_SPEC.equals(action)) {
        Integer specId = asInteger(update.get("spec-id"));
        if (specId == null) {
          return validationError("set-default-spec requires spec-id");
        }
      } else if (CommitUpdateInspector.ACTION_REMOVE_PARTITION_SPECS.equals(action)) {
        List<Integer> specIds = asIntegerList(update.get("spec-ids"));
        if (specIds.isEmpty()) {
          return validationError("remove-partition-specs requires spec-ids");
        }
      } else if (CommitUpdateInspector.ACTION_ADD_SORT_ORDER.equals(action)) {
        Map<String, Object> orderMap = asObjectMap(update.get("sort-order"));
        if (orderMap == null || orderMap.isEmpty()) {
          return validationError("add-sort-order requires sort-order");
        }
        try {
          buildSortOrder(orderMap);
        } catch (IllegalArgumentException e) {
          return validationError(e.getMessage());
        }
      } else if (CommitUpdateInspector.ACTION_SET_DEFAULT_SORT_ORDER.equals(action)) {
        Integer orderId = asInteger(update.get("sort-order-id"));
        if (orderId == null) {
          return validationError("set-default-sort-order requires sort-order-id");
        }
      } else if (CommitUpdateInspector.ACTION_SET_LOCATION.equals(action)) {
        String location = asString(update.get("location"));
        if (location == null || location.isBlank()) {
          return validationError("set-location requires location");
        }
      } else if (CommitUpdateInspector.ACTION_SET_PROPERTIES.equals(action)) {
        @SuppressWarnings("unchecked")
        Map<String, Object> props =
            update.get("updates") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
        if (props == null || props.isEmpty()) {
          return validationError("set-properties requires updates");
        }
      } else if (CommitUpdateInspector.ACTION_REMOVE_PROPERTIES.equals(action)) {
        @SuppressWarnings("unchecked")
        List<Object> removals =
            update.get("removals") instanceof List<?> l ? (List<Object>) l : null;
        if (removals == null || removals.isEmpty()) {
          return validationError("remove-properties requires removals");
        }
      } else if (CommitUpdateInspector.ACTION_SET_STATISTICS.equals(action)) {
        Map<String, Object> statsMap = asObjectMap(update.get("statistics"));
        if (statsMap == null || statsMap.isEmpty()) {
          return validationError("set-statistics requires statistics");
        }
        try {
          buildStatisticsFile(statsMap);
        } catch (IllegalArgumentException e) {
          return validationError(e.getMessage());
        }
      } else if (CommitUpdateInspector.ACTION_REMOVE_STATISTICS.equals(action)) {
        Long snapId = asLong(update.get("snapshot-id"));
        if (snapId == null) {
          return validationError("remove-statistics requires snapshot-id");
        }
      } else if (CommitUpdateInspector.ACTION_SET_PARTITION_STATISTICS.equals(action)) {
        Map<String, Object> statsMap = asObjectMap(update.get("partition-statistics"));
        if (statsMap == null || statsMap.isEmpty()) {
          return validationError("set-partition-statistics requires partition-statistics");
        }
        try {
          buildPartitionStatisticsFile(statsMap);
        } catch (IllegalArgumentException e) {
          return validationError(e.getMessage());
        }
      } else if (CommitUpdateInspector.ACTION_REMOVE_PARTITION_STATISTICS.equals(action)) {
        Long snapId = asLong(update.get("snapshot-id"));
        if (snapId == null) {
          return validationError("remove-partition-statistics requires snapshot-id");
        }
      } else if (CommitUpdateInspector.ACTION_ADD_ENCRYPTION_KEY.equals(action)) {
        Map<String, Object> keyMap = asObjectMap(update.get("encryption-key"));
        if (keyMap == null || keyMap.isEmpty()) {
          return validationError("add-encryption-key requires encryption-key");
        }
        try {
          buildEncryptionKey(keyMap);
        } catch (IllegalArgumentException e) {
          return validationError(e.getMessage());
        }
      } else if (CommitUpdateInspector.ACTION_REMOVE_ENCRYPTION_KEY.equals(action)) {
        String keyId = asString(update.get("key-id"));
        if (keyId == null || keyId.isBlank()) {
          return validationError("remove-encryption-key requires key-id");
        }
      } else if (CommitUpdateInspector.ACTION_REMOVE_SCHEMAS.equals(action)) {
        List<Integer> schemaIds = asIntegerList(update.get("schema-ids"));
        if (schemaIds.isEmpty()) {
          return validationError("remove-schemas requires schema-ids");
        }
      } else {
        return validationError("unsupported commit update action: " + action);
      }
    }
    return null;
  }

  public void deleteSnapshots(ResourceId tableId, List<Long> snapshotIds) {
    for (Long id : snapshotIds) {
      if (id == null) {
        continue;
      }
      try {
        snapshotClient.deleteSnapshot(
            DeleteSnapshotRequest.newBuilder().setTableId(tableId).setSnapshotId(id).build());
      } catch (StatusRuntimeException e) {
        if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
          continue;
        }
        throw e;
      }
    }
  }

  private IcebergSchema buildIcebergSchema(
      Map<String, Object> schemaMap, Map<String, Object> update) throws JsonProcessingException {
    Integer schemaId = asInteger(schemaMap.get("schema-id"));
    if (schemaId == null) {
      throw new IllegalArgumentException("add-schema requires schema.schema-id");
    }
    String schemaJson = mapper.writeValueAsString(schemaMap);
    IcebergSchema.Builder builder =
        IcebergSchema.newBuilder().setSchemaId(schemaId).setSchemaJson(schemaJson);
    Integer lastColumnId =
        asInteger(firstNonNull(update.get("last-column-id"), schemaMap.get("last-column-id")));
    if (lastColumnId != null) {
      builder.setLastColumnId(lastColumnId);
    }
    List<Integer> identifierIds = asIntegerList(schemaMap.get("identifier-field-ids"));
    if (!identifierIds.isEmpty()) {
      builder.addAllIdentifierFieldIds(identifierIds);
    }
    return builder.build();
  }

  private PartitionSpecInfo buildPartitionSpec(Map<String, Object> specMap) {
    Integer specId = asInteger(specMap.get("spec-id"));
    if (specId == null) {
      throw new IllegalArgumentException("add-spec requires spec.spec-id");
    }
    PartitionSpecInfo.Builder builder = PartitionSpecInfo.newBuilder().setSpecId(specId);
    String specName = asString(specMap.get("name"));
    builder.setSpecName(specName == null || specName.isBlank() ? "spec-" + specId : specName);
    List<Map<String, Object>> fields = asObjectMapList(specMap.get("fields"));
    for (Map<String, Object> field : fields) {
      String name = asString(field.get("name"));
      Integer fieldId = asInteger(firstNonNull(field.get("field-id"), field.get("source-id")));
      String transform = asString(field.get("transform"));
      if (name == null || fieldId == null || transform == null || transform.isBlank()) {
        throw new IllegalArgumentException(
            "add-spec fields require name, field-id/source-id, and transform");
      }
      builder.addFields(
          PartitionField.newBuilder()
              .setFieldId(fieldId)
              .setName(name)
              .setTransform(transform)
              .build());
    }
    return builder.build();
  }

  private IcebergSortOrder buildSortOrder(Map<String, Object> orderMap) {
    Integer orderId =
        asInteger(firstNonNull(orderMap.get("sort-order-id"), orderMap.get("order-id")));
    if (orderId == null) {
      throw new IllegalArgumentException("add-sort-order requires sort-order.sort-order-id");
    }
    IcebergSortOrder.Builder builder = IcebergSortOrder.newBuilder().setSortOrderId(orderId);
    List<Map<String, Object>> fields = asObjectMapList(orderMap.get("fields"));
    for (Map<String, Object> field : fields) {
      Integer sourceId = asInteger(field.get("source-id"));
      if (sourceId == null) {
        throw new IllegalArgumentException("sort-order.fields require source-id");
      }
      String transform = asString(field.get("transform"));
      if (transform == null || transform.isBlank()) {
        transform = "identity";
      }
      String direction = asString(field.get("direction"));
      if (direction == null || direction.isBlank()) {
        direction = "ASC";
      }
      String nullOrder = asString(field.get("null-order"));
      if (nullOrder == null || nullOrder.isBlank()) {
        nullOrder = "nulls-first";
      }
      builder.addFields(
          IcebergSortField.newBuilder()
              .setSourceFieldId(sourceId)
              .setTransform(transform)
              .setDirection(direction)
              .setNullOrder(nullOrder)
              .build());
    }
    return builder.build();
  }

  private IcebergStatisticsFile buildStatisticsFile(Map<String, Object> statsMap) {
    Long snapshotId = asLong(statsMap.get("snapshot-id"));
    String path = asString(statsMap.get("statistics-path"));
    Long size = asLong(statsMap.get("file-size-in-bytes"));
    Long footerSize = asLong(statsMap.get("file-footer-size-in-bytes"));
    List<Map<String, Object>> blobMaps = asObjectMapList(statsMap.get("blob-metadata"));
    if (snapshotId == null || path == null || path.isBlank() || size == null) {
      throw new IllegalArgumentException(
          "set-statistics requires snapshot-id, statistics-path, and file-size-in-bytes");
    }
    IcebergStatisticsFile.Builder builder =
        IcebergStatisticsFile.newBuilder()
            .setSnapshotId(snapshotId)
            .setStatisticsPath(path)
            .setFileSizeInBytes(size);
    if (footerSize != null) {
      builder.setFileFooterSizeInBytes(footerSize);
    }
    if (!blobMaps.isEmpty()) {
      for (Map<String, Object> blobMap : blobMaps) {
        builder.addBlobMetadata(buildBlobMetadata(blobMap));
      }
    }
    return builder.build();
  }

  private IcebergBlobMetadata buildBlobMetadata(Map<String, Object> blobMap) {
    String type = asString(blobMap.get("type"));
    Long snapshotId = asLong(blobMap.get("snapshot-id"));
    Long sequenceNumber = asLong(blobMap.get("sequence-number"));
    List<Integer> fields = asIntegerList(blobMap.get("fields"));
    if (type == null
        || type.isBlank()
        || snapshotId == null
        || sequenceNumber == null
        || fields.isEmpty()) {
      throw new IllegalArgumentException(
          "statistics blob-metadata requires type, snapshot-id, sequence-number, and fields");
    }
    IcebergBlobMetadata.Builder builder =
        IcebergBlobMetadata.newBuilder()
            .setType(type)
            .setSnapshotId(snapshotId)
            .setSequenceNumber(sequenceNumber)
            .addAllFields(fields);
    Map<String, String> props = asStringMap(blobMap.get("properties"));
    if (!props.isEmpty()) {
      builder.putAllProperties(props);
    }
    return builder.build();
  }

  private IcebergPartitionStatisticsFile buildPartitionStatisticsFile(
      Map<String, Object> statsMap) {
    Long snapshotId = asLong(statsMap.get("snapshot-id"));
    String path = asString(statsMap.get("statistics-path"));
    Long size = asLong(statsMap.get("file-size-in-bytes"));
    if (snapshotId == null || path == null || path.isBlank() || size == null) {
      throw new IllegalArgumentException(
          "set-partition-statistics requires snapshot-id, statistics-path, and file-size-in-bytes");
    }
    return IcebergPartitionStatisticsFile.newBuilder()
        .setSnapshotId(snapshotId)
        .setStatisticsPath(path)
        .setFileSizeInBytes(size)
        .build();
  }

  private IcebergEncryptedKey buildEncryptionKey(Map<String, Object> keyMap) {
    String keyId = asString(keyMap.get("key-id"));
    String metadataB64 = asString(keyMap.get("encrypted-key-metadata"));
    if (keyId == null || keyId.isBlank() || metadataB64 == null || metadataB64.isBlank()) {
      throw new IllegalArgumentException(
          "add-encryption-key requires key-id and encrypted-key-metadata");
    }
    byte[] decoded;
    try {
      decoded = Base64.getDecoder().decode(metadataB64);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("encrypted-key-metadata must be valid base64");
    }
    IcebergEncryptedKey.Builder builder =
        IcebergEncryptedKey.newBuilder()
            .setKeyId(keyId)
            .setEncryptedKeyMetadata(ByteString.copyFrom(decoded));
    String encryptedBy = asString(keyMap.get("encrypted-by-id"));
    if (encryptedBy != null && !encryptedBy.isBlank()) {
      builder.setEncryptedById(encryptedBy);
    }
    return builder.build();
  }

  private Response validationError(String message) {
    return IcebergErrorResponses.validation(message);
  }
}
