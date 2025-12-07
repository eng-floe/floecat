package ai.floedb.metacat.gateway.iceberg.rest.services.catalog;

import ai.floedb.metacat.catalog.rpc.CreateSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.DeleteSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.PartitionField;
import ai.floedb.metacat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.catalog.rpc.SnapshotSpec;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.UpdateSnapshotRequest;
import ai.floedb.metacat.common.rpc.IdempotencyKey;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.common.rpc.SpecialSnapshot;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.metacat.gateway.iceberg.rpc.IcebergBlobMetadata;
import ai.floedb.metacat.gateway.iceberg.rpc.IcebergEncryptedKey;
import ai.floedb.metacat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.metacat.gateway.iceberg.rpc.IcebergPartitionStatisticsFile;
import ai.floedb.metacat.gateway.iceberg.rpc.IcebergRef;
import ai.floedb.metacat.gateway.iceberg.rpc.IcebergSchema;
import ai.floedb.metacat.gateway.iceberg.rpc.IcebergSortField;
import ai.floedb.metacat.gateway.iceberg.rpc.IcebergSortOrder;
import ai.floedb.metacat.gateway.iceberg.rpc.IcebergStatisticsFile;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.FieldMask;
import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

@ApplicationScoped
public class SnapshotMetadataService {

  @Inject GrpcWithHeaders grpc;
  @Inject ObjectMapper mapper;

  public List<Map<String, Object>> snapshotAdditions(List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (Map<String, Object> update : updates) {
      String action = asString(update == null ? null : update.get("action"));
      if ("add-snapshot".equals(action)) {
        out.add(update);
      }
    }
    return out;
  }

  public IcebergMetadata loadSnapshotMetadata(ResourceId tableId, long snapshotId) {
    SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotStub =
        grpc.withHeaders(grpc.raw().snapshot());
    try {
      var resp =
          snapshotStub.getSnapshot(
              GetSnapshotRequest.newBuilder()
                  .setTableId(tableId)
                  .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId))
                  .build());
      if (resp == null || !resp.hasSnapshot()) {
        return null;
      }
      Snapshot snapshot = resp.getSnapshot();
      if (!snapshot.hasIceberg()) {
        return null;
      }
      return snapshot.getIceberg();
    } catch (io.grpc.StatusRuntimeException e) {
      return null;
    }
  }

  public Response applySnapshotUpdates(
      TableGatewaySupport tableSupport,
      ResourceId tableId,
      List<String> namespacePath,
      String tableName,
      Supplier<Table> tableSupplier,
      List<Map<String, Object>> updates,
      String idempotencyKey,
      SnapshotUpdateContext snapshotContext) {
    if (updates == null || updates.isEmpty()) {
      return null;
    }
    Table existing = null;
    Long lastSnapshotId = null;
    SnapshotMetadataChanges metadataChanges = new SnapshotMetadataChanges();
    for (Map<String, Object> update : updates) {
      if (update == null) {
        continue;
      }
      String action = asString(update.get("action"));
      if ("add-snapshot".equals(action)) {
        @SuppressWarnings("unchecked")
        Map<String, Object> snapshot =
            update.get("snapshot") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
        if (snapshot == null || snapshot.isEmpty()) {
          return validationError("add-snapshot requires snapshot");
        }
        if (existing == null) {
          existing = tableSupplier.get();
        }
        Response error =
            createSnapshotPlaceholder(
                tableSupport,
                tableId,
                namespacePath,
                tableName,
                existing,
                snapshot,
                idempotencyKey);
        if (error != null) {
          return error;
        }
        Long snapshotId = asLong(snapshot.get("snapshot-id"));
        if (snapshotId != null) {
          lastSnapshotId = snapshotId;
          if (snapshotContext != null) {
            snapshotContext.lastSnapshotId = snapshotId;
          }
        }
      } else if ("remove-snapshots".equals(action)) {
        List<Long> ids = asLongList(update.get("snapshot-ids"));
        if (ids.isEmpty()) {
          return validationError("remove-snapshots requires snapshot-ids");
        }
        deleteSnapshots(tableId, ids);
      } else if ("set-snapshot-ref".equals(action)) {
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
        IcebergRef.Builder refBuilder =
            IcebergRef.newBuilder().setSnapshotId(pointedSnapshot).setType(type);
        Long maxRefAge =
            asLong(firstNonNull(update.get("max-ref-age-ms"), update.get("max_ref_age_ms")));
        if (maxRefAge != null) {
          refBuilder.setMaxReferenceAgeMs(maxRefAge);
        }
        Long maxSnapshotAge =
            asLong(
                firstNonNull(update.get("max-snapshot-age-ms"), update.get("max_snapshot_age_ms")));
        if (maxSnapshotAge != null) {
          refBuilder.setMaxSnapshotAgeMs(maxSnapshotAge);
        }
        Integer minSnapshots =
            asInteger(
                firstNonNull(
                    update.get("min-snapshots-to-keep"), update.get("min_snapshots_to_keep")));
        if (minSnapshots != null) {
          refBuilder.setMinSnapshotsToKeep(minSnapshots);
        }
        metadataChanges.refsToSet.put(refName, refBuilder.build());
      } else if ("remove-snapshot-ref".equals(action)) {
        String ref = asString(update.get("ref-name"));
        if (ref == null || ref.isBlank()) {
          return validationError("remove-snapshot-ref requires ref-name");
        }
        metadataChanges.refsToRemove.add(ref);
      } else if ("assign-uuid".equals(action)) {
        String uuid = asString(update.get("uuid"));
        if (uuid == null || uuid.isBlank()) {
          return validationError("assign-uuid requires uuid");
        }
        metadataChanges.tableUuid = uuid;
      } else if ("upgrade-format-version".equals(action)) {
        Integer version = asInteger(update.get("format-version"));
        if (version == null) {
          return validationError("upgrade-format-version requires format-version");
        }
        metadataChanges.formatVersion = version;
      } else if ("add-schema".equals(action)) {
        Map<String, Object> schemaMap = asObjectMap(update.get("schema"));
        if (schemaMap == null || schemaMap.isEmpty()) {
          return validationError("add-schema requires schema");
        }
        try {
          metadataChanges.schemasToAdd.add(buildIcebergSchema(schemaMap, update));
        } catch (IllegalArgumentException | JsonProcessingException e) {
          return validationError(e.getMessage());
        }
      } else if ("set-current-schema".equals(action)) {
        Integer schemaId = asInteger(update.get("schema-id"));
        if (schemaId == null) {
          return validationError("set-current-schema requires schema-id");
        }
        if (schemaId == -1) {
          metadataChanges.setCurrentSchemaLast = true;
        } else {
          metadataChanges.currentSchemaId = schemaId;
        }
      } else if ("add-spec".equals(action)) {
        Map<String, Object> specMap = asObjectMap(update.get("spec"));
        if (specMap == null || specMap.isEmpty()) {
          return validationError("add-spec requires spec");
        }
        try {
          metadataChanges.partitionSpecsToAdd.add(buildPartitionSpec(specMap));
        } catch (IllegalArgumentException e) {
          return validationError(e.getMessage());
        }
      } else if ("set-default-spec".equals(action)) {
        Integer specId = asInteger(update.get("spec-id"));
        if (specId == null) {
          return validationError("set-default-spec requires spec-id");
        }
        if (specId == -1) {
          metadataChanges.setDefaultSpecLast = true;
        } else {
          metadataChanges.defaultSpecId = specId;
        }
      } else if ("remove-partition-specs".equals(action)) {
        List<Integer> specIds = asIntegerList(update.get("spec-ids"));
        if (specIds.isEmpty()) {
          return validationError("remove-partition-specs requires spec-ids");
        }
        metadataChanges.partitionSpecIdsToRemove.addAll(specIds);
      } else if ("add-sort-order".equals(action)) {
        Map<String, Object> orderMap = asObjectMap(update.get("sort-order"));
        if (orderMap == null || orderMap.isEmpty()) {
          return validationError("add-sort-order requires sort-order");
        }
        try {
          metadataChanges.sortOrdersToAdd.add(buildSortOrder(orderMap));
        } catch (IllegalArgumentException e) {
          return validationError(e.getMessage());
        }
      } else if ("set-default-sort-order".equals(action)) {
        Integer orderId = asInteger(update.get("sort-order-id"));
        if (orderId == null) {
          return validationError("set-default-sort-order requires sort-order-id");
        }
        if (orderId == -1) {
          metadataChanges.setDefaultSortOrderLast = true;
        } else {
          metadataChanges.defaultSortOrderId = orderId;
        }
      } else if ("set-statistics".equals(action)) {
        Map<String, Object> statsMap = asObjectMap(update.get("statistics"));
        if (statsMap == null || statsMap.isEmpty()) {
          return validationError("set-statistics requires statistics");
        }
        try {
          metadataChanges.statisticsToAdd.add(buildStatisticsFile(statsMap));
        } catch (IllegalArgumentException e) {
          return validationError(e.getMessage());
        }
      } else if ("remove-statistics".equals(action)) {
        Long snapId = asLong(update.get("snapshot-id"));
        if (snapId == null) {
          return validationError("remove-statistics requires snapshot-id");
        }
        metadataChanges.statisticsSnapshotsToRemove.add(snapId);
      } else if ("set-partition-statistics".equals(action)) {
        Map<String, Object> statsMap = asObjectMap(update.get("partition-statistics"));
        if (statsMap == null || statsMap.isEmpty()) {
          return validationError("set-partition-statistics requires partition-statistics");
        }
        try {
          metadataChanges.partitionStatisticsToAdd.add(buildPartitionStatisticsFile(statsMap));
        } catch (IllegalArgumentException e) {
          return validationError(e.getMessage());
        }
      } else if ("remove-partition-statistics".equals(action)) {
        Long snapId = asLong(update.get("snapshot-id"));
        if (snapId == null) {
          return validationError("remove-partition-statistics requires snapshot-id");
        }
        metadataChanges.partitionStatisticsSnapshotsToRemove.add(snapId);
      } else if ("add-encryption-key".equals(action)) {
        Map<String, Object> keyMap = asObjectMap(update.get("encryption-key"));
        if (keyMap == null || keyMap.isEmpty()) {
          return validationError("add-encryption-key requires encryption-key");
        }
        try {
          metadataChanges.encryptionKeysToAdd.add(buildEncryptionKey(keyMap));
        } catch (IllegalArgumentException e) {
          return validationError(e.getMessage());
        }
      } else if ("remove-encryption-key".equals(action)) {
        String keyId = asString(update.get("key-id"));
        if (keyId == null || keyId.isBlank()) {
          return validationError("remove-encryption-key requires key-id");
        }
        metadataChanges.encryptionKeysToRemove.add(keyId);
      } else if ("remove-schemas".equals(action)) {
        List<Integer> schemaIds = asIntegerList(update.get("schema-ids"));
        if (schemaIds.isEmpty()) {
          return validationError("remove-schemas requires schema-ids");
        }
        metadataChanges.schemaIdsToRemove.addAll(schemaIds);
      }
    }
    if (metadataChanges.hasChanges()) {
      if (existing == null) {
        existing = tableSupplier.get();
      }
      Response error =
          applySnapshotMetadataUpdates(
              tableId, tableSupplier, existing, lastSnapshotId, metadataChanges);
      if (error != null) {
        return error;
      }
    }
    return null;
  }

  private Response createSnapshotPlaceholder(
      TableGatewaySupport tableSupport,
      ResourceId tableId,
      List<String> namespacePath,
      String tableName,
      Table existing,
      Map<String, Object> snapshot,
      String idempotencyKey) {
    Long snapshotId = asLong(snapshot.get("snapshot-id"));
    if (snapshotId == null) {
      return validationError("add-snapshot requires snapshot.snapshot-id");
    }
    SnapshotServiceGrpc.SnapshotServiceBlockingStub stub = grpc.withHeaders(grpc.raw().snapshot());
    SnapshotSpec.Builder spec =
        SnapshotSpec.newBuilder().setTableId(tableId).setSnapshotId(snapshotId);
    Long upstreamCreated = asLong(snapshot.get("timestamp-ms"));
    if (upstreamCreated != null) {
      spec.setUpstreamCreatedAt(Timestamps.fromMillis(upstreamCreated));
    }
    Long parentId = asLong(snapshot.get("parent-snapshot-id"));
    if (parentId != null) {
      spec.setParentSnapshotId(parentId);
    }
    Long sequenceNumber = asLong(snapshot.get("sequence-number"));
    if (sequenceNumber != null) {
      spec.setSequenceNumber(sequenceNumber);
    }
    String manifestList = asString(snapshot.get("manifest-list"));
    if (manifestList != null && !manifestList.isBlank()) {
      spec.setManifestList(manifestList);
    }
    Map<String, String> summary = asStringMap(snapshot.get("summary"));
    if (!summary.isEmpty()) {
      spec.putAllSummary(summary);
    }
    Integer schemaId = asInteger(snapshot.get("schema-id"));
    IcebergMetadata metadata = null;
    if (schemaId == null) {
      schemaId = propertyInt(existing.getPropertiesMap(), "current-schema-id");
    }
    if (schemaId == null) {
      metadata = tableSupport.loadCurrentMetadata(existing);
      if (metadata != null && metadata.getCurrentSchemaId() > 0) {
        schemaId = metadata.getCurrentSchemaId();
      }
    } else {
      metadata = tableSupport.loadCurrentMetadata(existing);
    }
    if (schemaId == null) {
      schemaId = 0;
    }
    spec.setSchemaId(schemaId);
    String schemaJson = asString(snapshot.get("schema-json"));
    if (schemaJson == null || schemaJson.isBlank()) {
      schemaJson = existing.getSchemaJson();
    }
    if ((schemaJson == null || schemaJson.isBlank()) && metadata != null) {
      schemaJson = schemaJsonFromMetadata(metadata, schemaId);
    }
    if (schemaJson != null && !schemaJson.isBlank()) {
      spec.setSchemaJson(schemaJson);
    }
    CreateSnapshotRequest.Builder request =
        CreateSnapshotRequest.newBuilder().setSpec(spec.build());
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(
          IdempotencyKey.newBuilder().setKey(idempotencyKey + ":snapshot:" + snapshotId).build());
    }
    stub.createSnapshot(request.build());
    return null;
  }

  private void deleteSnapshots(ResourceId tableId, List<Long> snapshotIds) {
    SnapshotServiceGrpc.SnapshotServiceBlockingStub stub = grpc.withHeaders(grpc.raw().snapshot());
    for (Long id : snapshotIds) {
      if (id == null) {
        continue;
      }
      stub.deleteSnapshot(
          DeleteSnapshotRequest.newBuilder().setTableId(tableId).setSnapshotId(id).build());
    }
  }

  private Response applySnapshotMetadataUpdates(
      ResourceId tableId,
      Supplier<Table> tableSupplier,
      Table existingTable,
      Long preferredSnapshotId,
      SnapshotMetadataChanges changes) {
    if (!changes.hasChanges()) {
      return null;
    }
    Table table = existingTable != null ? existingTable : tableSupplier.get();
    SnapshotServiceGrpc.SnapshotServiceBlockingStub stub = grpc.withHeaders(grpc.raw().snapshot());
    Snapshot snapshot;
    Long targetSnapshotId = preferredSnapshotId;
    if (targetSnapshotId == null) {
      try {
        snapshot =
            stub.getSnapshot(
                    GetSnapshotRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshot(
                            SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT))
                        .build())
                .getSnapshot();
        targetSnapshotId = snapshot.getSnapshotId();
      } catch (io.grpc.StatusRuntimeException e) {
        Long propertySnapshot = propertyLong(table.getPropertiesMap(), "current-snapshot-id");
        targetSnapshotId = propertySnapshot;
        snapshot = null;
      }
    } else {
      snapshot = null;
    }

    if (targetSnapshotId == null || targetSnapshotId <= 0) {
      return null;
    }

    if (snapshot == null) {
      snapshot =
          stub.getSnapshot(
                  GetSnapshotRequest.newBuilder()
                      .setTableId(tableId)
                      .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(targetSnapshotId))
                      .build())
              .getSnapshot();
    }

    IcebergMetadata.Builder iceberg =
        snapshot != null && snapshot.hasIceberg()
            ? snapshot.getIceberg().toBuilder()
            : IcebergMetadata.newBuilder();
    boolean mutated = false;

    if (changes.tableUuid != null) {
      iceberg.setTableUuid(changes.tableUuid);
      mutated = true;
    }
    if (changes.formatVersion != null) {
      iceberg.setFormatVersion(changes.formatVersion);
      mutated = true;
    }
    if (!changes.schemaIdsToRemove.isEmpty()) {
      List<IcebergSchema> filtered = new ArrayList<>();
      for (IcebergSchema schema : iceberg.getSchemasList()) {
        if (changes.schemaIdsToRemove.contains(schema.getSchemaId())) {
          mutated = true;
          continue;
        }
        filtered.add(schema);
      }
      if (filtered.size() != iceberg.getSchemasCount()) {
        iceberg.clearSchemas();
        iceberg.addAllSchemas(filtered);
      }
    }
    if (!changes.schemasToAdd.isEmpty()) {
      iceberg.addAllSchemas(changes.schemasToAdd);
      mutated = true;
    }
    if (changes.setCurrentSchemaLast) {
      Integer lastSchema = lastSchemaId(iceberg);
      if (lastSchema == null) {
        return validationError("set-current-schema requires at least one schema");
      }
      iceberg.setCurrentSchemaId(lastSchema);
      mutated = true;
    } else if (changes.currentSchemaId != null) {
      iceberg.setCurrentSchemaId(changes.currentSchemaId);
      mutated = true;
    }
    if (!changes.partitionSpecIdsToRemove.isEmpty()) {
      List<PartitionSpecInfo> filtered = new ArrayList<>();
      for (PartitionSpecInfo spec : iceberg.getPartitionSpecsList()) {
        if (changes.partitionSpecIdsToRemove.contains(spec.getSpecId())) {
          mutated = true;
          continue;
        }
        filtered.add(spec);
      }
      if (filtered.size() != iceberg.getPartitionSpecsCount()) {
        iceberg.clearPartitionSpecs();
        iceberg.addAllPartitionSpecs(filtered);
      }
    }
    if (!changes.partitionSpecsToAdd.isEmpty()) {
      iceberg.addAllPartitionSpecs(changes.partitionSpecsToAdd);
      mutated = true;
    }
    if (changes.setDefaultSpecLast) {
      Integer lastSpec = lastPartitionSpecId(iceberg);
      if (lastSpec == null) {
        return validationError("set-default-spec requires at least one partition spec");
      }
      iceberg.setDefaultSpecId(lastSpec);
      mutated = true;
    } else if (changes.defaultSpecId != null) {
      iceberg.setDefaultSpecId(changes.defaultSpecId);
      mutated = true;
    }
    if (!changes.sortOrdersToAdd.isEmpty()) {
      iceberg.addAllSortOrders(changes.sortOrdersToAdd);
      mutated = true;
    }
    if (changes.setDefaultSortOrderLast) {
      Integer lastSortOrder = lastSortOrderId(iceberg);
      if (lastSortOrder == null) {
        return validationError("set-default-sort-order requires at least one sort order");
      }
      iceberg.setDefaultSortOrderId(lastSortOrder);
      mutated = true;
    } else if (changes.defaultSortOrderId != null) {
      iceberg.setDefaultSortOrderId(changes.defaultSortOrderId);
      mutated = true;
    }

    if (!changes.statisticsSnapshotsToRemove.isEmpty() || !changes.statisticsToAdd.isEmpty()) {
      var replace =
          changes.statisticsToAdd.stream()
              .map(IcebergStatisticsFile::getSnapshotId)
              .collect(java.util.stream.Collectors.toSet());
      List<IcebergStatisticsFile> filtered = new ArrayList<>();
      for (IcebergStatisticsFile file : iceberg.getStatisticsList()) {
        long snapId = file.getSnapshotId();
        if (changes.statisticsSnapshotsToRemove.contains(snapId) || replace.contains(snapId)) {
          mutated = true;
          continue;
        }
        filtered.add(file);
      }
      if (!changes.statisticsToAdd.isEmpty()) {
        filtered.addAll(changes.statisticsToAdd);
        mutated = true;
      }
      if (mutated) {
        iceberg.clearStatistics();
        iceberg.addAllStatistics(filtered);
      }
    }
    if (!changes.partitionStatisticsSnapshotsToRemove.isEmpty()
        || !changes.partitionStatisticsToAdd.isEmpty()) {
      var replace =
          changes.partitionStatisticsToAdd.stream()
              .map(IcebergPartitionStatisticsFile::getSnapshotId)
              .collect(java.util.stream.Collectors.toSet());
      List<IcebergPartitionStatisticsFile> filtered = new ArrayList<>();
      for (IcebergPartitionStatisticsFile file : iceberg.getPartitionStatisticsList()) {
        long snapId = file.getSnapshotId();
        if (changes.partitionStatisticsSnapshotsToRemove.contains(snapId)
            || replace.contains(snapId)) {
          mutated = true;
          continue;
        }
        filtered.add(file);
      }
      if (!changes.partitionStatisticsToAdd.isEmpty()) {
        filtered.addAll(changes.partitionStatisticsToAdd);
        mutated = true;
      }
      if (mutated) {
        iceberg.clearPartitionStatistics();
        iceberg.addAllPartitionStatistics(filtered);
      }
    }
    if (!changes.encryptionKeysToRemove.isEmpty() || !changes.encryptionKeysToAdd.isEmpty()) {
      Map<String, IcebergEncryptedKey> keys = new LinkedHashMap<>();
      for (IcebergEncryptedKey key : iceberg.getEncryptionKeysList()) {
        if (changes.encryptionKeysToRemove.contains(key.getKeyId())) {
          mutated = true;
          continue;
        }
        keys.put(key.getKeyId(), key);
      }
      for (IcebergEncryptedKey key : changes.encryptionKeysToAdd) {
        keys.put(key.getKeyId(), key);
        mutated = true;
      }
      iceberg.clearEncryptionKeys();
      iceberg.addAllEncryptionKeys(keys.values());
    }

    if (!changes.refsToRemove.isEmpty() || !changes.refsToSet.isEmpty()) {
      var refs = new LinkedHashMap<>(iceberg.getRefsMap());
      if (!changes.refsToRemove.isEmpty()) {
        mutated = refs.keySet().removeAll(changes.refsToRemove) || mutated;
      }
      if (!changes.refsToSet.isEmpty()) {
        refs.putAll(changes.refsToSet);
        mutated = true;
      }
      iceberg.clearRefs();
      iceberg.putAllRefs(refs);
    }

    if (!mutated) {
      return null;
    }

    SnapshotSpec.Builder spec =
        SnapshotSpec.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(targetSnapshotId)
            .setIceberg(iceberg);
    FieldMask mask = FieldMask.newBuilder().addPaths("iceberg").build();

    stub.updateSnapshot(
        UpdateSnapshotRequest.newBuilder().setSpec(spec).setUpdateMask(mask).build());
    return null;
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
    List<Map<String, Object>> fields = asMapList(specMap.get("fields"));
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
    List<Map<String, Object>> fields = asMapList(orderMap.get("fields"));
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
        nullOrder = "NULLS_FIRST";
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
    List<Map<String, Object>> blobMaps = asMapList(statsMap.get("blob-metadata"));
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

  private static Integer lastSchemaId(IcebergMetadata.Builder builder) {
    int count = builder.getSchemasCount();
    if (count == 0) {
      return null;
    }
    return builder.getSchemas(count - 1).getSchemaId();
  }

  private static Integer lastPartitionSpecId(IcebergMetadata.Builder builder) {
    int count = builder.getPartitionSpecsCount();
    if (count == 0) {
      return null;
    }
    return builder.getPartitionSpecs(count - 1).getSpecId();
  }

  private static Integer lastSortOrderId(IcebergMetadata.Builder builder) {
    int count = builder.getSortOrdersCount();
    if (count == 0) {
      return null;
    }
    return builder.getSortOrders(count - 1).getSortOrderId();
  }

  private Response validationError(String message) {
    return Response.status(Response.Status.BAD_REQUEST)
        .entity(
            new ai.floedb.metacat.gateway.iceberg.rest.api.error.IcebergErrorResponse(
                new ai.floedb.metacat.gateway.iceberg.rest.api.error.IcebergError(
                    message, "ValidationException", 400)))
        .build();
  }

  private static Map<String, String> asStringMap(Object value) {
    if (!(value instanceof Map<?, ?> map)) {
      return Map.of();
    }
    Map<String, String> result = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (entry.getKey() == null || entry.getValue() == null) {
        continue;
      }
      result.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return result;
  }

  private static String asString(Object value) {
    return value == null ? null : String.valueOf(value);
  }

  private static Long asLong(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number.longValue();
    }
    String text = value.toString();
    if (text.isBlank()) {
      return null;
    }
    try {
      return Long.parseLong(text);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static List<Long> asLongList(Object value) {
    if (!(value instanceof List<?> list)) {
      return List.of();
    }
    List<Long> out = new ArrayList<>();
    for (Object item : list) {
      Long val = asLong(item);
      if (val != null) {
        out.add(val);
      }
    }
    return out;
  }

  private static Integer asInteger(Object value) {
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

  private static List<Integer> asIntegerList(Object value) {
    if (!(value instanceof List<?> list)) {
      return List.of();
    }
    List<Integer> out = new ArrayList<>();
    for (Object item : list) {
      Integer val = asInteger(item);
      if (val != null) {
        out.add(val);
      }
    }
    return out;
  }

  private static List<Map<String, Object>> asMapList(Object value) {
    if (!(value instanceof List<?> list)) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (Object item : list) {
      Map<String, Object> map = asObjectMap(item);
      if (map != null && !map.isEmpty()) {
        out.add(map);
      }
    }
    return out;
  }

  private static Map<String, Object> asObjectMap(Object value) {
    if (!(value instanceof Map<?, ?> map)) {
      return null;
    }
    Map<String, Object> out = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (entry.getKey() == null) {
        continue;
      }
      out.put(entry.getKey().toString(), entry.getValue());
    }
    return out;
  }

  private static Integer propertyInt(Map<String, String> props, String key) {
    String value = props.get(key);
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Long propertyLong(Map<String, String> props, String key) {
    String value = props.get(key);
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Object firstNonNull(Object first, Object second) {
    return first != null ? first : second;
  }

  private String schemaJsonFromMetadata(IcebergMetadata metadata, Integer schemaId) {
    if (metadata == null || metadata.getSchemasCount() == 0) {
      return null;
    }
    Integer targetId = schemaId != null ? schemaId : metadata.getCurrentSchemaId();
    if (targetId != null && targetId > 0) {
      for (IcebergSchema schema : metadata.getSchemasList()) {
        if (schema.getSchemaId() == targetId) {
          return schema.getSchemaJson();
        }
      }
    }
    return null;
  }

  private static final class SnapshotMetadataChanges {
    String tableUuid;
    Integer formatVersion;
    final List<IcebergSchema> schemasToAdd = new ArrayList<>();
    final Set<Integer> schemaIdsToRemove = new LinkedHashSet<>();
    Integer currentSchemaId;
    boolean setCurrentSchemaLast;
    final List<PartitionSpecInfo> partitionSpecsToAdd = new ArrayList<>();
    final Set<Integer> partitionSpecIdsToRemove = new LinkedHashSet<>();
    Integer defaultSpecId;
    boolean setDefaultSpecLast;
    final List<IcebergSortOrder> sortOrdersToAdd = new ArrayList<>();
    Integer defaultSortOrderId;
    boolean setDefaultSortOrderLast;
    final List<IcebergStatisticsFile> statisticsToAdd = new ArrayList<>();
    final Set<Long> statisticsSnapshotsToRemove = new LinkedHashSet<>();
    final List<IcebergPartitionStatisticsFile> partitionStatisticsToAdd = new ArrayList<>();
    final Set<Long> partitionStatisticsSnapshotsToRemove = new LinkedHashSet<>();
    final List<IcebergEncryptedKey> encryptionKeysToAdd = new ArrayList<>();
    final Set<String> encryptionKeysToRemove = new LinkedHashSet<>();
    final Map<String, IcebergRef> refsToSet = new LinkedHashMap<>();
    final Set<String> refsToRemove = new LinkedHashSet<>();

    boolean hasChanges() {
      return tableUuid != null
          || formatVersion != null
          || !schemasToAdd.isEmpty()
          || !schemaIdsToRemove.isEmpty()
          || currentSchemaId != null
          || setCurrentSchemaLast
          || !partitionSpecsToAdd.isEmpty()
          || !partitionSpecIdsToRemove.isEmpty()
          || defaultSpecId != null
          || setDefaultSpecLast
          || !sortOrdersToAdd.isEmpty()
          || defaultSortOrderId != null
          || setDefaultSortOrderLast
          || !statisticsToAdd.isEmpty()
          || !statisticsSnapshotsToRemove.isEmpty()
          || !partitionStatisticsToAdd.isEmpty()
          || !partitionStatisticsSnapshotsToRemove.isEmpty()
          || !encryptionKeysToAdd.isEmpty()
          || !encryptionKeysToRemove.isEmpty()
          || !refsToSet.isEmpty()
          || !refsToRemove.isEmpty();
    }
  }
}
