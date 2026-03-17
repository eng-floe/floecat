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

package ai.floedb.floecat.gateway.iceberg.minimal.services.transaction;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.minimal.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.encryption.BaseEncryptedKey;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;

@ApplicationScoped
public class IcebergMetadataCommitService {
  private final TableMetadataImportService importService;
  private final MinimalGatewayConfig config;
  private final ObjectMapper mapper;

  @Inject
  public IcebergMetadataCommitService(
      TableMetadataImportService importService, MinimalGatewayConfig config, ObjectMapper mapper) {
    this.importService = importService;
    this.config = config;
    this.mapper = mapper;
  }

  public record PlannedCommit(
      Table table, List<ai.floedb.floecat.transaction.rpc.TxChange> extraChanges) {}

  public record PlannedImportedCommit(
      Table table,
      List<ai.floedb.floecat.transaction.rpc.TxChange> extraChanges,
      List<Long> effectiveSnapshotIds,
      List<Long> removedSnapshotIds) {}

  public PlannedCommit plan(
      Table currentTable,
      ResourceId tableId,
      List<Map<String, Object>> requirements,
      List<Map<String, Object>> updates) {
    String metadataLocation = currentTable.getPropertiesOrDefault("metadata-location", "");
    Map<String, String> ioProps = mergedIoProperties(currentTable, updates);
    TableMetadata currentMetadata = loadCurrentMetadata(currentTable, metadataLocation, ioProps);
    validateRequirements(currentMetadata, requirements);

    TableMetadata nextMetadata = applyUpdates(currentMetadata, updates);
    String nextMetadataLocation = nextMetadataLocation(ioProps, currentMetadata, nextMetadata);
    FileIO fileIO = null;
    try {
      fileIO = FileIoFactory.createFileIo(ioProps, config);
      TableMetadata finalMetadata = nextMetadata;
      TableMetadataParser.write(finalMetadata, fileIO.newOutputFile(nextMetadataLocation));
      Table updatedTable =
          applyCanonicalProperties(currentTable, finalMetadata, nextMetadataLocation);
      return new PlannedCommit(
          updatedTable, snapshotTxChanges(tableId, finalMetadata, updates, nextMetadataLocation));
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw Status.INTERNAL
          .withDescription("failed to materialize Iceberg metadata")
          .withCause(e)
          .asRuntimeException();
    } finally {
      if (fileIO instanceof AutoCloseable closable) {
        try {
          closable.close();
        } catch (Exception ignored) {
        }
      }
    }
  }

  public PlannedCommit planCreate(
      Table createStub, ResourceId tableId, List<Map<String, Object>> updates) {
    TableMetadata bootstrap = bootstrapCreateMetadata(updates);
    if (!containsCreateSnapshotUpdates(updates)) {
      Table updatedTable = applyCanonicalProperties(createStub, bootstrap, null);
      return new PlannedCommit(updatedTable, List.of());
    }

    Map<String, String> ioProps = mergedIoProperties(createStub, updates);
    String metadataLocation = nextMetadataLocation(ioProps, null, bootstrap);
    FileIO fileIO = null;
    try {
      fileIO = FileIoFactory.createFileIo(ioProps, config);
      TableMetadataParser.write(bootstrap, fileIO.newOutputFile(metadataLocation));
      Table updatedTable = applyCanonicalProperties(createStub, bootstrap, metadataLocation);
      return new PlannedCommit(
          updatedTable, snapshotTxChanges(tableId, bootstrap, updates, metadataLocation));
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw Status.INTERNAL
          .withDescription("failed to materialize Iceberg metadata")
          .withCause(e)
          .asRuntimeException();
    } finally {
      if (fileIO instanceof AutoCloseable closable) {
        try {
          closable.close();
        } catch (Exception ignored) {
        }
      }
    }
  }

  public PlannedImportedCommit planImported(
      Table currentTable,
      ResourceId tableId,
      TableMetadataImportService.ImportedMetadata imported,
      Map<String, String> mergedProperties,
      List<Long> existingSnapshotIds) {
    if (imported == null || imported.icebergMetadata() == null) {
      throw Status.INVALID_ARGUMENT
          .withDescription("imported metadata is required")
          .asRuntimeException();
    }
    String metadataLocation = importedMetadataLocation(imported, mergedProperties);
    if (metadataLocation == null || metadataLocation.isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("imported metadata-location is required")
          .asRuntimeException();
    }
    Table updated =
        applyImportedProperties(currentTable, imported, mergedProperties, metadataLocation);
    List<Long> importedSnapshotIds = importedSnapshotIds(imported);
    List<Long> removedSnapshotIds = new ArrayList<>();
    if (existingSnapshotIds != null && !existingSnapshotIds.isEmpty()) {
      for (Long snapshotId : existingSnapshotIds) {
        if (snapshotId != null && !importedSnapshotIds.contains(snapshotId)) {
          removedSnapshotIds.add(snapshotId);
        }
      }
    }
    return new PlannedImportedCommit(
        updated,
        importedSnapshotTxChanges(tableId, imported, metadataLocation),
        effectiveImportedSnapshotIds(imported),
        List.copyOf(removedSnapshotIds));
  }

  private void validateRequirements(
      TableMetadata metadata, List<Map<String, Object>> requirements) {
    if (requirements == null || requirements.isEmpty()) {
      return;
    }
    for (Map<String, Object> requirement : requirements) {
      if (requirement == null) {
        continue;
      }
      String type = stringValue(requirement.get("type"));
      if ("assert-table-uuid".equals(type)) {
        String expected = stringValue(requirement.get("uuid"));
        if (expected != null && !expected.equals(metadata.uuid())) {
          throw Status.FAILED_PRECONDITION
              .withDescription("assert-table-uuid failed")
              .asRuntimeException();
        }
      } else if ("assert-current-schema-id".equals(type)) {
        Integer expected = intValue(requirement.get("current-schema-id"));
        if (expected != null && expected != metadata.currentSchemaId()) {
          throw Status.FAILED_PRECONDITION
              .withDescription("assert-current-schema-id failed")
              .asRuntimeException();
        }
      } else if ("assert-last-assigned-field-id".equals(type)) {
        Integer expected = intValue(requirement.get("last-assigned-field-id"));
        if (expected != null && expected != metadata.lastColumnId()) {
          throw Status.FAILED_PRECONDITION
              .withDescription("assert-last-assigned-field-id failed")
              .asRuntimeException();
        }
      } else if ("assert-last-assigned-partition-id".equals(type)) {
        Integer expected = intValue(requirement.get("last-assigned-partition-id"));
        if (expected != null && expected != metadata.lastAssignedPartitionId()) {
          throw Status.FAILED_PRECONDITION
              .withDescription("assert-last-assigned-partition-id failed")
              .asRuntimeException();
        }
      } else if ("assert-default-spec-id".equals(type)) {
        Integer expected = intValue(requirement.get("default-spec-id"));
        if (expected != null && expected != metadata.defaultSpecId()) {
          throw Status.FAILED_PRECONDITION
              .withDescription("assert-default-spec-id failed")
              .asRuntimeException();
        }
      } else if ("assert-default-sort-order-id".equals(type)) {
        Integer expected = intValue(requirement.get("default-sort-order-id"));
        if (expected != null && expected != metadata.defaultSortOrderId()) {
          throw Status.FAILED_PRECONDITION
              .withDescription("assert-default-sort-order-id failed")
              .asRuntimeException();
        }
      } else if ("assert-ref-snapshot-id".equals(type)) {
        String refName = stringValue(requirement.get("ref"));
        Long expected = longValue(requirement.get("snapshot-id"));
        SnapshotRef ref = refName == null ? null : metadata.ref(refName);
        Long actual = ref == null ? null : ref.snapshotId();
        if ((expected == null && actual != null)
            || (expected != null && !expected.equals(actual))) {
          throw Status.FAILED_PRECONDITION
              .withDescription("assert-ref-snapshot-id failed for ref " + refName)
              .asRuntimeException();
        }
      }
    }
  }

  private TableMetadata applyUpdates(TableMetadata metadata, List<Map<String, Object>> updates) {
    TableMetadata working = metadata;
    Long latestAddedSnapshotId = null;
    boolean explicitMainRefMutation = false;
    for (Map<String, Object> update : updates) {
      if (update == null) {
        continue;
      }
      String action = stringValue(update.get("action"));
      org.apache.iceberg.TableMetadata.Builder builder = TableMetadata.buildFrom(working);
      switch (action) {
        case "set-properties" -> builder.setProperties(stringMap(update.get("updates")));
        case "remove-properties" ->
            builder.removeProperties(new HashSet<>(stringList(update.get("removals"))));
        case "set-location" -> {
          String location = stringValue(update.get("location"));
          if (location == null || location.isBlank()) {
            throw Status.INVALID_ARGUMENT
                .withDescription("set-location requires location")
                .asRuntimeException();
          }
          builder.setLocation(location);
        }
        case "assign-uuid" -> {
          String uuid = stringValue(update.get("uuid"));
          if (uuid == null || uuid.isBlank()) {
            builder.assignUUID();
          } else {
            builder.assignUUID(uuid);
          }
        }
        case "upgrade-format-version" -> {
          Integer version = intValue(update.get("format-version"));
          if (version == null || version < 1) {
            throw Status.INVALID_ARGUMENT
                .withDescription("upgrade-format-version requires format-version")
                .asRuntimeException();
          }
          builder.upgradeFormatVersion(version);
        }
        case "add-schema" -> builder.addSchema(parseSchema(update.get("schema")));
        case "set-current-schema" ->
            builder.setCurrentSchema(resolveSchemaId(working, update.get("schema-id")));
        case "add-spec" ->
            builder.addPartitionSpec(parsePartitionSpec(working.schema(), update.get("spec")));
        case "set-default-spec" ->
            builder.setDefaultPartitionSpec(resolveSpecId(working, update.get("spec-id")));
        case "remove-partition-specs" ->
            invokeBuilder(builder, "removeSpecs", intList(update.get("spec-ids")));
        case "add-sort-order" ->
            builder.addSortOrder(parseSortOrder(working.schema(), update.get("sort-order")));
        case "set-default-sort-order" ->
            builder.setDefaultSortOrder(resolveSortOrderId(working, update.get("sort-order-id")));
        case "add-snapshot" -> {
          org.apache.iceberg.Snapshot snapshot = parseSnapshot(update.get("snapshot"));
          builder.addSnapshot(snapshot);
          latestAddedSnapshotId = snapshot.snapshotId();
        }
        case "set-statistics",
            "remove-statistics",
            "set-partition-statistics",
            "remove-partition-statistics" -> {
          // Statistics sidecars are optional for correctness. Minimal accepts these updates
          // for client compatibility but does not persist them separately.
        }
        case "add-encryption-key" ->
            builder.addEncryptionKey(buildEncryptionKey(update.get("encryption-key")));
        case "remove-encryption-key" -> builder.removeEncryptionKey(requireKeyId(update));
        case "remove-schemas" ->
            invokeBuilder(builder, "removeSchemas", intList(update.get("schema-ids")));
        case "remove-snapshots" -> builder.removeSnapshots(longList(update.get("snapshot-ids")));
        case "set-snapshot-ref" -> {
          String refName = stringValue(update.get("ref-name"));
          if (refName == null || refName.isBlank()) {
            refName = stringValue(update.get("ref"));
          }
          if (refName == null || refName.isBlank()) {
            throw Status.INVALID_ARGUMENT
                .withDescription("set-snapshot-ref requires ref")
                .asRuntimeException();
          }
          SnapshotRef ref = buildRef(update);
          builder.setRef(refName, ref);
          if (SnapshotRef.MAIN_BRANCH.equals(refName)) {
            explicitMainRefMutation = true;
          }
        }
        case "remove-snapshot-ref" -> {
          String refName = stringValue(update.get("ref-name"));
          if (refName == null || refName.isBlank()) {
            refName = stringValue(update.get("ref"));
          }
          if (refName == null || refName.isBlank()) {
            throw Status.INVALID_ARGUMENT
                .withDescription("remove-snapshot-ref requires ref")
                .asRuntimeException();
          }
          builder.removeRef(refName);
          if (SnapshotRef.MAIN_BRANCH.equals(refName)) {
            explicitMainRefMutation = true;
          }
        }
        default ->
            throw Status.INVALID_ARGUMENT
                .withDescription("unsupported commit update action: " + action)
                .asRuntimeException();
      }
      working = builder.build();
    }
    if (latestAddedSnapshotId != null && !explicitMainRefMutation) {
      working =
          TableMetadata.buildFrom(working)
              .setBranchSnapshot(latestAddedSnapshotId, SnapshotRef.MAIN_BRANCH)
              .build();
    }
    return working;
  }

  private TableMetadata bootstrapCreateMetadata(List<Map<String, Object>> updates) {
    Schema schema = null;
    PartitionSpec spec = PartitionSpec.unpartitioned();
    SortOrder sortOrder = SortOrder.unsorted();
    String location = null;
    String assignedUuid = null;
    Integer formatVersion = 2;
    Map<String, String> properties = new LinkedHashMap<>();

    for (Map<String, Object> update : updates) {
      String action = stringValue(update == null ? null : update.get("action"));
      if ("set-location".equals(action)) {
        String requested = stringValue(update.get("location"));
        if (requested != null && !requested.isBlank()) {
          location = requested;
        }
      } else if ("set-properties".equals(action)) {
        properties.putAll(stringMap(update.get("updates")));
      } else if ("remove-properties".equals(action)) {
        for (String key : stringList(update.get("removals"))) {
          properties.remove(key);
        }
      } else if ("assign-uuid".equals(action)) {
        String requested = stringValue(update.get("uuid"));
        if (requested != null && !requested.isBlank()) {
          assignedUuid = requested;
        }
      } else if ("upgrade-format-version".equals(action)) {
        Integer requested = intValue(update.get("format-version"));
        if (requested != null && requested >= 1) {
          formatVersion = requested;
        }
      } else if ("add-schema".equals(action)) {
        schema = parseSchema(update.get("schema"));
      }
    }

    if (schema == null) {
      throw Status.INVALID_ARGUMENT
          .withDescription("create commit requires add-schema")
          .asRuntimeException();
    }

    for (Map<String, Object> update : updates) {
      String action = stringValue(update == null ? null : update.get("action"));
      if ("add-spec".equals(action)) {
        spec = parsePartitionSpec(schema, update.get("spec"));
      } else if ("add-sort-order".equals(action)) {
        sortOrder = parseSortOrder(schema, update.get("sort-order"));
      }
    }

    if (location == null || location.isBlank()) {
      location = stringValue(properties.get("location"));
    }
    if (location == null || location.isBlank()) {
      location = stringValue(properties.get("storage_location"));
    }
    if (location == null || location.isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("create commit requires location")
          .asRuntimeException();
    }

    TableMetadata metadata =
        TableMetadata.newTableMetadata(schema, spec, sortOrder, location, properties);
    org.apache.iceberg.TableMetadata.Builder builder = TableMetadata.buildFrom(metadata);
    if (formatVersion != null && formatVersion >= 1 && formatVersion != metadata.formatVersion()) {
      builder.upgradeFormatVersion(formatVersion);
    }
    if (assignedUuid != null && !assignedUuid.isBlank()) {
      builder.assignUUID(assignedUuid);
    }
    List<Map<String, Object>> postCreateUpdates = filterPostCreateUpdates(updates);
    return applyUpdates(builder.build(), postCreateUpdates);
  }

  private TableMetadata loadCurrentMetadata(
      Table currentTable, String metadataLocation, Map<String, String> ioProps) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      if (canBootstrapCurrentTable(currentTable)) {
        return bootstrapCurrentTableMetadata(currentTable);
      }
      throw Status.INVALID_ARGUMENT
          .withDescription("metadata-location is required for snapshot and metadata commits")
          .asRuntimeException();
    }
    try {
      return importService.loadTableMetadata(metadataLocation, ioProps);
    } catch (IllegalArgumentException e) {
      if (canBootstrapCurrentTable(currentTable)) {
        return bootstrapCurrentTableMetadata(currentTable);
      }
      throw e;
    }
  }

  private boolean canBootstrapCurrentTable(Table currentTable) {
    return currentTable != null
        && !currentTable.getSchemaJson().isBlank()
        && tableLocation(currentTable) != null
        && !tableLocation(currentTable).isBlank()
        && !hasCurrentSnapshot(currentTable);
  }

  private boolean hasCurrentSnapshot(Table currentTable) {
    String currentSnapshotId = currentTable.getPropertiesOrDefault("current-snapshot-id", "");
    return currentSnapshotId != null && !currentSnapshotId.isBlank();
  }

  private String tableLocation(Table currentTable) {
    String location = currentTable.getPropertiesOrDefault("location", "");
    if (location != null && !location.isBlank()) {
      return location;
    }
    location = currentTable.getPropertiesOrDefault("storage_location", "");
    return location == null || location.isBlank() ? null : location;
  }

  private TableMetadata bootstrapCurrentTableMetadata(Table currentTable) {
    try {
      return importService.bootstrapMetadataFromCatalogTable(currentTable);
    } catch (IllegalArgumentException e) {
      throw Status.INVALID_ARGUMENT
          .withDescription(e.getMessage())
          .withCause(e)
          .asRuntimeException();
    }
  }

  private List<Map<String, Object>> filterPostCreateUpdates(List<Map<String, Object>> updates) {
    List<Map<String, Object>> out = new ArrayList<>();
    for (Map<String, Object> update : updates) {
      String action = stringValue(update == null ? null : update.get("action"));
      if ("assign-uuid".equals(action)
          || "upgrade-format-version".equals(action)
          || "add-schema".equals(action)
          || "set-current-schema".equals(action)
          || "add-spec".equals(action)
          || "set-default-spec".equals(action)
          || "add-sort-order".equals(action)
          || "set-default-sort-order".equals(action)
          || "set-location".equals(action)
          || "set-properties".equals(action)
          || "remove-properties".equals(action)) {
        continue;
      }
      out.add(update);
    }
    return List.copyOf(out);
  }

  private boolean containsCreateSnapshotUpdates(List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return false;
    }
    for (Map<String, Object> update : updates) {
      String action = stringValue(update == null ? null : update.get("action"));
      if ("add-snapshot".equals(action)
          || "set-snapshot-ref".equals(action)
          || "remove-snapshot-ref".equals(action)
          || "remove-snapshots".equals(action)) {
        return true;
      }
    }
    return false;
  }

  private List<ai.floedb.floecat.transaction.rpc.TxChange> snapshotTxChanges(
      ResourceId tableId,
      TableMetadata metadata,
      List<Map<String, Object>> updates,
      String metadataLocation) {
    List<ai.floedb.floecat.transaction.rpc.TxChange> out = new ArrayList<>();
    String accountId = tableId.getAccountId();
    for (Map<String, Object> update : updates) {
      if (!"add-snapshot".equals(stringValue(update == null ? null : update.get("action")))) {
        continue;
      }
      Map<String, Object> snapshotMap = objectMap(update.get("snapshot"));
      Long snapshotId = longValue(snapshotMap == null ? null : snapshotMap.get("snapshot-id"));
      if (snapshotId == null || snapshotId < 0) {
        throw Status.INVALID_ARGUMENT
            .withDescription("add-snapshot requires snapshot-id")
            .asRuntimeException();
      }
      org.apache.iceberg.Snapshot icebergSnapshot = metadata.snapshot(snapshotId);
      Snapshot snapshot =
          toCatalogSnapshot(tableId, metadata, metadataLocation, icebergSnapshot, snapshotMap);
      long upstreamCreatedAtMs =
          snapshot.hasUpstreamCreatedAt()
              ? Timestamps.toMillis(snapshot.getUpstreamCreatedAt())
              : System.currentTimeMillis();
      ByteString payload = ByteString.copyFrom(snapshot.toByteArray());
      out.add(
          ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
              .setTargetPointerKey(snapshotPointerById(accountId, tableId.getId(), snapshotId))
              .setPayload(payload)
              .build());
      out.add(
          ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
              .setTargetPointerKey(
                  snapshotPointerByTime(
                      accountId, tableId.getId(), snapshotId, upstreamCreatedAtMs))
              .setPayload(payload)
              .build());
    }
    return List.copyOf(out);
  }

  private Snapshot toCatalogSnapshot(
      ResourceId tableId,
      TableMetadata metadata,
      String metadataLocation,
      org.apache.iceberg.Snapshot icebergSnapshot,
      Map<String, Object> snapshotMap) {
    Snapshot.Builder builder =
        Snapshot.newBuilder().setTableId(tableId).setSnapshotId(icebergSnapshot.snapshotId());
    if (icebergSnapshot.timestampMillis() > 0) {
      builder.setUpstreamCreatedAt(Timestamps.fromMillis(icebergSnapshot.timestampMillis()));
    }
    if (icebergSnapshot.parentId() != null) {
      builder.setParentSnapshotId(icebergSnapshot.parentId());
    }
    if (icebergSnapshot.sequenceNumber() > 0) {
      builder.setSequenceNumber(icebergSnapshot.sequenceNumber());
    }
    if (icebergSnapshot.manifestListLocation() != null) {
      builder.setManifestList(icebergSnapshot.manifestListLocation());
    }
    if (icebergSnapshot.summary() != null && !icebergSnapshot.summary().isEmpty()) {
      builder.putAllSummary(icebergSnapshot.summary());
    }
    if (icebergSnapshot.schemaId() != null && icebergSnapshot.schemaId() >= 0) {
      builder.setSchemaId(icebergSnapshot.schemaId());
    }
    Schema schema =
        icebergSnapshot.schemaId() != null
                && metadata.schemasById().containsKey(icebergSnapshot.schemaId())
            ? metadata.schemasById().get(icebergSnapshot.schemaId())
            : metadata.schema();
    if (schema != null) {
      builder.setSchemaJson(SchemaParser.toJson(schema));
    }
    IcebergMetadata snapshotMetadata =
        importService.toIcebergMetadata(metadata, metadataLocation).toBuilder()
            .setCurrentSnapshotId(icebergSnapshot.snapshotId())
            .setLastSequenceNumber(
                Math.max(metadata.lastSequenceNumber(), icebergSnapshot.sequenceNumber()))
            .build();
    builder.putFormatMetadata("iceberg", snapshotMetadata.toByteString());
    return builder.build();
  }

  private Table applyCanonicalProperties(
      Table currentTable, TableMetadata metadata, String metadataLocation) {
    Map<String, String> properties =
        metadataLocation == null || metadataLocation.isBlank()
            ? new LinkedHashMap<>(metadata.properties())
            : new LinkedHashMap<>(importService.canonicalProperties(metadata, metadataLocation));
    if (metadataLocation == null || metadataLocation.isBlank()) {
      properties.putIfAbsent("table-uuid", metadata.uuid());
      if (metadata.location() != null && !metadata.location().isBlank()) {
        properties.put("location", metadata.location());
        properties.put("storage_location", metadata.location());
      }
      properties.put("format-version", Integer.toString(metadata.formatVersion()));
      putInt(properties, "current-schema-id", metadata.currentSchemaId());
      putInt(properties, "last-column-id", metadata.lastColumnId());
      putInt(properties, "default-spec-id", metadata.defaultSpecId());
      putInt(properties, "last-partition-id", metadata.lastAssignedPartitionId());
      putInt(properties, "default-sort-order-id", metadata.defaultSortOrderId());
      putLong(properties, "last-sequence-number", metadata.lastSequenceNumber());
      putLong(properties, "last-updated-ms", metadata.lastUpdatedMillis());
      if (metadata.currentSnapshot() != null) {
        putLong(properties, "current-snapshot-id", metadata.currentSnapshot().snapshotId());
      } else {
        properties.remove("current-snapshot-id");
      }
      properties.remove("metadata-location");
    }
    if (!metadata.refs().isEmpty()) {
      properties.put("metadata.refs", encodeRefs(metadata.refs()));
    } else {
      properties.remove("metadata.refs");
    }
    Table.Builder updated = currentTable.toBuilder().clearProperties().putAllProperties(properties);
    if (metadata.schema() != null) {
      updated.setSchemaJson(SchemaParser.toJson(metadata.schema()));
    }
    if (currentTable.hasUpstream()
        && metadata.location() != null
        && !metadata.location().isBlank()) {
      updated.setUpstream(
          currentTable.getUpstream().toBuilder().setUri(metadata.location()).build());
    }
    return updated.build();
  }

  private static void putInt(Map<String, String> props, String key, int value) {
    if (value >= 0) {
      props.put(key, Integer.toString(value));
    }
  }

  private static void putLong(Map<String, String> props, String key, long value) {
    if (value >= 0) {
      props.put(key, Long.toString(value));
    }
  }

  private Table applyImportedProperties(
      Table currentTable,
      TableMetadataImportService.ImportedMetadata imported,
      Map<String, String> mergedProperties,
      String metadataLocation) {
    Map<String, String> properties = new LinkedHashMap<>();
    if (mergedProperties != null && !mergedProperties.isEmpty()) {
      properties.putAll(mergedProperties);
    }
    properties.put("metadata-location", metadataLocation);
    if (imported.tableLocation() != null && !imported.tableLocation().isBlank()) {
      properties.putIfAbsent("location", imported.tableLocation());
      properties.putIfAbsent("storage_location", imported.tableLocation());
    }
    Table.Builder updated = currentTable.toBuilder().clearProperties().putAllProperties(properties);
    if (imported.schemaJson() != null && !imported.schemaJson().isBlank()) {
      updated.setSchemaJson(imported.schemaJson());
    }
    if (updated.hasUpstream()) {
      String upstreamUri = properties.getOrDefault("location", imported.tableLocation());
      if (upstreamUri != null && !upstreamUri.isBlank()) {
        updated.setUpstream(updated.getUpstream().toBuilder().setUri(upstreamUri).build());
      }
    }
    return updated.build();
  }

  private List<ai.floedb.floecat.transaction.rpc.TxChange> importedSnapshotTxChanges(
      ResourceId tableId,
      TableMetadataImportService.ImportedMetadata imported,
      String metadataLocation) {
    List<ai.floedb.floecat.transaction.rpc.TxChange> out = new ArrayList<>();
    if (imported == null) {
      return List.of();
    }
    List<TableMetadataImportService.ImportedSnapshot> snapshots =
        imported.snapshots() != null && !imported.snapshots().isEmpty()
            ? imported.snapshots()
            : imported.currentSnapshot() == null ? List.of() : List.of(imported.currentSnapshot());
    String accountId = tableId.getAccountId();
    for (TableMetadataImportService.ImportedSnapshot importedSnapshot : snapshots) {
      if (importedSnapshot == null || importedSnapshot.snapshotId() == null) {
        continue;
      }
      Snapshot snapshot = toCatalogSnapshot(tableId, imported, metadataLocation, importedSnapshot);
      long upstreamCreatedAtMs =
          snapshot.hasUpstreamCreatedAt()
              ? Timestamps.toMillis(snapshot.getUpstreamCreatedAt())
              : System.currentTimeMillis();
      ByteString payload = ByteString.copyFrom(snapshot.toByteArray());
      out.add(
          ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
              .setTargetPointerKey(
                  snapshotPointerById(accountId, tableId.getId(), importedSnapshot.snapshotId()))
              .setPayload(payload)
              .build());
      out.add(
          ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
              .setTargetPointerKey(
                  snapshotPointerByTime(
                      accountId,
                      tableId.getId(),
                      importedSnapshot.snapshotId(),
                      upstreamCreatedAtMs))
              .setPayload(payload)
              .build());
    }
    return List.copyOf(out);
  }

  private Snapshot toCatalogSnapshot(
      ResourceId tableId,
      TableMetadataImportService.ImportedMetadata imported,
      String metadataLocation,
      TableMetadataImportService.ImportedSnapshot importedSnapshot) {
    Snapshot.Builder builder =
        Snapshot.newBuilder().setTableId(tableId).setSnapshotId(importedSnapshot.snapshotId());
    if (importedSnapshot.timestampMs() != null && importedSnapshot.timestampMs() >= 0) {
      builder.setUpstreamCreatedAt(Timestamps.fromMillis(importedSnapshot.timestampMs()));
    }
    if (importedSnapshot.parentSnapshotId() != null) {
      builder.setParentSnapshotId(importedSnapshot.parentSnapshotId());
    }
    if (importedSnapshot.sequenceNumber() != null && importedSnapshot.sequenceNumber() >= 0) {
      builder.setSequenceNumber(importedSnapshot.sequenceNumber());
    }
    if (importedSnapshot.manifestList() != null && !importedSnapshot.manifestList().isBlank()) {
      builder.setManifestList(importedSnapshot.manifestList());
    }
    if (importedSnapshot.summary() != null && !importedSnapshot.summary().isEmpty()) {
      builder.putAllSummary(importedSnapshot.summary());
    }
    if (importedSnapshot.schemaId() != null && importedSnapshot.schemaId() >= 0) {
      builder.setSchemaId(importedSnapshot.schemaId());
    }
    if (imported.schemaJson() != null && !imported.schemaJson().isBlank()) {
      builder.setSchemaJson(imported.schemaJson());
    }
    IcebergMetadata snapshotMetadata =
        imported.icebergMetadata().toBuilder()
            .setMetadataLocation(metadataLocation)
            .setCurrentSnapshotId(importedSnapshot.snapshotId())
            .build();
    builder.putFormatMetadata("iceberg", snapshotMetadata.toByteString());
    return builder.build();
  }

  private String importedMetadataLocation(
      TableMetadataImportService.ImportedMetadata imported, Map<String, String> mergedProperties) {
    if (mergedProperties != null) {
      String explicit = mergedProperties.get("metadata-location");
      if (explicit != null && !explicit.isBlank()) {
        return explicit;
      }
    }
    if (imported != null
        && imported.icebergMetadata() != null
        && !imported.icebergMetadata().getMetadataLocation().isBlank()) {
      return imported.icebergMetadata().getMetadataLocation();
    }
    return null;
  }

  private List<Long> importedSnapshotIds(TableMetadataImportService.ImportedMetadata imported) {
    List<Long> out = new ArrayList<>();
    if (imported == null) {
      return out;
    }
    List<TableMetadataImportService.ImportedSnapshot> snapshots =
        imported.snapshots() != null && !imported.snapshots().isEmpty()
            ? imported.snapshots()
            : imported.currentSnapshot() == null ? List.of() : List.of(imported.currentSnapshot());
    for (TableMetadataImportService.ImportedSnapshot snapshot : snapshots) {
      if (snapshot != null && snapshot.snapshotId() != null) {
        out.add(snapshot.snapshotId());
      }
    }
    return List.copyOf(out);
  }

  private List<Long> effectiveImportedSnapshotIds(
      TableMetadataImportService.ImportedMetadata imported) {
    if (imported == null) {
      return List.of();
    }
    if (imported.currentSnapshot() != null && imported.currentSnapshot().snapshotId() != null) {
      return List.of(imported.currentSnapshot().snapshotId());
    }
    return importedSnapshotIds(imported);
  }

  private String nextMetadataLocation(
      Map<String, String> ioProps, TableMetadata currentMetadata, TableMetadata nextMetadata) {
    String current = currentMetadata == null ? null : currentMetadata.metadataFileLocation();
    String directory =
        current == null || current.isBlank()
            ? defaultMetadataDirectory(nextMetadata.location())
            : directoryOf(current);
    if (directory == null || directory.isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Unable to derive metadata directory")
          .asRuntimeException();
    }
    long version = Math.max(parseVersion(current), scanMaxVersion(ioProps, directory)) + 1;
    return normalizeDirectory(directory)
        + String.format("%05d-%s.metadata.json", version, UUID.randomUUID());
  }

  private long scanMaxVersion(Map<String, String> ioProps, String directory) {
    FileIO fileIO = null;
    try {
      fileIO = FileIoFactory.createFileIo(ioProps, config);
      if (!(fileIO instanceof SupportsPrefixOperations prefixOps)) {
        return -1;
      }
      long max = -1;
      for (FileInfo info : prefixOps.listPrefix(normalizeDirectory(directory))) {
        long parsed = parseVersion(info.location());
        if (parsed > max) {
          max = parsed;
        }
      }
      return max;
    } catch (Exception ignored) {
      return -1;
    } finally {
      if (fileIO instanceof AutoCloseable closable) {
        try {
          closable.close();
        } catch (Exception ignored) {
        }
      }
    }
  }

  private Map<String, String> mergedIoProperties(
      Table currentTable, List<Map<String, Object>> updates) {
    Map<String, String> merged = new LinkedHashMap<>(currentTable.getPropertiesMap());
    for (Map<String, Object> update : updates) {
      if (!"set-properties".equals(stringValue(update == null ? null : update.get("action")))) {
        continue;
      }
      merged.putAll(stringMap(update.get("updates")));
    }
    Map<String, String> filtered = new LinkedHashMap<>(FileIoFactory.filterIoProperties(merged));
    if (config != null) {
      config.metadataFileIo().ifPresent(ioImpl -> filtered.putIfAbsent("io-impl", ioImpl));
      config
          .metadataFileIoRoot()
          .ifPresent(root -> filtered.putIfAbsent("fs.floecat.test-root", root));
      config
          .metadataS3Endpoint()
          .ifPresent(endpoint -> filtered.putIfAbsent("s3.endpoint", endpoint));
      filtered.putIfAbsent(
          "s3.path-style-access", Boolean.toString(config.metadataS3PathStyleAccess()));
      config.metadataS3Region().ifPresent(region -> filtered.putIfAbsent("s3.region", region));
      config
          .metadataClientRegion()
          .ifPresent(region -> filtered.putIfAbsent("client.region", region));
      config
          .metadataS3AccessKeyId()
          .ifPresent(accessKeyId -> filtered.putIfAbsent("s3.access-key-id", accessKeyId));
      config
          .metadataS3SecretAccessKey()
          .ifPresent(
              secretAccessKey -> filtered.putIfAbsent("s3.secret-access-key", secretAccessKey));
    }
    return Map.copyOf(filtered);
  }

  private Schema parseSchema(Object value) {
    try {
      return SchemaParser.fromJson(mapper.writeValueAsString(value));
    } catch (IOException e) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Invalid add-schema payload")
          .asRuntimeException();
    }
  }

  private PartitionSpec parsePartitionSpec(Schema schema, Object value) {
    try {
      JsonNode node = mapper.valueToTree(value);
      return PartitionSpecParser.fromJson(schema, node);
    } catch (RuntimeException e) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Invalid add-spec payload")
          .asRuntimeException();
    }
  }

  private SortOrder parseSortOrder(Schema schema, Object value) {
    try {
      JsonNode node = mapper.valueToTree(value);
      return SortOrderParser.fromJson(schema, node);
    } catch (RuntimeException e) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Invalid add-sort-order payload")
          .asRuntimeException();
    }
  }

  private org.apache.iceberg.Snapshot parseSnapshot(Object value) {
    Map<String, Object> snapshot = objectMap(value);
    if (snapshot == null || snapshot.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("add-snapshot requires snapshot")
          .asRuntimeException();
    }
    Map<String, Object> sanitized = new LinkedHashMap<>();
    copyIfPresent(snapshot, sanitized, "sequence-number");
    copyIfPresent(snapshot, sanitized, "snapshot-id");
    copyIfPresent(snapshot, sanitized, "parent-snapshot-id");
    copyIfPresent(snapshot, sanitized, "timestamp-ms");
    copyIfPresent(snapshot, sanitized, "manifest-list");
    copyIfPresent(snapshot, sanitized, "schema-id");
    Object summary = snapshot.get("summary");
    if (summary instanceof Map<?, ?>) {
      sanitized.put("summary", summary);
    } else {
      Map<String, Object> generatedSummary = new LinkedHashMap<>();
      String operation = stringValue(snapshot.get("operation"));
      if (operation != null && !operation.isBlank()) {
        generatedSummary.put("operation", operation);
      }
      if (!generatedSummary.isEmpty()) {
        sanitized.put("summary", generatedSummary);
      }
    }
    try {
      return SnapshotParser.fromJson(mapper.writeValueAsString(sanitized));
    } catch (IOException e) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Invalid add-snapshot payload")
          .asRuntimeException();
    }
  }

  private SnapshotRef buildRef(Map<String, Object> update) {
    Long snapshotId = longValue(update.get("snapshot-id"));
    if (snapshotId == null || snapshotId <= 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("set-snapshot-ref requires snapshot-id")
          .asRuntimeException();
    }
    String type = stringValue(update.get("type"));
    SnapshotRef.Builder builder =
        "tag".equalsIgnoreCase(type)
            ? SnapshotRef.tagBuilder(snapshotId)
            : SnapshotRef.branchBuilder(snapshotId);
    Long maxRefAgeMs = longValue(update.get("max-ref-age-ms"));
    if (maxRefAgeMs != null && maxRefAgeMs >= 0) {
      builder.maxRefAgeMs(maxRefAgeMs);
    }
    Long maxSnapshotAgeMs = longValue(update.get("max-snapshot-age-ms"));
    if (maxSnapshotAgeMs != null && maxSnapshotAgeMs >= 0) {
      builder.maxSnapshotAgeMs(maxSnapshotAgeMs);
    }
    Integer minSnapshotsToKeep = intValue(update.get("min-snapshots-to-keep"));
    if (minSnapshotsToKeep != null && minSnapshotsToKeep >= 0) {
      builder.minSnapshotsToKeep(minSnapshotsToKeep);
    }
    return builder.build();
  }

  private EncryptedKey buildEncryptionKey(Object value) {
    Map<String, Object> key = objectMap(value);
    if (key == null || key.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("add-encryption-key requires encryption-key")
          .asRuntimeException();
    }
    String keyId = stringValue(key.get("key-id"));
    String metadataB64 = stringValue(key.get("encrypted-key-metadata"));
    if (keyId == null || keyId.isBlank() || metadataB64 == null || metadataB64.isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("add-encryption-key requires key-id and encrypted-key-metadata")
          .asRuntimeException();
    }
    byte[] decoded;
    try {
      decoded = Base64.getDecoder().decode(metadataB64);
    } catch (IllegalArgumentException e) {
      throw Status.INVALID_ARGUMENT
          .withDescription("encrypted-key-metadata must be valid base64")
          .asRuntimeException();
    }
    String encryptedBy = stringValue(key.get("encrypted-by-id"));
    return new BaseEncryptedKey(
        keyId,
        ByteBuffer.wrap(decoded),
        encryptedBy == null || encryptedBy.isBlank() ? null : encryptedBy,
        stringMap(key.get("properties")));
  }

  private String requireKeyId(Map<String, Object> update) {
    String keyId = stringValue(update.get("key-id"));
    if (keyId == null || keyId.isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("remove-encryption-key requires key-id")
          .asRuntimeException();
    }
    return keyId;
  }

  private void invokeBuilder(
      org.apache.iceberg.TableMetadata.Builder builder, String methodName, List<Integer> ids) {
    if (ids.isEmpty()) {
      String description =
          "removeSpecs".equals(methodName)
              ? "remove-partition-specs requires spec-ids"
              : "remove-schemas requires schema-ids";
      throw Status.INVALID_ARGUMENT.withDescription(description).asRuntimeException();
    }
    try {
      Method method = builder.getClass().getDeclaredMethod(methodName, Iterable.class);
      method.setAccessible(true);
      method.invoke(builder, ids);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause() == null ? e : e.getCause();
      if (cause instanceof RuntimeException runtimeException) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                cause.getMessage() == null ? "invalid metadata update" : cause.getMessage())
            .withCause(runtimeException)
            .asRuntimeException();
      }
      throw Status.INTERNAL
          .withDescription("failed to apply metadata update")
          .withCause(cause)
          .asRuntimeException();
    } catch (ReflectiveOperationException e) {
      throw Status.INTERNAL
          .withDescription("unsupported Iceberg metadata builder method: " + methodName)
          .withCause(e)
          .asRuntimeException();
    }
  }

  private int resolveSchemaId(TableMetadata metadata, Object requested) {
    Integer schemaId = intValue(requested);
    if (schemaId != null && schemaId == -1) {
      int max = -1;
      for (Schema schema : metadata.schemas()) {
        max = Math.max(max, schema.schemaId());
      }
      return max;
    }
    if (schemaId == null) {
      throw Status.INVALID_ARGUMENT
          .withDescription("set-current-schema requires schema-id")
          .asRuntimeException();
    }
    return schemaId;
  }

  private int resolveSpecId(TableMetadata metadata, Object requested) {
    Integer specId = intValue(requested);
    if (specId != null && specId == -1) {
      int max = -1;
      for (PartitionSpec spec : metadata.specs()) {
        max = Math.max(max, spec.specId());
      }
      return max;
    }
    if (specId == null) {
      throw Status.INVALID_ARGUMENT
          .withDescription("set-default-spec requires spec-id")
          .asRuntimeException();
    }
    return specId;
  }

  private int resolveSortOrderId(TableMetadata metadata, Object requested) {
    Integer orderId = intValue(requested);
    if (orderId != null && orderId == -1) {
      int max = -1;
      for (SortOrder order : metadata.sortOrders()) {
        max = Math.max(max, order.orderId());
      }
      return max;
    }
    if (orderId == null) {
      throw Status.INVALID_ARGUMENT
          .withDescription("set-default-sort-order requires sort-order-id")
          .asRuntimeException();
    }
    return orderId;
  }

  private String encodeRefs(Map<String, SnapshotRef> refs) {
    Map<String, Map<String, Object>> encoded = new LinkedHashMap<>();
    refs.forEach(
        (name, ref) -> {
          Map<String, Object> values = new LinkedHashMap<>();
          values.put("snapshot-id", ref.snapshotId());
          values.put("type", ref.type().name().toLowerCase(Locale.ROOT));
          if (ref.maxRefAgeMs() != null) {
            values.put("max-ref-age-ms", ref.maxRefAgeMs());
          }
          if (ref.maxSnapshotAgeMs() != null) {
            values.put("max-snapshot-age-ms", ref.maxSnapshotAgeMs());
          }
          if (ref.minSnapshotsToKeep() != null) {
            values.put("min-snapshots-to-keep", ref.minSnapshotsToKeep());
          }
          encoded.put(name, values);
        });
    try {
      return mapper.writeValueAsString(encoded);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to encode metadata refs property", e);
    }
  }

  private String snapshotPointerById(String accountId, String tableId, long snapshotId) {
    return "/accounts/"
        + encode(accountId)
        + "/tables/"
        + encode(tableId)
        + "/snapshots/by-id/"
        + String.format("%019d", snapshotId);
  }

  private String snapshotPointerByTime(
      String accountId, String tableId, long snapshotId, long upstreamCreatedAtMs) {
    long inverted = Long.MAX_VALUE - upstreamCreatedAtMs;
    long invertedSnapshotId = Long.MAX_VALUE - snapshotId;
    return "/accounts/"
        + encode(accountId)
        + "/tables/"
        + encode(tableId)
        + "/snapshots/by-time/"
        + String.format("%019d-%019d", inverted, invertedSnapshotId);
  }

  private String encode(String value) {
    return java.net.URLEncoder.encode(value, java.nio.charset.StandardCharsets.UTF_8)
        .replace("+", "%20");
  }

  private String defaultMetadataDirectory(String tableLocation) {
    if (tableLocation == null || tableLocation.isBlank()) {
      return null;
    }
    String base =
        tableLocation.endsWith("/")
            ? tableLocation.substring(0, tableLocation.length() - 1)
            : tableLocation;
    return base + "/metadata/";
  }

  private String normalizeDirectory(String directory) {
    if (directory == null || directory.isBlank()) {
      return null;
    }
    String normalized = directory;
    while (normalized.endsWith("/") && normalized.length() > 1) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }
    return normalized + "/";
  }

  private String directoryOf(String location) {
    if (location == null || location.isBlank()) {
      return null;
    }
    int slash = location.lastIndexOf('/');
    return slash < 0 ? null : location.substring(0, slash + 1);
  }

  private long parseVersion(String location) {
    if (location == null || location.isBlank()) {
      return -1;
    }
    int slash = location.lastIndexOf('/');
    String name = slash >= 0 ? location.substring(slash + 1) : location;
    int dash = name.indexOf('-');
    if (dash <= 0) {
      return -1;
    }
    try {
      return Long.parseLong(name.substring(0, dash));
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  private void copyIfPresent(Map<String, Object> source, Map<String, Object> target, String key) {
    if (source.containsKey(key) && source.get(key) != null) {
      target.put(key, source.get(key));
    }
  }

  private Map<String, Object> objectMap(Object value) {
    if (!(value instanceof Map<?, ?> map)) {
      return null;
    }
    Map<String, Object> out = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (entry.getKey() != null) {
        out.put(String.valueOf(entry.getKey()), entry.getValue());
      }
    }
    return out;
  }

  private Map<String, String> stringMap(Object value) {
    if (!(value instanceof Map<?, ?> map)) {
      return Map.of();
    }
    Map<String, String> out = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (entry.getKey() != null && entry.getValue() != null) {
        out.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
      }
    }
    return out;
  }

  private List<String> stringList(Object value) {
    if (!(value instanceof List<?> list)) {
      return List.of();
    }
    List<String> out = new ArrayList<>();
    for (Object item : list) {
      if (item != null) {
        out.add(String.valueOf(item));
      }
    }
    return out;
  }

  private List<Long> longList(Object value) {
    if (!(value instanceof List<?> list)) {
      return List.of();
    }
    List<Long> out = new ArrayList<>();
    for (Object item : list) {
      Long parsed = longValue(item);
      if (parsed != null) {
        out.add(parsed);
      }
    }
    return out;
  }

  private String stringValue(Object value) {
    return value == null ? null : String.valueOf(value);
  }

  private Long longValue(Object value) {
    if (value instanceof Number number) {
      return number.longValue();
    }
    if (value == null) {
      return null;
    }
    try {
      return Long.parseLong(String.valueOf(value));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private Integer intValue(Object value) {
    if (value instanceof Number number) {
      return number.intValue();
    }
    if (value == null) {
      return null;
    }
    try {
      return Integer.parseInt(String.valueOf(value));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private List<Integer> intList(Object value) {
    List<Integer> out = new ArrayList<>();
    if (value instanceof List<?> list) {
      for (Object item : list) {
        Integer parsed = intValue(item);
        if (parsed != null) {
          out.add(parsed);
        }
      }
    }
    return out;
  }
}
