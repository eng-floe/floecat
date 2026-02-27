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

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableMetadataBuilder;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataException;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataResult;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableCommitMaterializationService {
  private static final Logger LOG = Logger.getLogger(TableCommitMaterializationService.class);

  @Inject MaterializeMetadataService materializeMetadataService;
  @Inject IcebergGatewayConfig config;
  @Inject TableMetadataImportService tableMetadataImportService;
  @Inject TableGatewaySupport tableGatewaySupport;

  public MaterializeMetadataResult materializeMetadata(
      String namespace,
      ResourceId tableId,
      String table,
      Table tableRecord,
      TableMetadataView metadata,
      String metadataLocation) {
    if (metadata == null) {
      return MaterializeMetadataResult.failure(
          IcebergErrorResponses.validation("metadata is required for materialization"));
    }
    if (!hasText(metadataLocation) && !hasText(metadata.metadataLocation())) {
      metadataLocation = deriveMetadataLocation(tableRecord, metadata);
    }
    if (!hasText(metadataLocation) && !hasText(metadata.metadataLocation())) {
      metadataLocation = deriveDefaultMetadataLocation(namespace, table);
    }
    metadata = preferExistingTableDefinitions(table, tableRecord, metadata);
    metadata = ensureTableLocation(namespace, table, tableRecord, metadata, metadataLocation);
    metadata = ensureRequiredMetadataFields(metadata);
    boolean requestedLocation = hasText(metadataLocation) || hasText(metadata.metadataLocation());
    try {
      MaterializeMetadataService.MaterializeResult materializeResult =
          materializeMetadataService.materialize(namespace, table, metadata, metadataLocation);
      String resolvedLocation = materializeResult.metadataLocation();
      if (resolvedLocation == null || resolvedLocation.isBlank()) {
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

  private String deriveMetadataLocation(Table tableRecord, TableMetadataView metadata) {
    String location = tableLocation(tableRecord);
    if (location == null && metadata != null) {
      location = metadata.location();
    }
    if (location == null || location.isBlank()) {
      return null;
    }
    String base = stripTrailingSlash(location);
    if (base.toLowerCase(Locale.ROOT).endsWith("/metadata")) {
      return base + "/";
    }
    return base + "/metadata/";
  }

  private String stripTrailingSlash(String value) {
    if (value == null || value.length() <= 1) {
      return value;
    }
    return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
  }

  private String tableLocation(Table tableRecord) {
    if (tableRecord == null) {
      return null;
    }
    if (tableRecord.hasUpstream()) {
      String uri = tableRecord.getUpstream().getUri();
      if (uri != null && !uri.isBlank()) {
        return uri;
      }
    }
    Map<String, String> props = tableRecord.getPropertiesMap();
    String location = props.get("location");
    if (location != null && !location.isBlank()) {
      return location;
    }
    return null;
  }

  private String deriveDefaultMetadataLocation(String namespace, String tableName) {
    if (config == null || tableName == null || tableName.isBlank()) {
      return null;
    }
    String warehouse = config.defaultWarehousePath().orElse(null);
    if (warehouse == null || warehouse.isBlank()) {
      return null;
    }
    StringBuilder path = new StringBuilder(stripTrailingSlash(warehouse));
    if (namespace != null && !namespace.isBlank()) {
      for (String segment : namespace.split("\\.")) {
        if (segment != null && !segment.isBlank()) {
          path.append('/').append(segment);
        }
      }
    }
    path.append('/').append(tableName).append("/metadata/");
    return path.toString();
  }

  private TableMetadataView ensureTableLocation(
      String namespace,
      String tableName,
      Table tableRecord,
      TableMetadataView metadata,
      String metadataLocation) {
    if (metadata == null || hasText(metadata.location())) {
      return metadata;
    }
    String resolvedLocation =
        firstNonBlank(
            tableLocation(tableRecord),
            metadata.properties() == null ? null : metadata.properties().get("location"),
            deriveTableLocationFromMetadataLocation(metadataLocation),
            deriveDefaultTableLocation(namespace, tableName));
    if (!hasText(resolvedLocation)) {
      return metadata;
    }
    return new TableMetadataView(
        metadata.formatVersion(),
        metadata.tableUuid(),
        resolvedLocation,
        metadata.metadataLocation(),
        metadata.lastUpdatedMs(),
        metadata.properties(),
        metadata.lastColumnId(),
        metadata.currentSchemaId(),
        metadata.defaultSpecId(),
        metadata.lastPartitionId(),
        metadata.defaultSortOrderId(),
        metadata.currentSnapshotId(),
        metadata.lastSequenceNumber(),
        metadata.schemas(),
        metadata.partitionSpecs(),
        metadata.sortOrders(),
        metadata.refs(),
        metadata.snapshotLog(),
        metadata.metadataLog(),
        metadata.statistics(),
        metadata.partitionStatistics(),
        metadata.snapshots());
  }

  private String deriveDefaultTableLocation(String namespace, String tableName) {
    if (config == null || tableName == null || tableName.isBlank()) {
      return null;
    }
    String warehouse = config.defaultWarehousePath().orElse(null);
    if (warehouse == null || warehouse.isBlank()) {
      return null;
    }
    StringBuilder path = new StringBuilder(stripTrailingSlash(warehouse));
    if (namespace != null && !namespace.isBlank()) {
      for (String segment : namespace.split("\\.")) {
        if (segment != null && !segment.isBlank()) {
          path.append('/').append(segment);
        }
      }
    }
    path.append('/').append(tableName);
    return path.toString();
  }

  private String deriveTableLocationFromMetadataLocation(String metadataLocation) {
    if (!hasText(metadataLocation)) {
      return null;
    }
    String trimmed = stripTrailingSlash(metadataLocation);
    if (!hasText(trimmed)) {
      return null;
    }
    if (trimmed.endsWith(".metadata.json")) {
      int slash = trimmed.lastIndexOf('/');
      if (slash > 0) {
        trimmed = trimmed.substring(0, slash);
      }
    }
    if (trimmed.endsWith("/metadata")) {
      return trimmed.substring(0, trimmed.length() - "/metadata".length());
    }
    return null;
  }

  private String firstNonBlank(String... values) {
    if (values == null) {
      return null;
    }
    for (String value : values) {
      if (hasText(value)) {
        return value;
      }
    }
    return null;
  }

  private TableMetadataView ensureRequiredMetadataFields(TableMetadataView metadata) {
    if (metadata == null) {
      return null;
    }
    List<Map<String, Object>> schemas = ensureSchemas(metadata.schemas());
    Integer currentSchemaId = firstNonNull(metadata.currentSchemaId(), firstSchemaId(schemas), 0);
    Integer lastColumnId =
        firstNonNull(metadata.lastColumnId(), maxColumnIdFromSchemas(schemas), 0);
    List<Map<String, Object>> partitionSpecs = ensurePartitionSpecs(metadata.partitionSpecs());
    Integer defaultSpecId = firstNonNull(metadata.defaultSpecId(), firstSpecId(partitionSpecs), 0);
    Integer lastPartitionId =
        firstNonNull(metadata.lastPartitionId(), maxPartitionFieldId(partitionSpecs), 0);
    List<Map<String, Object>> sortOrders = ensureSortOrders(metadata.sortOrders());
    Integer defaultSortOrderId =
        firstNonNull(metadata.defaultSortOrderId(), firstSortOrderId(sortOrders), 0);
    Integer formatVersion = metadata.formatVersion();

    return new TableMetadataView(
        formatVersion,
        metadata.tableUuid(),
        metadata.location(),
        metadata.metadataLocation(),
        metadata.lastUpdatedMs(),
        metadata.properties(),
        lastColumnId,
        currentSchemaId,
        defaultSpecId,
        lastPartitionId,
        defaultSortOrderId,
        metadata.currentSnapshotId(),
        metadata.lastSequenceNumber(),
        schemas,
        partitionSpecs,
        sortOrders,
        metadata.refs(),
        metadata.snapshotLog(),
        metadata.metadataLog(),
        metadata.statistics(),
        metadata.partitionStatistics(),
        metadata.snapshots());
  }

  private TableMetadataView preferExistingTableDefinitions(
      String tableName, Table tableRecord, TableMetadataView metadata) {
    if (metadata == null || tableRecord == null || tableMetadataImportService == null) {
      return metadata;
    }
    boolean missingSchemas = metadata.schemas() == null || metadata.schemas().isEmpty();
    boolean missingSpecs = metadata.partitionSpecs() == null || metadata.partitionSpecs().isEmpty();
    boolean missingSortOrders = metadata.sortOrders() == null || metadata.sortOrders().isEmpty();
    if (!missingSchemas && !missingSpecs && !missingSortOrders) {
      return metadata;
    }
    String existingMetadataLocation =
        MetadataLocationUtil.metadataLocation(tableRecord.getPropertiesMap());
    if (!hasText(existingMetadataLocation)) {
      return metadata;
    }
    try {
      Map<String, String> ioProps = new LinkedHashMap<>();
      if (tableGatewaySupport != null) {
        ioProps.putAll(tableGatewaySupport.defaultFileIoProperties());
      }
      ioProps.putAll(FileIoFactory.filterIoProperties(tableRecord.getPropertiesMap()));
      var imported = tableMetadataImportService.importMetadata(existingMetadataLocation, ioProps);
      TableMetadataView existing =
          TableMetadataBuilder.fromCatalog(
              tableName,
              tableRecord,
              new LinkedHashMap<>(tableRecord.getPropertiesMap()),
              imported.icebergMetadata(),
              List.of());
      return new TableMetadataView(
          metadata.formatVersion(),
          metadata.tableUuid(),
          metadata.location(),
          metadata.metadataLocation(),
          metadata.lastUpdatedMs(),
          metadata.properties(),
          firstNonNull(metadata.lastColumnId(), existing.lastColumnId()),
          firstNonNull(metadata.currentSchemaId(), existing.currentSchemaId()),
          firstNonNull(metadata.defaultSpecId(), existing.defaultSpecId()),
          firstNonNull(metadata.lastPartitionId(), existing.lastPartitionId()),
          firstNonNull(metadata.defaultSortOrderId(), existing.defaultSortOrderId()),
          metadata.currentSnapshotId(),
          metadata.lastSequenceNumber(),
          missingSchemas ? existing.schemas() : metadata.schemas(),
          missingSpecs ? existing.partitionSpecs() : metadata.partitionSpecs(),
          missingSortOrders ? existing.sortOrders() : metadata.sortOrders(),
          metadata.refs(),
          metadata.snapshotLog(),
          metadata.metadataLog(),
          metadata.statistics(),
          metadata.partitionStatistics(),
          metadata.snapshots());
    } catch (RuntimeException e) {
      LOG.debugf(
          e,
          "Unable to import existing table metadata for definition fallback table=%s location=%s",
          tableName,
          existingMetadataLocation);
      return metadata;
    }
  }

  private List<Map<String, Object>> ensureSchemas(List<Map<String, Object>> schemas) {
    if (schemas != null && !schemas.isEmpty()) {
      return schemas;
    }
    Map<String, Object> schema = new LinkedHashMap<>();
    schema.put("type", "struct");
    schema.put("schema-id", 0);
    schema.put("fields", List.of());
    return List.of(Map.copyOf(schema));
  }

  private List<Map<String, Object>> ensurePartitionSpecs(List<Map<String, Object>> specs) {
    if (specs != null && !specs.isEmpty()) {
      return specs;
    }
    Map<String, Object> spec = new LinkedHashMap<>();
    spec.put("spec-id", 0);
    spec.put("fields", List.of());
    return List.of(Map.copyOf(spec));
  }

  private List<Map<String, Object>> ensureSortOrders(List<Map<String, Object>> sortOrders) {
    if (sortOrders != null && !sortOrders.isEmpty()) {
      return sortOrders;
    }
    Map<String, Object> order = new LinkedHashMap<>();
    order.put("order-id", 0);
    order.put("fields", List.of());
    return List.of(Map.copyOf(order));
  }

  private Integer firstSchemaId(List<Map<String, Object>> schemas) {
    if (schemas == null || schemas.isEmpty()) {
      return null;
    }
    return asInteger(schemas.getFirst().get("schema-id"));
  }

  private Integer firstSpecId(List<Map<String, Object>> specs) {
    if (specs == null || specs.isEmpty()) {
      return null;
    }
    return asInteger(specs.getFirst().get("spec-id"));
  }

  private Integer firstSortOrderId(List<Map<String, Object>> sortOrders) {
    if (sortOrders == null || sortOrders.isEmpty()) {
      return null;
    }
    return asInteger(sortOrders.getFirst().get("order-id"));
  }

  private Integer maxColumnIdFromSchemas(List<Map<String, Object>> schemas) {
    if (schemas == null || schemas.isEmpty()) {
      return null;
    }
    int max = -1;
    for (Map<String, Object> schema : schemas) {
      if (schema == null) {
        continue;
      }
      int schemaMax = maxColumnIdFromFields(asListOfMaps(schema.get("fields")));
      if (schemaMax > max) {
        max = schemaMax;
      }
    }
    return max < 0 ? null : max;
  }

  private int maxColumnIdFromFields(List<Map<String, Object>> fields) {
    int max = -1;
    for (Map<String, Object> field : fields) {
      if (field == null) {
        continue;
      }
      Integer id = asInteger(field.get("id"));
      if (id != null && id > max) {
        max = id;
      }
      Object nestedType = field.get("type");
      if (nestedType instanceof Map<?, ?> nestedMap) {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nestedFields =
            asListOfMaps(((Map<String, Object>) nestedMap).get("fields"));
        int nestedMax = maxColumnIdFromFields(nestedFields);
        if (nestedMax > max) {
          max = nestedMax;
        }
      }
    }
    return max;
  }

  private Integer maxPartitionFieldId(List<Map<String, Object>> specs) {
    if (specs == null || specs.isEmpty()) {
      return null;
    }
    int max = -1;
    for (Map<String, Object> spec : specs) {
      if (spec == null) {
        continue;
      }
      for (Map<String, Object> field : asListOfMaps(spec.get("fields"))) {
        Integer fieldId = asInteger(field == null ? null : field.get("field-id"));
        if (fieldId != null && fieldId > max) {
          max = fieldId;
        }
      }
    }
    return max < 0 ? null : max;
  }

  private List<Map<String, Object>> asListOfMaps(Object value) {
    if (!(value instanceof List<?> list) || list.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>(list.size());
    for (Object entry : list) {
      if (entry instanceof Map<?, ?> raw) {
        @SuppressWarnings("unchecked")
        Map<String, Object> typed = (Map<String, Object>) raw;
        out.add(typed);
      }
    }
    return out;
  }

  private Integer asInteger(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number n) {
      return n.intValue();
    }
    try {
      return Integer.parseInt(String.valueOf(value));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  @SafeVarargs
  private final <T> T firstNonNull(T... values) {
    if (values == null) {
      return null;
    }
    for (T value : values) {
      if (value != null) {
        return value;
      }
    }
    return null;
  }
}
