package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asString;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.StageCommitProcessor.StageCommitResult;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.jboss.logging.Logger;

@ApplicationScoped
public class CommitResponseBuilder {
  private static final Logger LOG = Logger.getLogger(CommitResponseBuilder.class);

  @Inject SnapshotClient snapshotClient;

  void setSnapshotClient(SnapshotClient snapshotClient) {
    this.snapshotClient = snapshotClient;
  }

  public String resolveRequestedMetadataLocation(TableRequests.Commit req) {
    if (req == null) {
      return null;
    }
    String location = MetadataLocationUtil.metadataLocation(req.properties());
    String updateLocation = metadataLocationFromUpdates(req.updates());
    return nonBlank(updateLocation, location);
  }

  public Set<Long> removedSnapshotIds(TableRequests.Commit req) {
    if (req == null || req.updates() == null) {
      return Set.of();
    }
    Set<Long> ids = new LinkedHashSet<>();
    for (Map<String, Object> update : req.updates()) {
      String action = update == null ? null : String.valueOf(update.get("action"));
      if (!"remove-snapshots".equals(action)) {
        continue;
      }
      Object raw = update.get("snapshot-ids");
      if (raw instanceof List<?> list) {
        for (Object item : list) {
          Long val = parseLong(item);
          if (val != null) {
            ids.add(val);
          }
        }
      }
    }
    return ids.isEmpty() ? Set.of() : Set.copyOf(ids);
  }

  public boolean containsSnapshotUpdates(TableRequests.Commit req) {
    if (req == null || req.updates() == null) {
      return false;
    }
    for (Map<String, Object> update : req.updates()) {
      String action = update == null ? null : String.valueOf(update.get("action"));
      if ("add-snapshot".equals(action)
          || "remove-snapshots".equals(action)
          || "set-snapshot-ref".equals(action)
          || "remove-snapshot-ref".equals(action)) {
        return true;
      }
    }
    return false;
  }

  public CommitTableResponseDto buildInitialResponse(
      String tableName,
      Table committedTable,
      ResourceId tableId,
      StageCommitResult stageMaterialization,
      TableRequests.Commit req,
      TableGatewaySupport tableSupport,
      IcebergMetadata metadata) {
    List<Snapshot> snapshotList =
        SnapshotLister.fetchSnapshots(snapshotClient, tableId, SnapshotLister.Mode.ALL, metadata);
    Set<Long> removedSnapshotIds = removedSnapshotIds(req);
    if (!removedSnapshotIds.isEmpty() && snapshotList != null && !snapshotList.isEmpty()) {
      snapshotList =
          snapshotList.stream()
              .filter(s -> !removedSnapshotIds.contains(s.getSnapshotId()))
              .toList();
    }
    CommitTableResponseDto initialResponse =
        TableResponseMapper.toCommitResponse(tableName, committedTable, metadata, snapshotList);
    CommitTableResponseDto stageAwareResponse =
        containsSnapshotUpdates(req)
            ? initialResponse
            : preferStageMetadata(initialResponse, stageMaterialization);
    stageAwareResponse = normalizeMetadataLocation(stageAwareResponse);
    stageAwareResponse =
        preferRequestedMetadata(stageAwareResponse, resolveRequestedMetadataLocation(req));
    stageAwareResponse = preferRequestedSequence(stageAwareResponse, req);
    stageAwareResponse = preferSnapshotSequence(stageAwareResponse, req);
    return mergeSnapshotUpdates(stageAwareResponse, req);
  }

  public CommitTableResponseDto buildFinalResponse(
      String tableName,
      Table committedTable,
      ResourceId tableId,
      StageCommitResult stageMaterialization,
      TableRequests.Commit req,
      TableGatewaySupport tableSupport,
      Set<Long> removedSnapshotIds) {
    IcebergMetadata refreshedMetadata = tableSupport.loadCurrentMetadata(committedTable);
    List<Snapshot> refreshedSnapshots =
        SnapshotLister.fetchSnapshots(
            snapshotClient, tableId, SnapshotLister.Mode.ALL, refreshedMetadata);
    if (!removedSnapshotIds.isEmpty()
        && refreshedSnapshots != null
        && !refreshedSnapshots.isEmpty()) {
      refreshedSnapshots =
          refreshedSnapshots.stream()
              .filter(s -> !removedSnapshotIds.contains(s.getSnapshotId()))
              .toList();
    }
    CommitTableResponseDto finalResponse =
        TableResponseMapper.toCommitResponse(
            tableName, committedTable, refreshedMetadata, refreshedSnapshots);
    if (!containsSnapshotUpdates(req)) {
      finalResponse = preferStageMetadata(finalResponse, stageMaterialization);
    }
    finalResponse = normalizeMetadataLocation(finalResponse);
    finalResponse = preferRequestedMetadata(finalResponse, resolveRequestedMetadataLocation(req));
    return finalResponse;
  }

  public CommitTableResponseDto preferStageMetadata(
      CommitTableResponseDto response, StageCommitResult stageMaterialization) {
    if (stageMaterialization == null || stageMaterialization.loadResult() == null) {
      return response;
    }
    LoadTableResultDto staged = stageMaterialization.loadResult();
    TableMetadataView stagedMetadata = staged.metadata();
    String stagedLocation = staged.metadataLocation();
    if ((stagedLocation == null || stagedLocation.isBlank())
        && stagedMetadata != null
        && stagedMetadata.metadataLocation() != null
        && !stagedMetadata.metadataLocation().isBlank()) {
      stagedLocation = stagedMetadata.metadataLocation();
    }
    if (stagedLocation == null || stagedLocation.isBlank()) {
      if (stagedMetadata == null) {
        return response;
      }
      boolean responseIncomplete =
          response == null
              || response.metadata() == null
              || response.metadata().formatVersion() == null
              || response.metadata().schemas() == null
              || response.metadata().schemas().isEmpty();
      if (!responseIncomplete) {
        return response;
      }
      return commitResponse(stagedMetadata);
    }
    String originalLocation = response == null ? "<null>" : response.metadataLocation();
    LOG.infof(
        "Stage metadata evaluation stagedLocation=%s originalLocation=%s",
        stagedLocation, originalLocation);
    if (stagedMetadata != null) {
      stagedMetadata = stagedMetadata.withMetadataLocation(stagedLocation);
    }
    if (response != null
        && stagedLocation.equals(response.metadataLocation())
        && Objects.equals(stagedMetadata, response.metadata())) {
      return response;
    }
    LOG.infof("Preferring staged metadata location %s over %s", stagedLocation, originalLocation);
    return commitResponse(stagedMetadata);
  }

  public CommitTableResponseDto preferRequestedMetadata(
      CommitTableResponseDto response, String requestedMetadataLocation) {
    if (requestedMetadataLocation == null || requestedMetadataLocation.isBlank()) {
      return response;
    }
    TableMetadataView metadata = response == null ? null : response.metadata();
    if (metadata != null) {
      metadata = metadata.withMetadataLocation(requestedMetadataLocation);
    }
    if (response != null && requestedMetadataLocation.equals(response.metadataLocation())) {
      if (Objects.equals(metadata, response.metadata())) {
        return response;
      }
    }
    return commitResponse(metadata);
  }

  public CommitTableResponseDto normalizeMetadataLocation(CommitTableResponseDto response) {
    return response;
  }

  public CommitTableResponseDto commitResponse(TableMetadataView metadata) {
    if (metadata == null) {
      return new CommitTableResponseDto(null, null);
    }
    return new CommitTableResponseDto(metadata.metadataLocation(), metadata);
  }

  public CommitTableResponseDto preferSnapshotSequence(
      CommitTableResponseDto response, TableRequests.Commit req) {
    if (response == null || response.metadata() == null) {
      return response;
    }
    Long latestSequence = maxSequenceNumber(req);
    if (latestSequence == null || latestSequence <= 0) {
      return response;
    }
    TableMetadataView metadata = response.metadata();
    Map<String, String> props =
        metadata.properties() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(metadata.properties());
    props.put("last-sequence-number", Long.toString(latestSequence));
    TableMetadataView updated =
        new TableMetadataView(
            metadata.formatVersion(),
            metadata.tableUuid(),
            metadata.location(),
            metadata.metadataLocation(),
            metadata.lastUpdatedMs(),
            Map.copyOf(props),
            metadata.lastColumnId(),
            metadata.currentSchemaId(),
            metadata.defaultSpecId(),
            metadata.lastPartitionId(),
            metadata.defaultSortOrderId(),
            metadata.currentSnapshotId(),
            latestSequence,
            metadata.schemas(),
            metadata.partitionSpecs(),
            metadata.sortOrders(),
            metadata.refs(),
            metadata.snapshotLog(),
            metadata.metadataLog(),
            metadata.statistics(),
            metadata.partitionStatistics(),
            metadata.snapshots());
    return commitResponse(updated);
  }

  public CommitTableResponseDto preferRequestedSequence(
      CommitTableResponseDto response, TableRequests.Commit req) {
    if (response == null || response.metadata() == null) {
      return response;
    }
    Long requested = requestedSequenceNumber(req);
    if (requested == null || requested <= 0) {
      return response;
    }
    TableMetadataView metadata = response.metadata();
    Long existing = metadata.lastSequenceNumber();
    if (existing != null && existing >= requested) {
      return response;
    }
    Map<String, String> props =
        metadata.properties() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(metadata.properties());
    props.put("last-sequence-number", Long.toString(requested));
    TableMetadataView updated =
        new TableMetadataView(
            metadata.formatVersion(),
            metadata.tableUuid(),
            metadata.location(),
            metadata.metadataLocation(),
            metadata.lastUpdatedMs(),
            Map.copyOf(props),
            metadata.lastColumnId(),
            metadata.currentSchemaId(),
            metadata.defaultSpecId(),
            metadata.lastPartitionId(),
            metadata.defaultSortOrderId(),
            metadata.currentSnapshotId(),
            requested,
            metadata.schemas(),
            metadata.partitionSpecs(),
            metadata.sortOrders(),
            metadata.refs(),
            metadata.snapshotLog(),
            metadata.metadataLog(),
            metadata.statistics(),
            metadata.partitionStatistics(),
            metadata.snapshots());
    return commitResponse(updated);
  }

  public CommitTableResponseDto mergeSnapshotUpdates(
      CommitTableResponseDto response, TableRequests.Commit req) {
    if (response == null || response.metadata() == null || req == null || req.updates() == null) {
      return response;
    }
    List<Map<String, Object>> addedSnapshots = extractSnapshots(req.updates());
    if (addedSnapshots.isEmpty()) {
      return response;
    }
    TableMetadataView metadata = response.metadata();
    List<Map<String, Object>> existing =
        metadata.snapshots() == null ? List.of() : metadata.snapshots();
    Map<Long, Map<String, Object>> merged = new LinkedHashMap<>();
    for (Map<String, Object> snapshot : existing) {
      Long id = snapshotId(snapshot);
      if (id != null) {
        merged.put(id, new LinkedHashMap<>(snapshot));
      }
    }
    for (Map<String, Object> snapshot : addedSnapshots) {
      Long id = snapshotId(snapshot);
      if (id == null) {
        continue;
      }
      merged.put(id, new LinkedHashMap<>(snapshot));
    }
    List<Map<String, Object>> updatedSnapshots =
        merged.isEmpty() ? List.of() : List.copyOf(merged.values());
    Long requestSequence = maxSequenceNumber(req);
    Long maxSequence =
        requestSequence != null && requestSequence > 0
            ? requestSequence
            : maxSequenceFromSnapshots(updatedSnapshots);
    Integer formatVersion = metadata.formatVersion();
    if (requestSequence != null && requestSequence > 0) {
      if (formatVersion == null || formatVersion < 2) {
        formatVersion = 2;
      }
    }
    Long currentSnapshotId =
        metadata.currentSnapshotId() == null ? latestSnapshotId(updatedSnapshots) : null;
    Map<String, String> props =
        metadata.properties() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(metadata.properties());
    if (maxSequence != null && maxSequence > 0) {
      props.put("last-sequence-number", Long.toString(maxSequence));
    }
    if (currentSnapshotId != null && currentSnapshotId > 0) {
      props.put("current-snapshot-id", Long.toString(currentSnapshotId));
    }
    TableMetadataView updated =
        new TableMetadataView(
            formatVersion,
            metadata.tableUuid(),
            metadata.location(),
            metadata.metadataLocation(),
            metadata.lastUpdatedMs(),
            Map.copyOf(props),
            metadata.lastColumnId(),
            metadata.currentSchemaId(),
            metadata.defaultSpecId(),
            metadata.lastPartitionId(),
            metadata.defaultSortOrderId(),
            currentSnapshotId != null ? currentSnapshotId : metadata.currentSnapshotId(),
            maxSequence != null ? maxSequence : metadata.lastSequenceNumber(),
            metadata.schemas(),
            metadata.partitionSpecs(),
            metadata.sortOrders(),
            metadata.refs(),
            metadata.snapshotLog(),
            metadata.metadataLog(),
            metadata.statistics(),
            metadata.partitionStatistics(),
            updatedSnapshots);
    return commitResponse(updated);
  }

  public Long maxSequenceNumber(TableRequests.Commit req) {
    if (req == null || req.updates() == null) {
      return null;
    }
    Long max = null;
    for (Map<String, Object> update : req.updates()) {
      if (update == null) {
        continue;
      }
      String action = asString(update.get("action"));
      if (!"add-snapshot".equals(action)) {
        continue;
      }
      @SuppressWarnings("unchecked")
      Map<String, Object> snapshot =
          update.get("snapshot") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
      if (snapshot == null) {
        continue;
      }
      Long sequence = parseLong(snapshot.get("sequence-number"));
      if (sequence == null || sequence <= 0) {
        continue;
      }
      max = max == null ? sequence : Math.max(max, sequence);
    }
    return max;
  }

  private Long requestedSequenceNumber(TableRequests.Commit req) {
    if (req == null) {
      return null;
    }
    Long max = null;
    Long direct =
        parseLong(req.properties() == null ? null : req.properties().get("last-sequence-number"));
    if (direct != null && direct > 0) {
      max = direct;
    }
    if (req.updates() == null) {
      return max;
    }
    for (Map<String, Object> update : req.updates()) {
      if (update == null) {
        continue;
      }
      String action = asString(update.get("action"));
      if (!"set-properties".equals(action)) {
        continue;
      }
      Map<String, String> updates = asStringMap(update.get("updates"));
      Long candidate = parseLong(updates.get("last-sequence-number"));
      if (candidate != null && candidate > 0) {
        max = max == null ? candidate : Math.max(max, candidate);
      }
    }
    return max;
  }

  private List<Map<String, Object>> extractSnapshots(List<Map<String, Object>> updates) {
    List<Map<String, Object>> out = new ArrayList<>();
    for (Map<String, Object> update : updates) {
      if (update == null) {
        continue;
      }
      String action = asString(update.get("action"));
      if (!"add-snapshot".equals(action)) {
        continue;
      }
      @SuppressWarnings("unchecked")
      Map<String, Object> snapshot =
          update.get("snapshot") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
      if (snapshot == null || snapshot.isEmpty()) {
        continue;
      }
      out.add(snapshot);
    }
    return out;
  }

  private Long snapshotId(Map<String, Object> snapshot) {
    if (snapshot == null) {
      return null;
    }
    return parseLong(snapshot.get("snapshot-id"));
  }

  private Long latestSnapshotId(List<Map<String, Object>> snapshots) {
    Long latest = null;
    for (Map<String, Object> snapshot : snapshots) {
      Long id = snapshotId(snapshot);
      if (id != null && id > 0) {
        latest = id;
      }
    }
    return latest;
  }

  private Long maxSequenceFromSnapshots(List<Map<String, Object>> snapshots) {
    Long max = null;
    for (Map<String, Object> snapshot : snapshots) {
      if (snapshot == null) {
        continue;
      }
      Long seq = parseLong(snapshot.get("sequence-number"));
      if (seq == null || seq <= 0) {
        continue;
      }
      max = max == null ? seq : Math.max(max, seq);
    }
    return max;
  }

  private Long parseLong(Object value) {
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

  private String metadataLocationFromUpdates(List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return null;
    }
    String location = null;
    for (Map<String, Object> update : updates) {
      if (update == null) {
        continue;
      }
      String action = asString(update.get("action"));
      if (!"set-properties".equals(action)) {
        continue;
      }
      Map<String, String> toSet = asStringMap(update.get("updates"));
      if (toSet.isEmpty()) {
        continue;
      }
      String candidate = MetadataLocationUtil.metadataLocation(toSet);
      if (candidate != null && !candidate.isBlank()) {
        location = candidate;
      }
    }
    return location;
  }

  @SuppressWarnings("unchecked")
  private Map<String, String> asStringMap(Object value) {
    if (!(value instanceof Map<?, ?> map) || map.isEmpty()) {
      return Map.of();
    }
    Map<String, String> converted = new LinkedHashMap<>();
    map.forEach(
        (k, v) -> {
          String key = asString(k);
          String strValue = asString(v);
          if (key != null && strValue != null) {
            converted.put(key, strValue);
          }
        });
    return converted;
  }

  private static String nonBlank(String primary, String fallback) {
    return primary != null && !primary.isBlank() ? primary : fallback;
  }
}
