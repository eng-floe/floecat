package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.CommitStageResolver;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataResult;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.SnapshotMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.StageCommitProcessor.StageCommitResult;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableCommitService {
  private static final Logger LOG = Logger.getLogger(TableCommitService.class);

  @Inject GrpcWithHeaders grpc;
  @Inject SnapshotClient snapshotClient;
  @Inject TableLifecycleService tableLifecycleService;
  @Inject TableCommitSideEffectService sideEffectService;
  @Inject StageMaterializationService stageMaterializationService;
  @Inject CommitStageResolver stageResolver;
  @Inject TableUpdatePlanner tableUpdatePlanner;
  @Inject SnapshotMetadataService snapshotMetadataService;
  @Inject TableMetadataImportService tableMetadataImportService;

  public Response commit(CommitCommand command) {
    String prefix = command.prefix();
    String namespace = command.namespace();
    List<String> namespacePath = command.namespacePath();
    String table = command.table();
    ResourceId catalogId = command.catalogId();
    ResourceId namespaceId = command.namespaceId();
    String idempotencyKey = command.idempotencyKey();
    String transactionId = command.transactionId();
    TableRequests.Commit req = command.request();
    TableGatewaySupport tableSupport = command.tableSupport();

    CommitStageResolver.StageResolution stageResolution = stageResolver.resolve(command);
    if (stageResolution.hasError()) {
      return stageResolution.error();
    }
    ResourceId tableId = stageResolution.tableId();
    Supplier<Table> tableSupplier = createTableSupplier(stageResolution.stagedTable(), tableId);

    TableUpdatePlanner.UpdatePlan updatePlan =
        tableUpdatePlanner.planUpdates(command, tableSupplier, tableId);
    if (updatePlan.hasError()) {
      return updatePlan.error();
    }

    Table committedTable =
        applyTableUpdates(tableSupplier, tableId, updatePlan.spec(), updatePlan.mask());
    Map<String, String> ioProps =
        committedTable == null
            ? Map.of()
            : FileIoFactory.filterIoProperties(committedTable.getPropertiesMap());
    LOG.infof("Commit table io props namespace=%s.%s props=%s", namespace, table, ioProps);
    StageCommitResult stageMaterialization = stageResolution.stageCommitResult();
    IcebergMetadata metadata = tableSupport.loadCurrentMetadata(committedTable);
    String requestedMetadataOverride = resolveRequestedMetadataLocation(req);
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
        TableResponseMapper.toCommitResponse(table, committedTable, metadata, snapshotList);
    CommitTableResponseDto stageAwareResponse =
        containsSnapshotUpdates(req)
            ? initialResponse
            : preferStageMetadata(initialResponse, stageMaterialization);
    stageAwareResponse =
        normalizeMetadataLocation(tableSupport, committedTable, stageAwareResponse);
    stageAwareResponse =
        preferRequestedMetadata(tableSupport, stageAwareResponse, requestedMetadataOverride);
    stageAwareResponse = preferRequestedSequence(stageAwareResponse, req);
    stageAwareResponse = preferSnapshotSequence(stageAwareResponse, req);
    stageAwareResponse = mergeSnapshotUpdates(stageAwareResponse, req);
    if (LOG.isDebugEnabled() && stageAwareResponse != null) {
      TableMetadataView debugMeta = stageAwareResponse.metadata();
      Long debugLastSeq = debugMeta == null ? null : debugMeta.lastSequenceNumber();
      String debugPropSeq =
          debugMeta == null || debugMeta.properties() == null
              ? null
              : debugMeta.properties().get("last-sequence-number");
      LOG.debugf(
          "Commit response sequence debug namespace=%s table=%s reqSeq=%s metaSeq=%s propSeq=%s",
          namespace, table, maxSequenceNumber(req), debugLastSeq, debugPropSeq);
    }
    var sideEffects =
        sideEffectService.finalizeCommitResponse(
            namespace, table, tableId, committedTable, stageAwareResponse, false);
    if (sideEffects.hasError()) {
      return sideEffects.error();
    }
    CommitTableResponseDto responseDto = sideEffects.response();

    ResourceId connectorId =
        sideEffectService.synchronizeConnector(
            tableSupport,
            prefix,
            namespacePath,
            namespaceId,
            catalogId,
            table,
            committedTable,
            responseDto == null ? null : responseDto.metadata(),
            responseDto == null ? null : responseDto.metadataLocation(),
            idempotencyKey);
    syncExternalSnapshotsIfNeeded(
        tableSupport,
        tableId,
        namespacePath,
        table,
        committedTable,
        responseDto,
        req,
        idempotencyKey);
    runConnectorSync(tableSupport, connectorId, namespacePath, table);

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
            table, committedTable, refreshedMetadata, refreshedSnapshots);
    String preferredLocation = responseDto == null ? null : responseDto.metadataLocation();
    if (preferredLocation != null && !preferredLocation.isBlank()) {
      TableMetadataView finalMetadata = finalResponse.metadata();
      if (finalMetadata != null) {
        finalMetadata = finalMetadata.withMetadataLocation(preferredLocation);
      }
      finalResponse = new CommitTableResponseDto(preferredLocation, finalMetadata);
    } else {
      if (!containsSnapshotUpdates(req)) {
        finalResponse = preferStageMetadata(finalResponse, stageMaterialization);
      }
      finalResponse = normalizeMetadataLocation(tableSupport, committedTable, finalResponse);
      finalResponse =
          preferRequestedMetadata(tableSupport, finalResponse, requestedMetadataOverride);
    }

    Response.ResponseBuilder builder = Response.ok(finalResponse);
    LOG.infof(
        "Commit response for %s.%s tableId=%s currentSnapshot=%s snapshotCount=%d",
        namespace,
        table,
        committedTable.hasResourceId() ? committedTable.getResourceId().getId() : "<missing>",
        refreshedMetadata != null ? refreshedMetadata.getCurrentSnapshotId() : "<null>",
        refreshedSnapshots == null ? 0 : refreshedSnapshots.size());
    if (finalResponse != null && finalResponse.metadataLocation() != null) {
      builder.tag(finalResponse.metadataLocation());
    }
    logStageCommit(
        stageMaterialization,
        nonBlank(
            stageResolution.materializedStageId(),
            stageMaterializationService.resolveStageId(req, transactionId)),
        namespace,
        table,
        finalResponse == null ? null : finalResponse.metadata());
    return builder.build();
  }

  private void logStageCommit(
      StageCommitResult stageMaterialization,
      String stageId,
      String namespace,
      String table,
      TableMetadataView metadata) {
    if (stageMaterialization == null) {
      return;
    }
    Table staged = stageMaterialization.table();
    String tableId =
        staged != null && staged.hasResourceId() ? staged.getResourceId().getId() : "<missing>";
    String snapshotStr =
        metadata == null || metadata.currentSnapshotId() == null
            ? "<null>"
            : metadata.currentSnapshotId().toString();
    int snapshotCount =
        metadata == null || metadata.snapshots() == null ? 0 : metadata.snapshots().size();
    LOG.infof(
        "Stage commit satisfied for %s.%s tableId=%s stageId=%s currentSnapshot=%s"
            + " snapshotCount=%d",
        namespace, table, tableId, stageId, snapshotStr, snapshotCount);
  }

  private boolean containsRemoveSnapshots(TableRequests.Commit req) {
    if (req == null || req.updates() == null) {
      return false;
    }
    for (Map<String, Object> update : req.updates()) {
      String action = update == null ? null : String.valueOf(update.get("action"));
      if ("remove-snapshots".equals(action)) {
        return true;
      }
    }
    return false;
  }

  private boolean containsSnapshotUpdates(TableRequests.Commit req) {
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

  private Set<Long> removedSnapshotIds(TableRequests.Commit req) {
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

  private void syncExternalSnapshotsIfNeeded(
      TableGatewaySupport tableSupport,
      ResourceId tableId,
      List<String> namespacePath,
      String tableName,
      Table committedTable,
      CommitTableResponseDto responseDto,
      TableRequests.Commit req,
      String idempotencyKey) {
    String metadataLocation = responseDto == null ? null : responseDto.metadataLocation();
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      metadataLocation = tableSupport.stripMetadataMirrorPrefix(metadataLocation);
    }
    if (!isExternalLocationTable(committedTable, metadataLocation, tableSupport)) {
      return;
    }
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return;
    }
    String previousLocation =
        committedTable == null
            ? null
            : MetadataLocationUtil.metadataLocation(committedTable.getPropertiesMap());
    if (previousLocation != null && !previousLocation.isBlank()) {
      previousLocation = tableSupport.stripMetadataMirrorPrefix(previousLocation);
    }
    if (previousLocation != null && metadataLocation.equals(previousLocation)) {
      return;
    }
    try {
      Map<String, String> ioProps =
          committedTable == null
              ? Map.of()
              : FileIoFactory.filterIoProperties(committedTable.getPropertiesMap());
      var imported = tableMetadataImportService.importMetadata(metadataLocation, ioProps);
      snapshotMetadataService.syncSnapshotsFromImportedMetadata(
          tableSupport,
          tableId,
          namespacePath,
          tableName,
          () -> committedTable,
          imported,
          idempotencyKey,
          true);
    } catch (Exception e) {
      LOG.warnf(
          e,
          "Snapshot sync from metadata failed for %s.%s (metadata=%s)",
          namespacePath,
          tableName,
          metadataLocation);
    }
  }

  private boolean isExternalLocationTable(
      Table table, String metadataLocation, TableGatewaySupport tableSupport) {
    if (table == null) {
      return false;
    }
    if (!table.hasUpstream()) {
      if (metadataLocation == null || metadataLocation.isBlank()) {
        return false;
      }
      return tableSupport == null || !tableSupport.isMirrorMetadataLocation(metadataLocation);
    }
    String uri = table.getUpstream().getUri();
    return uri != null && !uri.isBlank();
  }

  private CommitTableResponseDto preferStageMetadata(
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
      String responseLocation = response == null ? null : response.metadataLocation();
      return new CommitTableResponseDto(responseLocation, stagedMetadata);
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
    return new CommitTableResponseDto(stagedLocation, stagedMetadata);
  }

  private CommitTableResponseDto preferRequestedMetadata(
      TableGatewaySupport tableSupport,
      CommitTableResponseDto response,
      String requestedMetadataLocation) {
    if (requestedMetadataLocation == null || requestedMetadataLocation.isBlank()) {
      return response;
    }
    String resolved = tableSupport.stripMetadataMirrorPrefix(requestedMetadataLocation);
    if (resolved == null || resolved.isBlank()) {
      return response;
    }
    TableMetadataView metadata = response == null ? null : response.metadata();
    if (metadata != null) {
      metadata = metadata.withMetadataLocation(resolved);
    }
    if (response != null && resolved.equals(response.metadataLocation())) {
      if (Objects.equals(metadata, response.metadata())) {
        return response;
      }
    }
    return new CommitTableResponseDto(resolved, metadata);
  }

  private CommitTableResponseDto normalizeMetadataLocation(
      TableGatewaySupport tableSupport, Table tableRecord, CommitTableResponseDto response) {
    if (response == null || response.metadataLocation() == null) {
      return response;
    }
    String metadataLocation = response.metadataLocation();
    if (!tableSupport.isMirrorMetadataLocation(metadataLocation)) {
      return response;
    }
    String resolvedLocation = tableSupport.stripMetadataMirrorPrefix(metadataLocation);
    TableMetadataView metadata = response.metadata();
    if (metadata != null) {
      metadata = metadata.withMetadataLocation(resolvedLocation);
    }
    return new CommitTableResponseDto(resolvedLocation, metadata);
  }

  private CommitTableResponseDto preferSnapshotSequence(
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
    return new CommitTableResponseDto(response.metadataLocation(), updated);
  }

  private CommitTableResponseDto preferRequestedSequence(
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
    return new CommitTableResponseDto(response.metadataLocation(), updated);
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

  private Long maxSequenceNumber(TableRequests.Commit req) {
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

  private CommitTableResponseDto mergeSnapshotUpdates(
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
    return new CommitTableResponseDto(response.metadataLocation(), updated);
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

  private static String asString(Object value) {
    return value == null ? null : String.valueOf(value);
  }

  private String resolveRequestedMetadataLocation(TableRequests.Commit req) {
    if (req == null) {
      return null;
    }
    String location = MetadataLocationUtil.metadataLocation(req.properties());
    String updateLocation = metadataLocationFromUpdates(req.updates());
    return nonBlank(updateLocation, location);
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

  public MaterializeMetadataResult materializeMetadata(
      String namespace,
      ResourceId tableId,
      String table,
      Table tableRecord,
      TableMetadataView metadata,
      String metadataLocation) {
    return sideEffectService.materializeMetadata(
        namespace, tableId, table, tableRecord, metadata, metadataLocation);
  }

  public void runConnectorSync(
      TableGatewaySupport tableSupport,
      ResourceId connectorId,
      List<String> namespacePath,
      String tableName) {
    if (LOG.isDebugEnabled()) {
      String namespace =
          namespacePath == null
              ? "<missing>"
              : (namespacePath.isEmpty() ? "<empty>" : String.join(".", namespacePath));
      String connector =
          connectorId == null || connectorId.getId().isBlank() ? "<missing>" : connectorId.getId();
      LOG.debugf(
          "Connector sync request namespace=%s table=%s connectorId=%s",
          namespace, tableName == null ? "<missing>" : tableName, connector);
    }
    try {
      sideEffectService.runConnectorSync(tableSupport, connectorId, namespacePath, tableName);
    } catch (Throwable e) {
      String namespace =
          namespacePath == null
              ? "<missing>"
              : (namespacePath.isEmpty() ? "<empty>" : String.join(".", namespacePath));
      LOG.warnf(
          e,
          "Post-commit connector sync failed for %s.%s",
          namespace,
          tableName == null ? "<missing>" : tableName);
    }
  }

  public record CommitCommand(
      String prefix,
      String namespace,
      List<String> namespacePath,
      String table,
      String catalogName,
      ResourceId catalogId,
      ResourceId namespaceId,
      String idempotencyKey,
      String transactionId,
      TableRequests.Commit request,
      TableGatewaySupport tableSupport) {}

  private Supplier<Table> createTableSupplier(Table stagedTable, ResourceId tableId) {
    return new Supplier<>() {
      private Table cached = stagedTable;

      @Override
      public Table get() {
        if (cached == null) {
          cached = tableLifecycleService.getTable(tableId);
        }
        return cached;
      }
    };
  }

  private Table applyTableUpdates(
      Supplier<Table> tableSupplier,
      ResourceId tableId,
      TableSpec.Builder spec,
      FieldMask.Builder mask) {
    if (mask.getPathsCount() == 0) {
      return tableSupplier.get();
    }
    UpdateTableRequest.Builder updateRequest =
        UpdateTableRequest.newBuilder().setTableId(tableId).setSpec(spec).setUpdateMask(mask);
    return tableLifecycleService.updateTable(updateRequest.build());
  }

  private static String nonBlank(String primary, String fallback) {
    return primary != null && !primary.isBlank() ? primary : fallback;
  }
}
