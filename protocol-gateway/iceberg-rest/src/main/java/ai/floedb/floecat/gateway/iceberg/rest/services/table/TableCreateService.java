package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StageCreateResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.NamespaceRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StageState;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableEntry;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableKey;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableCreateService {
  private static final Logger LOG = Logger.getLogger(TableCreateService.class);
  private static final List<Map<String, Object>> STAGE_CREATE_REQUIREMENTS =
      List.of(Map.of("type", "assert-create"));

  @Inject TableLifecycleService tableLifecycleService;
  @Inject StagedTableService stagedTableService;
  @Inject AccountContext accountContext;
  @Inject IcebergGatewayConfig config;
  @Inject ObjectMapper mapper;

  public Response create(
      NamespaceRequestContext namespaceContext,
      String idempotencyKey,
      String transactionId,
      TableRequests.Create request,
      TableGatewaySupport tableSupport) {
    if (request == null) {
      return IcebergErrorResponses.validation("Request body is required");
    }
    if (request.name() == null || request.name().isBlank()) {
      return IcebergErrorResponses.validation("name is required");
    }
    if (!hasSchema(request)) {
      return IcebergErrorResponses.validation("schema is required");
    }

    String tableName = request.name().trim();
    TableRequests.Create effectiveReq =
        applyDefaultLocationIfMissing(
            namespaceContext.prefix(), namespaceContext.namespacePath(), tableName, request);
    if (Boolean.TRUE.equals(effectiveReq.stageCreate())) {
      return handleStageCreate(
          namespaceContext, tableName, effectiveReq, transactionId, idempotencyKey, tableSupport);
    }

    TableSpec.Builder spec;
    try {
      spec =
          tableSupport.buildCreateSpec(
              namespaceContext.catalogId(),
              namespaceContext.namespaceId(),
              tableName,
              effectiveReq);
    } catch (IllegalArgumentException | JsonProcessingException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    String specMetadataLocation = MetadataLocationUtil.metadataLocation(spec.getPropertiesMap());
    effectiveReq = ensureMetadataLocation(effectiveReq, specMetadataLocation);

    Table created = tableLifecycleService.createTable(spec, idempotencyKey);
    IcebergMetadata metadata = tableSupport.loadCurrentMetadata(created);
    Map<String, String> tableConfig = tableSupport.defaultTableConfig();
    List<StorageCredentialDto> credentials = tableSupport.defaultCredentials();
    LoadTableResultDto loadResult;
    if (metadata == null && request != null) {
      try {
        loadResult =
            TableResponseMapper.toLoadResultFromCreate(
                tableName, created, effectiveReq, tableConfig, credentials);
      } catch (IllegalArgumentException e) {
        return IcebergErrorResponses.validation(e.getMessage());
      }
    } else {
      loadResult =
          TableResponseMapper.toLoadResult(
              tableName, created, metadata, List.of(), tableConfig, credentials);
    }

    TableMetadataView responseMetadata = loadResult.metadata();
    String responseMetadataLocation = loadResult.metadataLocation();
    String actualMetadataLocation = metadataLocation(created, metadata);
    if (actualMetadataLocation != null && !actualMetadataLocation.isBlank()) {
      responseMetadataLocation = actualMetadataLocation;
    }
    if (responseMetadata != null && responseMetadataLocation != null) {
      responseMetadata = responseMetadata.withMetadataLocation(responseMetadataLocation);
    }
    Map<String, String> responseConfig =
        loadResult.config() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(loadResult.config());
    if (responseMetadataLocation != null && !responseMetadataLocation.isBlank()) {
      String metadataDirectory =
          MetadataLocationUtil.canonicalMetadataDirectory(responseMetadataLocation);
      if (metadataDirectory != null && !metadataDirectory.isBlank()) {
        responseConfig.put("write.metadata.path", metadataDirectory);
      }
    }
    loadResult =
        new LoadTableResultDto(
            responseMetadataLocation,
            responseMetadata,
            Map.copyOf(responseConfig),
            loadResult.storageCredentials());
    LOG.infof(
        "Create table response namespace=%s table=%s metadata=%s location=%s configKeys=%s",
        namespaceContext.namespacePath(),
        tableName,
        responseMetadata == null ? "<null>" : responseMetadata.metadataLocation(),
        responseMetadataLocation,
        loadResult.config().keySet());

    Response.ResponseBuilder builder = Response.ok(loadResult);
    String etagValue =
        responseMetadataLocation != null && !responseMetadataLocation.isBlank()
            ? responseMetadataLocation
            : metadataLocation(created, metadata);
    if (etagValue != null) {
      builder.tag(etagValue);
    }
    return builder.build();
  }

  private Response handleStageCreate(
      NamespaceRequestContext namespaceContext,
      String tableName,
      TableRequests.Create request,
      String transactionId,
      String idempotencyKey,
      TableGatewaySupport tableSupport) {
    if (request == null) {
      return IcebergErrorResponses.validation("stage-create requires a request body");
    }
    TableRequests.Create effectiveReq =
        applyDefaultLocationIfMissing(
            namespaceContext.prefix(), namespaceContext.namespacePath(), tableName, request);
    if (effectiveReq.location() == null || effectiveReq.location().isBlank()) {
      LOG.warnf(
          "Stage-create request missing location prefix=%s namespace=%s table=%s payload=%s",
          namespaceContext.prefix(),
          namespaceContext.namespacePath(),
          tableName,
          safeSerializeCreate(request));
      return IcebergErrorResponses.validation("location is required");
    }
    String accountId = accountContext.getAccountId();
    if (accountId == null || accountId.isBlank()) {
      return IcebergErrorResponses.validation("account context is required");
    }
    String stageId =
        (transactionId == null || transactionId.isBlank())
            ? UUID.randomUUID().toString()
            : transactionId.trim();
    try {
      LOG.infof(
          "Stage-create request payload prefix=%s namespace=%s table=%s stageId=%s location=%s"
              + " properties=%s",
          namespaceContext.prefix(),
          namespaceContext.namespacePath(),
          tableName,
          stageId,
          effectiveReq.location(),
          effectiveReq.properties());
      TableSpec.Builder specBuilder =
          tableSupport.buildCreateSpec(
              namespaceContext.catalogId(),
              namespaceContext.namespaceId(),
              tableName,
              effectiveReq);
      TableSpec spec = specBuilder.build();
      String stagedMetadataLocation =
          MetadataLocationUtil.metadataLocation(spec.getPropertiesMap());
      TableRequests.Create normalizedReq =
          ensureMetadataLocation(effectiveReq, stagedMetadataLocation);
      StagedTableEntry entry =
          new StagedTableEntry(
              new StagedTableKey(
                  accountId,
                  namespaceContext.catalogName(),
                  namespaceContext.namespacePath(),
                  tableName,
                  stageId),
              namespaceContext.catalogId(),
              namespaceContext.namespaceId(),
              normalizedReq,
              spec,
              STAGE_CREATE_REQUIREMENTS,
              StageState.STAGED,
              null,
              null,
              idempotencyKey);
      StagedTableEntry stored = stagedTableService.saveStage(entry);
      LOG.infof(
          "Stored stage-create payload account=%s catalog=%s namespace=%s table=%s stageId=%s"
              + " txnHeader=%s",
          accountContext.getAccountId(),
          namespaceContext.catalogName(),
          namespaceContext.namespacePath(),
          tableName,
          stored.key().stageId(),
          transactionId);
      Table stubTable =
          Table.newBuilder()
              .setCatalogId(namespaceContext.catalogId())
              .setNamespaceId(namespaceContext.namespaceId())
              .setDisplayName(tableName)
              .build();
      LoadTableResultDto loadResult =
          TableResponseMapper.toLoadResultFromCreate(
              tableName,
              stubTable,
              normalizedReq,
              tableSupport.defaultTableConfig(),
              tableSupport.defaultCredentials());
      LOG.infof(
          "Stage-create metadata resolved stageId=%s location=%s requestProperty=%s",
          stored.key().stageId(),
          loadResult.metadataLocation(),
          normalizedReq.properties() == null
              ? "<none>"
              : normalizedReq.properties().get("metadata-location"));
      LOG.infof(
          "Stage-create response stageId=%s metadataLocation=%s configKeys=%s",
          stored.key().stageId(), loadResult.metadataLocation(), loadResult.config().keySet());
      return Response.ok(
              new StageCreateResponseDto(
                  loadResult.metadataLocation(),
                  loadResult.metadata(),
                  loadResult.config(),
                  loadResult.storageCredentials(),
                  stored.requirements(),
                  stored.key().stageId()))
          .build();
    } catch (IllegalArgumentException | JsonProcessingException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
  }

  private TableRequests.Create applyDefaultLocationIfMissing(
      String prefix, List<String> namespacePath, String tableName, TableRequests.Create request) {
    if (request == null) {
      return null;
    }
    if (request.location() != null && !request.location().isBlank()) {
      return request;
    }
    String base = config.defaultWarehousePath().orElse(null);
    if (base == null || base.isBlank()) {
      return request;
    }
    String resolvedName = (tableName == null || tableName.isBlank()) ? "table" : tableName.trim();
    StringBuilder builder = new StringBuilder(ensureEndsWithSlash(base.trim()));
    if (prefix != null && !prefix.isBlank()) {
      builder.append(trimSlashes(prefix)).append('/');
    }
    if (namespacePath != null && !namespacePath.isEmpty()) {
      builder.append(String.join("/", namespacePath)).append('/');
    }
    builder.append(resolvedName);
    String computedLocation = builder.toString();
    return new TableRequests.Create(
        request.name(),
        request.schemaJson(),
        request.schema(),
        computedLocation,
        request.properties(),
        request.partitionSpec(),
        request.writeOrder(),
        request.stageCreate());
  }

  private static String ensureEndsWithSlash(String base) {
    if (base == null || base.isBlank()) {
      return "";
    }
    return base.endsWith("/") ? base : base + "/";
  }

  private static String trimSlashes(String text) {
    if (text == null) {
      return "";
    }
    String trimmed = text.trim();
    while (trimmed.startsWith("/")) {
      trimmed = trimmed.substring(1);
    }
    while (trimmed.endsWith("/") && !trimmed.isEmpty()) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed;
  }

  private static TableRequests.Create ensureMetadataLocation(
      TableRequests.Create request, String metadataLocation) {
    if (request == null || metadataLocation == null || metadataLocation.isBlank()) {
      return request;
    }
    Map<String, String> props = request.properties();
    String existing = MetadataLocationUtil.metadataLocation(props);
    if (metadataLocation.equals(existing)) {
      return request;
    }
    Map<String, String> updated =
        props == null || props.isEmpty() ? new LinkedHashMap<>() : new LinkedHashMap<>(props);
    MetadataLocationUtil.setMetadataLocation(updated, metadataLocation);
    return new TableRequests.Create(
        request.name(),
        request.schemaJson(),
        request.schema(),
        request.location(),
        Map.copyOf(updated),
        request.partitionSpec(),
        request.writeOrder(),
        request.stageCreate());
  }

  private String safeSerializeCreate(TableRequests.Create request) {
    if (request == null) {
      return "<null>";
    }
    try {
      return mapper.writeValueAsString(request);
    } catch (JsonProcessingException e) {
      return String.valueOf(request);
    }
  }

  private boolean hasSchema(TableRequests.Create request) {
    if (request == null) {
      return false;
    }
    if (request.schemaJson() != null && !request.schemaJson().isBlank()) {
      return true;
    }
    return request.schema() != null && !request.schema().isNull();
  }

  private String metadataLocation(Table table, IcebergMetadata metadata) {
    Map<String, String> props =
        table == null || table.getPropertiesMap() == null ? Map.of() : table.getPropertiesMap();
    String propertyLocation = MetadataLocationUtil.metadataLocation(props);
    if (propertyLocation != null
        && !propertyLocation.isBlank()
        && !MetadataLocationUtil.isPointer(propertyLocation)) {
      return propertyLocation;
    }
    if (metadata != null
        && metadata.getMetadataLocation() != null
        && !metadata.getMetadataLocation().isBlank()) {
      return metadata.getMetadataLocation();
    }
    if (propertyLocation != null && !propertyLocation.isBlank()) {
      return propertyLocation;
    }
    return table != null && table.hasResourceId() ? table.getResourceId().getId() : null;
  }
}
