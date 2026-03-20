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

package ai.floedb.floecat.gateway.iceberg.rest.table;

import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.asString;

import ai.floedb.floecat.common.rpc.Operator;
import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.execution.rpc.ScanBundle;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.execution.rpc.ScanFileContent;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.ContentFileDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.FileScanTaskDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TablePlanResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TablePlanTasksResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.support.GrpcServiceFacade;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.DescribeInputsRequest;
import ai.floedb.floecat.query.rpc.EndQueryRequest;
import ai.floedb.floecat.query.rpc.FetchScanBundleRequest;
import ai.floedb.floecat.query.rpc.GetQueryRequest;
import ai.floedb.floecat.query.rpc.QueryDescriptor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

@ApplicationScoped
public class TablePlanService {
  private static final long MIN_TTL_SECONDS = 60L;
  private static final int DEFAULT_FILES_PER_TASK = 128;
  private final ConcurrentMap<String, PlanContext> planContexts = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, PlanEntry> plans = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, PlanTaskPayload> planTasks = new ConcurrentHashMap<>();

  @Inject IcebergGatewayConfig config;
  @Inject GrpcServiceFacade grpcClient;
  @Inject ObjectMapper mapper;

  public PlanHandle startPlan(
      ResourceId catalogId,
      ResourceId tableId,
      List<String> selectColumns,
      Long startSnapshotId,
      Long endSnapshotId,
      Long snapshotId,
      List<String> statsFields,
      Map<String, Object> filter,
      boolean caseSensitive,
      boolean useSnapshotSchema,
      Long minRows) {
    Long resolvedSnapshot = endSnapshotId != null ? endSnapshotId : snapshotId;
    List<Predicate> predicates = buildPredicates(filter, caseSensitive);

    var begin =
        grpcClient.beginQuery(
            BeginQueryRequest.newBuilder().setDefaultCatalogId(catalogId).build());
    String queryId = begin.getQuery().getQueryId();

    planContexts.put(
        queryId,
        new PlanContext(
            tableId,
            selectColumns == null ? null : List.copyOf(selectColumns),
            startSnapshotId,
            resolvedSnapshot,
            predicates,
            statsFields == null ? null : List.copyOf(statsFields),
            useSnapshotSchema,
            caseSensitive,
            minRows));
    registerPlanInput(queryId, tableId, resolvedSnapshot);
    return new PlanHandle(queryId, resolvedSnapshot, startSnapshotId);
  }

  public TablePlanResponseDto fetchPlan(String planId, List<StorageCredentialDto> credentials) {
    PlanContext ctx = planContexts.remove(planId);
    if (ctx == null) {
      throw new IllegalArgumentException("unknown plan id " + planId);
    }
    var resp = grpcClient.getQuery(GetQueryRequest.newBuilder().setQueryId(planId).build());
    QueryDescriptor query = resp.getQuery();

    ScanBundle bundle = fetchScanBundle(ctx, query.getQueryId());
    TablePlanTasksResponseDto scanTasks = toScanTasksDto(bundle);
    String resolvedPlanId =
        (query.getQueryId() == null || query.getQueryId().isBlank()) ? planId : query.getQueryId();
    List<String> planTasks =
        (scanTasks.fileScanTasks() == null || scanTasks.fileScanTasks().isEmpty())
            ? List.of()
            : List.of(resolvedPlanId);
    return new TablePlanResponseDto(
        "completed",
        resolvedPlanId,
        planTasks,
        scanTasks.fileScanTasks(),
        scanTasks.deleteFiles(),
        credentials);
  }

  public void cancelPlan(String planId) {
    grpcClient.endQuery(EndQueryRequest.newBuilder().setQueryId(planId).setCommit(false).build());
    planContexts.remove(planId);
    expire();
    PlanEntry entry = plans.get(planId);
    if (entry == null) {
      return;
    }
    for (String taskId : entry.planTaskIds()) {
      planTasks.remove(taskId);
    }
    entry.clearTasks();
    entry.markCancelled();
  }

  public PlanDescriptor registerCompletedPlan(
      String planId,
      String namespace,
      String table,
      List<FileScanTaskDto> fileScanTasks,
      List<ContentFileDto> deleteFiles,
      List<StorageCredentialDto> credentials) {
    expire();
    Objects.requireNonNull(planId, "planId is required");
    List<FileScanTaskDto> files =
        fileScanTasks == null
            ? List.of()
            : Collections.unmodifiableList(new ArrayList<>(fileScanTasks));
    List<ContentFileDto> deletes =
        deleteFiles == null
            ? List.of()
            : Collections.unmodifiableList(new ArrayList<>(deleteFiles));
    PlanEntry entry =
        new PlanEntry(
            planId,
            namespace,
            table,
            credentials == null ? List.of() : List.copyOf(credentials),
            files,
            deletes,
            PlanStatus.COMPLETED);
    if (!files.isEmpty()) {
      int chunkSize = Math.max(1, filesPerTask());
      for (int offset = 0; offset < files.size(); offset += chunkSize) {
        int end = Math.min(files.size(), offset + chunkSize);
        List<FileScanTaskDto> chunk = files.subList(offset, end);
        TablePlanTasksResponseDto payload =
            new TablePlanTasksResponseDto(null, List.copyOf(chunk), deletes);
        String taskId = planId + "-task-" + offset / chunkSize;
        entry.addTask(taskId);
        planTasks.put(taskId, new PlanTaskPayload(taskId, planId, namespace, table, payload));
      }
    }
    plans.put(planId, entry);
    return entry.toDescriptor();
  }

  public PlanDescriptor registerSubmittedPlan(String planId, String namespace, String table) {
    expire();
    Objects.requireNonNull(planId, "planId is required");
    PlanEntry entry =
        new PlanEntry(
            planId, namespace, table, List.of(), List.of(), List.of(), PlanStatus.SUBMITTED);
    plans.put(planId, entry);
    return entry.toDescriptor();
  }

  public Optional<PlanDescriptor> findPlan(String planId) {
    expire();
    PlanEntry entry = plans.get(planId);
    return entry == null ? Optional.empty() : Optional.of(entry.toDescriptor());
  }

  public Optional<TablePlanTasksResponseDto> consumeTask(
      String namespace, String table, String planTaskId) {
    expire();
    PlanTaskPayload payload = planTasks.remove(planTaskId);
    if (payload == null) {
      return Optional.empty();
    }
    if (!Objects.equals(namespace, payload.namespace())
        || !Objects.equals(table, payload.table())) {
      planTasks.put(payload.planTaskId(), payload);
      return Optional.empty();
    }
    PlanEntry entry = plans.get(payload.planId());
    if (entry != null) {
      entry.removeTask(planTaskId);
      if (entry.planTaskIds().isEmpty()) {
        plans.remove(entry.planId(), entry);
      }
    }
    return Optional.of(payload.payload());
  }

  private ScanBundle fetchScanBundle(PlanContext ctx, String queryId) {
    FetchScanBundleRequest.Builder builder =
        FetchScanBundleRequest.newBuilder().setQueryId(queryId).setTableId(ctx.tableId());
    if (ctx.requiredColumns() != null && !ctx.requiredColumns().isEmpty()) {
      builder.addAllRequiredColumns(ctx.requiredColumns());
    }
    if (ctx.predicates() != null && !ctx.predicates().isEmpty()) {
      builder.addAllPredicates(ctx.predicates());
    }
    return grpcClient.fetchScanBundle(builder.build()).getBundle();
  }

  private TablePlanTasksResponseDto toScanTasksDto(ScanBundle bundle) {
    List<ContentFileDto> deleteFiles = new ArrayList<>();
    List<FileScanTaskDto> tasks = new ArrayList<>();
    if (bundle != null) {
      for (ScanFile delete : bundle.getDeleteFilesList()) {
        deleteFiles.add(toContentFile(delete));
      }
      for (ScanFile file : bundle.getDataFilesList()) {
        List<Integer> deleteRefs =
            file.getDeleteFileIndicesCount() > 0 ? file.getDeleteFileIndicesList() : List.of();
        tasks.add(new FileScanTaskDto(toContentFile(file), deleteRefs, null));
      }
    }
    return new TablePlanTasksResponseDto(List.of(), tasks, deleteFiles);
  }

  private ContentFileDto toContentFile(ScanFile file) {
    String content;
    if (file.getFileContent() == ScanFileContent.SCAN_FILE_CONTENT_EQUALITY_DELETES) {
      content = "equality-deletes";
    } else if (file.getFileContent() == ScanFileContent.SCAN_FILE_CONTENT_POSITION_DELETES) {
      content = "position-deletes";
    } else {
      content = "data";
    }
    List<Object> partition = parsePartition(file.getPartitionDataJson());
    List<Integer> equality =
        file.getEqualityFieldIdsList().isEmpty() ? null : file.getEqualityFieldIdsList();
    return new ContentFileDto(
        content,
        file.getFilePath(),
        file.getFileFormat(),
        file.getPartitionSpecId(),
        partition,
        file.getFileSizeInBytes(),
        file.getRecordCount(),
        null,
        List.of(),
        null,
        equality);
  }

  private void registerPlanInput(String queryId, ResourceId tableId, Long snapshotId) {
    grpcClient.describeInputs(
        DescribeInputsRequest.newBuilder()
            .setQueryId(queryId)
            .addInputs(buildPlanInput(tableId, snapshotId))
            .build());
  }

  private QueryInput buildPlanInput(ResourceId tableId, Long snapshotId) {
    QueryInput.Builder input = QueryInput.newBuilder().setTableId(tableId);
    if (snapshotId != null) {
      input.setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId));
    } else {
      input.setSnapshot(SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT));
    }
    return input.build();
  }

  private List<Object> parsePartition(String json) {
    if (json == null || json.isBlank()) {
      return List.of();
    }
    try {
      JsonNode node = mapper.readTree(json);
      if (node.has("partitionValues") && node.get("partitionValues").isArray()) {
        List<Object> values = new ArrayList<>();
        node.get("partitionValues")
            .forEach(val -> values.add(val.isNumber() ? val.numberValue() : val.asText()));
        return values;
      }
      return List.of();
    } catch (Exception e) {
      return List.of();
    }
  }

  private List<Predicate> buildPredicates(Map<String, Object> filter, Boolean caseSensitiveFlag) {
    if (filter == null || filter.isEmpty()) {
      return List.of();
    }
    boolean caseSensitive = caseSensitiveFlag == null || caseSensitiveFlag;
    List<Predicate> out = new ArrayList<>();
    parseFilterExpression(filter, caseSensitive, out);
    return List.copyOf(out);
  }

  private void parseFilterExpression(
      Object expression, boolean caseSensitive, List<Predicate> out) {
    Map<String, Object> expr = asObjectMap(expression);
    if (expr == null || expr.isEmpty()) {
      throw new IllegalArgumentException("filter expression must be an object");
    }
    String type = normalizeExpressionType(asString(expr.get("type")));
    if (type == null || type.isBlank()) {
      throw new IllegalArgumentException("filter expression missing type");
    }
    switch (type) {
      case "and" -> {
        List<Map<String, Object>> children = expressionChildren(expr);
        if (children.isEmpty()) {
          throw new IllegalArgumentException("and expression requires child expressions");
        }
        for (Map<String, Object> child : children) {
          parseFilterExpression(child, caseSensitive, out);
        }
      }
      case "true", "always-true" -> {}
      case "false", "always-false", "or", "not" -> {
        // Skip unsupported logical types to keep filtering conservative.
      }
      default -> {
        Predicate predicate = buildLeafPredicate(type, expr, caseSensitive);
        if (predicate != null) {
          out.add(predicate);
        }
      }
    }
  }

  private Predicate buildLeafPredicate(
      String type, Map<String, Object> expr, boolean caseSensitive) {
    String column = resolveColumn(expr.get("term"), caseSensitive);
    if (column == null || column.isBlank()) {
      throw new IllegalArgumentException("filter expression requires a term reference");
    }
    Operator op;
    List<String> values = List.of();
    switch (type) {
      case "eq", "equal", "equals", "==" -> {
        values = literalValues(expr, "literal", "value");
        op = Operator.OP_EQ;
      }
      case "not-eq", "neq", "not-equal", "not_equal", "!=" -> {
        values = literalValues(expr, "literal", "value");
        op = Operator.OP_NEQ;
      }
      case "lt", "less-than", "less_than", "<" -> {
        values = literalValues(expr, "literal", "value");
        op = Operator.OP_LT;
      }
      case "lt-eq", "lte", "less-than-or-equal", "less_than_or_equal", "<=" -> {
        values = literalValues(expr, "literal", "value");
        op = Operator.OP_LTE;
      }
      case "gt", "greater-than", "greater_than", ">" -> {
        values = literalValues(expr, "literal", "value");
        op = Operator.OP_GT;
      }
      case "gt-eq", "gte", "greater-than-or-equal", "greater_than_or_equal", ">=" -> {
        values = literalValues(expr, "literal", "value");
        op = Operator.OP_GTE;
      }
      case "between" -> {
        values = literalValues(expr, "literals", "values", "literal");
        if (values.size() < 2) {
          List<String> bounds = new ArrayList<>();
          String lower = literalValue(expr.get("lower"));
          String upper = literalValue(expr.get("upper"));
          if (lower != null) {
            bounds.add(lower);
          }
          if (upper != null) {
            bounds.add(upper);
          }
          values = bounds;
        }
        if (values.size() < 2) {
          throw new IllegalArgumentException("between expression requires two bounds");
        }
        values = List.of(values.get(0), values.get(1));
        op = Operator.OP_BETWEEN;
      }
      case "is_null", "is-null" -> {
        op = Operator.OP_IS_NULL;
        values = List.of();
      }
      case "not-null", "not_null", "is-not-null", "is_not_null" -> {
        op = Operator.OP_IS_NOT_NULL;
        values = List.of();
      }
      case "in" -> {
        values = literalValues(expr, "literals", "values");
        op = Operator.OP_IN;
      }
      case "not-in", "starts-with", "not-starts-with", "is-nan", "not-nan" -> {
        return null;
      }
      default -> {
        return null;
      }
    }
    if (requiresLiteral(op) && values.isEmpty()) {
      throw new IllegalArgumentException(type + " filter requires a literal value");
    }
    Predicate.Builder builder = Predicate.newBuilder().setColumn(column).setOp(op);
    if (!values.isEmpty()) {
      builder.addAllValues(values);
    }
    return builder.build();
  }

  private boolean requiresLiteral(Operator op) {
    return op == Operator.OP_EQ
        || op == Operator.OP_NEQ
        || op == Operator.OP_LT
        || op == Operator.OP_LTE
        || op == Operator.OP_GT
        || op == Operator.OP_GTE
        || op == Operator.OP_BETWEEN
        || op == Operator.OP_IN;
  }

  private List<Map<String, Object>> expressionChildren(Map<String, Object> expr) {
    List<Map<String, Object>> expressions = asMapList(expr.get("expressions"));
    if (!expressions.isEmpty()) {
      return expressions;
    }
    List<Map<String, Object>> out = new ArrayList<>();
    Map<String, Object> left = asObjectMap(expr.get("left"));
    if (left != null && !left.isEmpty()) {
      out.add(left);
    }
    Map<String, Object> right = asObjectMap(expr.get("right"));
    if (right != null && !right.isEmpty()) {
      out.add(right);
    }
    return out;
  }

  private String normalizeExpressionType(String raw) {
    if (raw == null) {
      return null;
    }
    return raw.trim().toLowerCase(Locale.ROOT).replace('_', '-');
  }

  private String resolveColumn(Object termNode, boolean caseSensitive) {
    String column = null;
    if (termNode instanceof String str) {
      column = str;
    } else {
      Map<String, Object> term = asObjectMap(termNode);
      if (term != null) {
        if (term.containsKey("term")) {
          return resolveColumn(term.get("term"), caseSensitive);
        }
        column = asString(term.get("ref"));
        if (column == null || column.isBlank()) {
          column = asString(term.get("name"));
        }
        if (column == null || column.isBlank()) {
          column = asString(term.get("column"));
        }
      }
    }
    if (column == null || column.isBlank()) {
      return null;
    }
    return caseSensitive ? column : column.toLowerCase(Locale.ROOT);
  }

  private List<String> literalValues(Map<String, Object> expr, String... keys) {
    for (String key : keys) {
      if (expr.containsKey(key)) {
        List<String> values = literalList(expr.get(key));
        if (!values.isEmpty()) {
          return values;
        }
      }
    }
    return List.of();
  }

  private List<String> literalList(Object node) {
    if (node == null) {
      return List.of();
    }
    if (node instanceof List<?> list) {
      List<String> out = new ArrayList<>();
      for (Object item : list) {
        String value = literalValue(item);
        if (value != null) {
          out.add(value);
        }
      }
      return out;
    }
    Map<String, Object> map = asObjectMap(node);
    if (map != null && !map.isEmpty()) {
      if (map.containsKey("values")) {
        return literalList(map.get("values"));
      }
      if (map.containsKey("literals")) {
        return literalList(map.get("literals"));
      }
      if (map.containsKey("literal")) {
        return literalList(map.get("literal"));
      }
      if (map.containsKey("value")) {
        return literalList(map.get("value"));
      }
    }
    String single = literalValue(node);
    return single == null ? List.of() : List.of(single);
  }

  private String literalValue(Object node) {
    if (node == null) {
      return null;
    }
    Map<String, Object> map = asObjectMap(node);
    if (map != null && !map.isEmpty()) {
      Object value =
          firstPresent(
              map.get("value"),
              map.get("literal"),
              map.get("string"),
              map.get("long"),
              map.get("int"),
              map.get("double"),
              map.get("float"),
              map.get("boolean"));
      return value == null ? null : value.toString();
    }
    return node.toString();
  }

  private Object firstPresent(Object... values) {
    for (Object value : values) {
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  private Map<String, Object> asObjectMap(Object value) {
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

  private List<Map<String, Object>> asMapList(Object value) {
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

  private void expire() {
    Instant cutoff = Instant.now().minus(planTtl());
    for (var entry : plans.values()) {
      if (entry.isExpired(cutoff)) {
        cancelPlan(entry.planId());
        plans.remove(entry.planId(), entry);
      }
    }
    for (var task : planTasks.values()) {
      if (task.createdAt().isBefore(cutoff)) {
        planTasks.remove(task.planTaskId(), task);
      }
    }
  }

  private Duration planTtl() {
    Duration configuredTtl = config == null ? null : config.planTaskTtl();
    if (configuredTtl == null || configuredTtl.isNegative() || configuredTtl.isZero()) {
      configuredTtl = Duration.ofSeconds(MIN_TTL_SECONDS);
    }
    if (configuredTtl.getSeconds() < MIN_TTL_SECONDS) {
      configuredTtl = Duration.ofSeconds(MIN_TTL_SECONDS);
    }
    return configuredTtl;
  }

  private int filesPerTask() {
    int configuredChunk = config == null ? 0 : config.planTaskFilesPerTask();
    if (configuredChunk <= 0) {
      configuredChunk = DEFAULT_FILES_PER_TASK;
    }
    return configuredChunk;
  }

  // TableMappingUtil provides asString.

  private record PlanContext(
      ResourceId tableId,
      List<String> requiredColumns,
      Long startSnapshotId,
      Long snapshotId,
      List<Predicate> predicates,
      List<String> statsFields,
      Boolean useSnapshotSchema,
      Boolean caseSensitive,
      Long minRowsRequested) {}

  public record PlanHandle(String queryId, Long snapshotId, Long startSnapshotId) {}

  public enum PlanStatus {
    SUBMITTED("submitted"),
    COMPLETED("completed"),
    FAILED("failed"),
    CANCELLED("cancelled");

    private final String value;

    PlanStatus(String value) {
      this.value = value;
    }

    public String value() {
      return value;
    }
  }

  public record PlanDescriptor(
      String planId,
      String namespace,
      String table,
      PlanStatus status,
      List<String> planTasks,
      List<StorageCredentialDto> credentials,
      List<FileScanTaskDto> fileScanTasks,
      List<ContentFileDto> deleteFiles) {}

  private static final class PlanEntry {
    private final String planId;
    private final String namespace;
    private final String table;
    private final List<StorageCredentialDto> credentials;
    private final List<FileScanTaskDto> fileScanTasks;
    private final List<ContentFileDto> deleteFiles;
    private final CopyOnWriteArrayList<String> taskIds = new CopyOnWriteArrayList<>();
    private volatile PlanStatus status;
    private volatile Instant updatedAt = Instant.now();

    PlanEntry(
        String planId,
        String namespace,
        String table,
        List<StorageCredentialDto> credentials,
        List<FileScanTaskDto> fileScanTasks,
        List<ContentFileDto> deleteFiles,
        PlanStatus status) {
      this.planId = planId;
      this.namespace = namespace;
      this.table = table;
      this.credentials = credentials;
      this.fileScanTasks = fileScanTasks;
      this.deleteFiles = deleteFiles;
      this.status = status;
    }

    String planId() {
      return planId;
    }

    List<String> planTaskIds() {
      return taskIds;
    }

    void addTask(String taskId) {
      taskIds.addIfAbsent(taskId);
      updatedAt = Instant.now();
    }

    void removeTask(String taskId) {
      taskIds.remove(taskId);
      updatedAt = Instant.now();
    }

    void clearTasks() {
      taskIds.clear();
      updatedAt = Instant.now();
    }

    void markCancelled() {
      status = PlanStatus.CANCELLED;
      updatedAt = Instant.now();
    }

    boolean isExpired(Instant cutoff) {
      return updatedAt.isBefore(cutoff);
    }

    PlanDescriptor toDescriptor() {
      return new PlanDescriptor(
          planId,
          namespace,
          table,
          status,
          List.copyOf(taskIds),
          credentials,
          fileScanTasks,
          deleteFiles);
    }
  }

  private record PlanTaskPayload(
      String planTaskId,
      String planId,
      String namespace,
      String table,
      TablePlanTasksResponseDto payload,
      Instant createdAt) {
    private PlanTaskPayload(
        String planTaskId,
        String planId,
        String namespace,
        String table,
        TablePlanTasksResponseDto payload) {
      this(planTaskId, planId, namespace, table, payload, Instant.now());
    }
  }
}
