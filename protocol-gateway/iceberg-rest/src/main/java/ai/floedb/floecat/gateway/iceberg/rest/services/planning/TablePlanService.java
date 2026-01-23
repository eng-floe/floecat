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

package ai.floedb.floecat.gateway.iceberg.rest.services.planning;

import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asString;

import ai.floedb.floecat.common.rpc.Operator;
import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.execution.rpc.ScanBundle;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.execution.rpc.ScanFileContent;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.ContentFileDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.FileScanTaskDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TablePlanResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TablePlanTasksResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.QueryClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.QuerySchemaClient;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@ApplicationScoped
public class TablePlanService {
  private final ConcurrentMap<String, PlanContext> planContexts = new ConcurrentHashMap<>();

  @Inject QueryClient queryClient;
  @Inject QuerySchemaClient querySchemaClient;
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
        queryClient.beginQuery(
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
    var resp = queryClient.getQuery(GetQueryRequest.newBuilder().setQueryId(planId).build());
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
    queryClient.endQuery(EndQueryRequest.newBuilder().setQueryId(planId).setCommit(false).build());
    planContexts.remove(planId);
  }

  public TablePlanTasksResponseDto fetchTasks(String planId) {
    PlanContext ctx = planContexts.get(planId);
    if (ctx == null) {
      throw new IllegalArgumentException("unknown plan id " + planId);
    }
    ScanBundle bundle = fetchScanBundle(ctx, planId);
    planContexts.remove(planId);
    return toScanTasksDto(bundle);
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
    return queryClient.fetchScanBundle(builder.build()).getBundle();
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
    querySchemaClient.describeInputs(
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
}
