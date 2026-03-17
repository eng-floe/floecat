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

package ai.floedb.floecat.gateway.iceberg.minimal.services.planning;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.Operator;
import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.execution.rpc.ScanBundle;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.ContentFileDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.FileScanTaskDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.TablePlanResponseDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.PlanRequests;
import ai.floedb.floecat.gateway.iceberg.minimal.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.DescribeInputsRequest;
import ai.floedb.floecat.query.rpc.EndQueryRequest;
import ai.floedb.floecat.query.rpc.FetchScanBundleRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

@ApplicationScoped
public class TablePlanningService {
  private final GrpcWithHeaders grpc;
  private final ObjectMapper mapper;
  private final PlanTaskManager planTaskManager;

  @Inject
  public TablePlanningService(
      GrpcWithHeaders grpc, ObjectMapper mapper, PlanTaskManager planTaskManager) {
    this.grpc = grpc;
    this.mapper = mapper;
    this.planTaskManager = planTaskManager;
  }

  public Response plan(
      Table tableRecord,
      String namespace,
      String tableName,
      PlanRequests.Plan rawRequest,
      List<StorageCredentialDto> credentials) {
    PlanRequests.Plan request = rawRequest == null ? PlanRequests.Plan.empty() : rawRequest;
    Response validation = validateRequest(request);
    if (validation != null) {
      return validation;
    }
    if (tableRecord == null || !tableRecord.hasResourceId() || !tableRecord.hasCatalogId()) {
      return failedPlanning("Table metadata is incomplete");
    }

    String queryId = null;
    try {
      var begin =
          grpc.withHeaders(grpc.raw().query())
              .beginQuery(
                  BeginQueryRequest.newBuilder()
                      .setDefaultCatalogId(tableRecord.getCatalogId())
                      .build());
      queryId = begin.getQuery().getQueryId();
      registerPlanInput(queryId, tableRecord.getResourceId(), request);
      ScanBundle bundle = fetchScanBundle(queryId, tableRecord.getResourceId(), request);
      String planId = UUID.randomUUID().toString();
      planTaskManager.registerSubmittedPlan(planId, namespace, tableName);
      PlanTaskManager.PlanDescriptor descriptor =
          planTaskManager.registerCompletedPlan(
              planId,
              namespace,
              tableName,
              toFileScanTasks(bundle),
              toDeleteFiles(bundle),
              credentials == null ? List.of() : List.copyOf(credentials));
      return Response.ok(toPlanResponse(descriptor, true)).build();
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    } catch (StatusRuntimeException e) {
      return IcebergErrorResponses.grpc(e);
    } catch (RuntimeException e) {
      return failedPlanning(e.getMessage() == null ? "Scan planning failed" : e.getMessage());
    } finally {
      if (queryId != null && !queryId.isBlank()) {
        try {
          grpc.withHeaders(grpc.raw().query())
              .endQuery(EndQueryRequest.newBuilder().setQueryId(queryId).setCommit(false).build());
        } catch (RuntimeException ignored) {
        }
      }
    }
  }

  public Response fetchPlan(String planId) {
    return planTaskManager
        .findPlan(planId)
        .map(descriptor -> Response.ok(toPlanResponse(descriptor, false)).build())
        .orElseGet(() -> IcebergErrorResponses.noSuchPlanId("plan " + planId + " not found"));
  }

  public Response cancelPlan(String planId) {
    if (planTaskManager.findPlan(planId).isEmpty()) {
      return IcebergErrorResponses.noSuchPlanId("plan " + planId + " not found");
    }
    planTaskManager.cancelPlan(planId);
    return Response.noContent().build();
  }

  public Response consumeTask(String namespace, String tableName, String planTaskId) {
    return planTaskManager
        .consumeTask(namespace, tableName, planTaskId)
        .map(response -> Response.ok(response).build())
        .orElseGet(() -> IcebergErrorResponses.noSuchPlanTask("plan-task not found"));
  }

  private Response validateRequest(PlanRequests.Plan request) {
    Long startSnapshotId = request.startSnapshotId();
    Long endSnapshotId = request.endSnapshotId();
    Long snapshotId = request.snapshotId();
    if (snapshotId != null && (startSnapshotId != null || endSnapshotId != null)) {
      return IcebergErrorResponses.validation(
          "snapshot-id cannot be combined with start-snapshot-id or end-snapshot-id");
    }
    if (endSnapshotId != null && startSnapshotId == null) {
      return IcebergErrorResponses.validation("end-snapshot-id requires start-snapshot-id");
    }
    if (startSnapshotId != null && endSnapshotId == null) {
      return IcebergErrorResponses.validation("start-snapshot-id requires end-snapshot-id");
    }
    if (startSnapshotId != null && endSnapshotId != null && startSnapshotId >= endSnapshotId) {
      return IcebergErrorResponses.validation(
          "start-snapshot-id must be less than end-snapshot-id");
    }
    return null;
  }

  private void registerPlanInput(String queryId, ResourceId tableId, PlanRequests.Plan request) {
    QueryInput.Builder input = QueryInput.newBuilder().setTableId(tableId);
    Long snapshotId =
        request.endSnapshotId() != null ? request.endSnapshotId() : request.snapshotId();
    if (snapshotId != null) {
      input.setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId));
    } else {
      input.setSnapshot(SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT));
    }
    grpc.withHeaders(grpc.raw().querySchema())
        .describeInputs(
            DescribeInputsRequest.newBuilder().setQueryId(queryId).addInputs(input).build());
  }

  private ScanBundle fetchScanBundle(
      String queryId, ResourceId tableId, PlanRequests.Plan request) {
    FetchScanBundleRequest.Builder builder =
        FetchScanBundleRequest.newBuilder().setQueryId(queryId).setTableId(tableId);
    if (request.select() != null && !request.select().isEmpty()) {
      builder.addAllRequiredColumns(request.select());
    }
    List<Predicate> predicates = buildPredicates(request.filter(), request.caseSensitive());
    if (!predicates.isEmpty()) {
      builder.addAllPredicates(predicates);
    }
    return grpc.withHeaders(grpc.raw().queryScan()).fetchScanBundle(builder.build()).getBundle();
  }

  private List<FileScanTaskDto> toFileScanTasks(ScanBundle bundle) {
    if (bundle == null || bundle.getDataFilesCount() == 0) {
      return List.of();
    }
    List<FileScanTaskDto> tasks = new ArrayList<>();
    for (ScanFile file : bundle.getDataFilesList()) {
      tasks.add(
          new FileScanTaskDto(
              toContentFile(file),
              file.getDeleteFileIndicesCount() > 0 ? file.getDeleteFileIndicesList() : List.of(),
              null));
    }
    return List.copyOf(tasks);
  }

  private List<ContentFileDto> toDeleteFiles(ScanBundle bundle) {
    if (bundle == null || bundle.getDeleteFilesCount() == 0) {
      return List.of();
    }
    List<ContentFileDto> files = new ArrayList<>();
    for (ScanFile file : bundle.getDeleteFilesList()) {
      files.add(toContentFile(file));
    }
    return List.copyOf(files);
  }

  private ContentFileDto toContentFile(ScanFile file) {
    String content =
        switch (file.getFileContent()) {
          case SCAN_FILE_CONTENT_EQUALITY_DELETES -> "equality-deletes";
          case SCAN_FILE_CONTENT_POSITION_DELETES -> "position-deletes";
          default -> "data";
        };
    return new ContentFileDto(
        content,
        file.getFilePath(),
        file.getFileFormat(),
        file.getPartitionSpecId(),
        parsePartition(file.getPartitionDataJson()),
        file.getFileSizeInBytes(),
        file.getRecordCount(),
        null,
        List.of(),
        null,
        file.getEqualityFieldIdsCount() == 0 ? null : file.getEqualityFieldIdsList());
  }

  private List<Object> parsePartition(String json) {
    if (json == null || json.isBlank()) {
      return List.of();
    }
    try {
      JsonNode node = mapper.readTree(json);
      JsonNode values = node.get("partitionValues");
      if (values == null || !values.isArray()) {
        return List.of();
      }
      List<Object> out = new ArrayList<>();
      values.forEach(value -> out.add(value.isNumber() ? value.numberValue() : value.asText()));
      return List.copyOf(out);
    } catch (Exception ignored) {
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
      case "true", "always-true", "false", "always-false", "or", "not" -> {}
      default -> {
        Predicate predicate = buildLeafPredicate(type, expr, caseSensitive);
        if (predicate != null) {
          out.add(predicate);
        }
      }
    }
  }

  private TablePlanResponseDto toPlanResponse(
      PlanTaskManager.PlanDescriptor descriptor, boolean includePlanId) {
    String status = descriptor.status().value();
    if ("cancelled".equals(status)) {
      return new TablePlanResponseDto(status, null, null, null, null, null);
    }
    if (!"completed".equals(status)) {
      return new TablePlanResponseDto(
          status, includePlanId ? descriptor.planId() : null, null, null, null, null);
    }
    List<FileScanTaskDto> fileScanTasks = copyOfOrEmpty(descriptor.fileScanTasks());
    List<ContentFileDto> deleteFiles = copyOfOrEmpty(descriptor.deleteFiles());
    return new TablePlanResponseDto(
        descriptor.status().value(),
        includePlanId ? descriptor.planId() : null,
        descriptor.planTasks(),
        fileScanTasks,
        deleteFiles,
        descriptor.credentials());
  }

  private static <T> List<T> copyOfOrEmpty(List<T> values) {
    if (values == null || values.isEmpty()) {
      return List.of();
    }
    return List.copyOf(values);
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
      case "is_null", "is-null" -> op = Operator.OP_IS_NULL;
      case "not-null", "not_null", "is-not-null", "is_not_null" -> op = Operator.OP_IS_NOT_NULL;
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
    String single = literalValue(node);
    return single == null ? List.of() : List.of(single);
  }

  private String literalValue(Object node) {
    if (node == null) {
      return null;
    }
    if (node instanceof Map<?, ?> map) {
      if (map.containsKey("literal")) {
        return literalValue(map.get("literal"));
      }
      if (map.containsKey("value")) {
        return literalValue(map.get("value"));
      }
      if (map.containsKey("term")) {
        return literalValue(map.get("term"));
      }
      return null;
    }
    return String.valueOf(node);
  }

  private Map<String, Object> asObjectMap(Object value) {
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

  private String asString(Object value) {
    return value == null ? null : String.valueOf(value);
  }

  private Response failedPlanning(String message) {
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(
            Map.of(
                "status",
                "failed",
                "error",
                Map.of(
                    "message",
                    message,
                    "type",
                    "InternalServerError",
                    "code",
                    Response.Status.INTERNAL_SERVER_ERROR.getStatusCode())))
        .build();
  }
}
