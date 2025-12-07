package ai.floedb.floecat.gateway.iceberg.rest.services.planning;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.execution.rpc.ScanBundle;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.execution.rpc.ScanFileContent;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.ContentFileDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.FileScanTaskDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.PlanResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.ScanTasksResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.DescribeInputsRequest;
import ai.floedb.floecat.query.rpc.EndQueryRequest;
import ai.floedb.floecat.query.rpc.FetchScanBundleRequest;
import ai.floedb.floecat.query.rpc.GetQueryRequest;
import ai.floedb.floecat.query.rpc.Operator;
import ai.floedb.floecat.query.rpc.Predicate;
import ai.floedb.floecat.query.rpc.QueryDescriptor;
import ai.floedb.floecat.query.rpc.QueryInput;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
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

  @Inject GrpcWithHeaders grpc;
  @Inject ObjectMapper mapper;

  public PlanHandle startPlan(
      String catalogName,
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

    QueryServiceGrpc.QueryServiceBlockingStub queryStub = grpc.withHeaders(grpc.raw().query());
    var begin = queryStub.beginQuery(BeginQueryRequest.newBuilder().build());
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

  public PlanResponseDto fetchPlan(String planId, List<StorageCredentialDto> credentials) {
    PlanContext ctx = planContexts.remove(planId);
    if (ctx == null) {
      throw new IllegalArgumentException("unknown plan id " + planId);
    }
    QueryServiceGrpc.QueryServiceBlockingStub queryStub = grpc.withHeaders(grpc.raw().query());
    var resp = queryStub.getQuery(GetQueryRequest.newBuilder().setQueryId(planId).build());
    QueryDescriptor query = resp.getQuery();

    QueryScanServiceGrpc.QueryScanServiceBlockingStub scanStub =
        grpc.withHeaders(grpc.raw().queryScan());
    ScanBundle bundle = fetchScanBundle(scanStub, ctx, query.getQueryId());
    ScanTasksResponseDto scanTasks = toScanTasksDto(bundle);
    String resolvedPlanId =
        (query.getQueryId() == null || query.getQueryId().isBlank()) ? planId : query.getQueryId();
    List<String> planTasks =
        (scanTasks.fileScanTasks() == null || scanTasks.fileScanTasks().isEmpty())
            ? List.of()
            : List.of(resolvedPlanId);
    return new PlanResponseDto(
        "completed",
        resolvedPlanId,
        planTasks,
        scanTasks.fileScanTasks(),
        scanTasks.deleteFiles(),
        credentials);
  }

  public void cancelPlan(String planId) {
    QueryServiceGrpc.QueryServiceBlockingStub queryStub = grpc.withHeaders(grpc.raw().query());
    queryStub.endQuery(EndQueryRequest.newBuilder().setQueryId(planId).setCommit(false).build());
    planContexts.remove(planId);
  }

  public ScanTasksResponseDto fetchTasks(String planId) {
    PlanContext ctx = planContexts.get(planId);
    if (ctx == null) {
      throw new IllegalArgumentException("unknown plan id " + planId);
    }
    QueryScanServiceGrpc.QueryScanServiceBlockingStub scanStub =
        grpc.withHeaders(grpc.raw().queryScan());
    ScanBundle bundle = fetchScanBundle(scanStub, ctx, planId);
    planContexts.remove(planId);
    return toScanTasksDto(bundle);
  }

  private ScanBundle fetchScanBundle(
      QueryScanServiceGrpc.QueryScanServiceBlockingStub stub, PlanContext ctx, String queryId) {
    FetchScanBundleRequest.Builder builder =
        FetchScanBundleRequest.newBuilder().setQueryId(queryId).setTableId(ctx.tableId());
    if (ctx.requiredColumns() != null && !ctx.requiredColumns().isEmpty()) {
      builder.addAllRequiredColumns(ctx.requiredColumns());
    }
    if (ctx.predicates() != null && !ctx.predicates().isEmpty()) {
      builder.addAllPredicates(ctx.predicates());
    }
    return stub.fetchScanBundle(builder.build()).getBundle();
  }

  private ScanTasksResponseDto toScanTasksDto(ScanBundle bundle) {
    List<ContentFileDto> deleteFiles = new ArrayList<>();
    List<FileScanTaskDto> tasks = new ArrayList<>();
    if (bundle != null) {
      for (ScanFile delete : bundle.getDeleteFilesList()) {
        deleteFiles.add(toContentFile(delete));
      }
      for (ScanFile file : bundle.getDataFilesList()) {
        tasks.add(new FileScanTaskDto(toContentFile(file), List.of(), null));
      }
    }
    return new ScanTasksResponseDto(List.of(), tasks, deleteFiles);
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
    QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub schemaStub =
        grpc.withHeaders(grpc.raw().querySchema());
    schemaStub.describeInputs(
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
      case "always_true" -> {}
      case "always_false", "or", "not" -> {
        throw new IllegalArgumentException("filter type " + type + " is not supported");
      }
      default -> out.add(buildLeafPredicate(type, expr, caseSensitive));
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
      case "neq", "not_equal", "!=" -> {
        values = literalValues(expr, "literal", "value");
        op = Operator.OP_NEQ;
      }
      case "lt", "less_than", "<" -> {
        values = literalValues(expr, "literal", "value");
        op = Operator.OP_LT;
      }
      case "lte", "less_than_or_equal", "<=" -> {
        values = literalValues(expr, "literal", "value");
        op = Operator.OP_LTE;
      }
      case "gt", "greater_than", ">" -> {
        values = literalValues(expr, "literal", "value");
        op = Operator.OP_GT;
      }
      case "gte", "greater_than_or_equal", ">=" -> {
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
      case "is_not_null", "not_null", "not-null" -> {
        op = Operator.OP_IS_NOT_NULL;
        values = List.of();
      }
      case "in" -> {
        values = literalValues(expr, "literals", "values");
        op = Operator.OP_IN;
      }
      default -> throw new IllegalArgumentException("filter type " + type + " is not supported");
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
    return raw.trim().toLowerCase(Locale.ROOT).replace('-', '_');
  }

  private String resolveColumn(Object termNode, boolean caseSensitive) {
    String column = null;
    if (termNode instanceof String str) {
      column = str;
    } else {
      Map<String, Object> term = asObjectMap(termNode);
      if (term != null) {
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

  private String asString(Object value) {
    return value == null ? null : value.toString();
  }

  private record PlanContext(
      ResourceId tableId,
      List<String> requiredColumns,
      Long startSnapshotId,
      Long snapshotId,
      List<Predicate> predicates,
      List<String> statsFields,
      Boolean useSnapshotSchema,
      Boolean caseSensitive,
      Long minRowsRequested) {
    PlanContext withSnapshot(Long snapshot) {
      return new PlanContext(
          tableId,
          requiredColumns,
          startSnapshotId,
          snapshot,
          predicates,
          statsFields,
          useSnapshotSchema,
          caseSensitive,
          minRowsRequested);
    }

    PlanContext withTableId(ResourceId id) {
      return new PlanContext(
          id,
          requiredColumns,
          startSnapshotId,
          snapshotId,
          predicates,
          statsFields,
          useSnapshotSchema,
          caseSensitive,
          minRowsRequested);
    }
  }

  public record PlanHandle(String queryId, Long snapshotId, Long startSnapshotId) {}
}
