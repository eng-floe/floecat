package ai.floedb.metacat.service.query.impl;

import ai.floedb.metacat.catalog.rpc.GetSchemaRequest;
import ai.floedb.metacat.catalog.rpc.SchemaServiceGrpc;
import ai.floedb.metacat.execution.rpc.ScanFile;
import ai.floedb.metacat.query.rpc.BeginPlanExRequest;
import ai.floedb.metacat.query.rpc.BeginPlanExResponse;
import ai.floedb.metacat.query.rpc.BeginQueryRequest;
import ai.floedb.metacat.query.rpc.PlanningEx;
import ai.floedb.metacat.query.rpc.Predicate;
import ai.floedb.metacat.query.rpc.QueryDescriptor;
import ai.floedb.metacat.query.rpc.QueryServiceGrpc;
import ai.floedb.metacat.query.rpc.SchemaDescriptor;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.common.LogHelper;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.jboss.logging.Logger;

/**
 * Transitional PlanningEx implementation: delegates to the existing Planning service and
 * (optionally) returns an empty SchemaDescriptor. Projection/predicate pushdown is not yet
 * enforced; this preserves compatibility while the richer planner is built.
 */
@GrpcService
public class PlanningExImpl extends BaseServiceImpl implements PlanningEx {

  @GrpcClient("metacat")
  QueryServiceGrpc.QueryServiceBlockingStub planning;

  @GrpcClient("metacat")
  SchemaServiceGrpc.SchemaServiceBlockingStub schemas;

  private static final Logger LOG = Logger.getLogger(PlanningEx.class);

  @Override
  public Uni<BeginPlanExResponse> beginPlanEx(BeginPlanExRequest request) {
    var L = LogHelper.start(LOG, "BeginPlanEx");

    return mapFailures(
            run(
                () -> {
                  // Build a legacy BeginPlan request from the shared fields.
                  BeginQueryRequest.Builder legacy =
                      BeginQueryRequest.newBuilder()
                          .addAllInputs(request.getInputsList())
                          .setTtlSeconds(request.getTtlSeconds());
                  if (request.hasAsOfDefault()) {
                    legacy.setAsOfDefault(request.getAsOfDefault());
                  }

                  var planResp = planning.beginQuery(legacy.build());

                  SchemaDescriptor schema = SchemaDescriptor.getDefaultInstance();
                  if (request.getIncludeSchema()
                      && planResp.getQuery().getSnapshots().getPinsCount() > 0) {
                    // Use the first pinned table as the schema target (single-table plans for now)
                    var pin = planResp.getQuery().getSnapshots().getPins(0);
                    schema =
                        schemas
                            .getSchema(
                                GetSchemaRequest.newBuilder().setTableId(pin.getTableId()).build())
                            .getSchema();
                  }

                  QueryDescriptor pruned =
                      prune(
                          planResp.getQuery(),
                          request.getRequiredColumnsList(),
                          request.getPredicatesList());

                  return BeginPlanExResponse.newBuilder().setPlan(pruned).setSchema(schema).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private QueryDescriptor prune(
      QueryDescriptor plan, List<String> requiredColumns, List<Predicate> predicates) {
    if (requiredColumns == null || requiredColumns.isEmpty()) {
      return filterByPredicates(plan, predicates);
    }
    Set<String> cols = new HashSet<>(requiredColumns);
    QueryDescriptor.Builder pb = plan.toBuilder().clearDataFiles().clearDeleteFiles();
    for (ScanFile pf : plan.getDataFilesList()) {
      pb.addDataFiles(filterColumns(pf, cols));
    }
    for (ScanFile pf : plan.getDeleteFilesList()) {
      pb.addDeleteFiles(filterColumns(pf, cols));
    }
    return filterByPredicates(pb.build(), predicates);
  }

  private ScanFile filterColumns(ScanFile pf, Set<String> cols) {
    ScanFile.Builder b = pf.toBuilder().clearColumns();
    for (var c : pf.getColumnsList()) {
      if (cols.contains(c.getColumnName())) {
        b.addColumns(c);
      }
    }
    return b.build();
  }

  private QueryDescriptor filterByPredicates(QueryDescriptor plan, List<Predicate> predicates) {
    if (predicates == null || predicates.isEmpty()) {
      return plan;
    }
    List<Predicate> equalsPreds =
        predicates.stream().filter(p -> !p.getExpression().isBlank()).collect(Collectors.toList());
    if (equalsPreds.isEmpty()) {
      return plan;
    }
    QueryDescriptor.Builder pb = plan.toBuilder().clearDataFiles().clearDeleteFiles();
    for (ScanFile pf : plan.getDataFilesList()) {
      if (matches(pf, equalsPreds)) {
        pb.addDataFiles(pf);
      }
    }
    for (ScanFile pf : plan.getDeleteFilesList()) {
      if (matches(pf, equalsPreds)) {
        pb.addDeleteFiles(pf);
      }
    }
    return pb.build();
  }

  private boolean matches(ScanFile pf, List<Predicate> preds) {
    for (var p : preds) {
      String col = p.getColumn();
      String expr = p.getExpression();
      if (col == null || col.isBlank() || expr == null || expr.isBlank()) {
        continue;
      }
      String value = expr.startsWith("=") ? expr.substring(1) : expr;
      boolean colSeen = false;
      for (var c : pf.getColumnsList()) {
        if (col.equalsIgnoreCase(c.getColumnName())) {
          colSeen = true;
          String min = c.getMin();
          String max = c.getMax();
          // Only prune if stats say the file cannot match.
          if (min != null && max != null && !min.isBlank() && !max.isBlank()) {
            if (!value.equals(min) || !value.equals(max)) {
              return false;
            }
          }
        }
      }
      if (!colSeen) {
        // No stats for this column; keep the file.
        continue;
      }
    }
    return true;
  }
}
