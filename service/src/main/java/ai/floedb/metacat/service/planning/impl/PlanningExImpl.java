package ai.floedb.metacat.service.planning.impl;

import ai.floedb.metacat.catalog.rpc.GetSchemaRequest;
import ai.floedb.metacat.catalog.rpc.SchemaServiceGrpc;
import ai.floedb.metacat.planning.rpc.BeginPlanExRequest;
import ai.floedb.metacat.planning.rpc.BeginPlanExResponse;
import ai.floedb.metacat.planning.rpc.BeginPlanRequest;
import ai.floedb.metacat.planning.rpc.PlanningEx;
import ai.floedb.metacat.planning.rpc.SchemaDescriptor;
import ai.floedb.metacat.planning.rpc.PlanDescriptor;
import ai.floedb.metacat.planning.rpc.PlanFile;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.common.LogHelper;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.jboss.logging.Logger;

/**
 * Transitional PlanningEx implementation: delegates to the existing Planning service and (optionally)
 * returns an empty SchemaDescriptor. Projection/predicate pushdown is not yet enforced; this
 * preserves compatibility while the richer planner is built.
 */
@GrpcService
public class PlanningExImpl extends BaseServiceImpl implements PlanningEx {

  @GrpcClient("metacat")
  ai.floedb.metacat.planning.rpc.PlanningGrpc.PlanningBlockingStub planning;

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
                  BeginPlanRequest.Builder legacy =
                      BeginPlanRequest.newBuilder()
                          .addAllInputs(request.getInputsList())
                          .setTtlSeconds(request.getTtlSeconds());
                  if (request.hasAsOfDefault()) {
                    legacy.setAsOfDefault(request.getAsOfDefault());
                  }

                  var planResp = planning.beginPlan(legacy.build());

                  SchemaDescriptor schema = SchemaDescriptor.getDefaultInstance();
                  if (request.getIncludeSchema() && planResp.getPlan().getSnapshots().getPinsCount() > 0) {
                    // Use the first pinned table as the schema target (single-table plans for now)
                    var pin = planResp.getPlan().getSnapshots().getPins(0);
                    schema =
                        schemas.getSchema(
                            GetSchemaRequest.newBuilder().setTableId(pin.getTableId()).build())
                            .getSchema();
                  }

                  PlanDescriptor pruned =
                      prune(planResp.getPlan(), request.getRequiredColumnsList(), request.getPredicatesList());

                  return BeginPlanExResponse.newBuilder()
                      .setPlan(pruned)
                      .setSchema(schema)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private PlanDescriptor prune(
      PlanDescriptor plan, List<String> requiredColumns, List<ai.floedb.metacat.planning.rpc.Predicate> predicates) {
    if (requiredColumns == null || requiredColumns.isEmpty()) {
      return filterByPredicates(plan, predicates);
    }
    Set<String> cols = new HashSet<>(requiredColumns);
    PlanDescriptor.Builder pb = plan.toBuilder().clearDataFiles().clearDeleteFiles();
    for (PlanFile pf : plan.getDataFilesList()) {
      pb.addDataFiles(filterColumns(pf, cols));
    }
    for (PlanFile pf : plan.getDeleteFilesList()) {
      pb.addDeleteFiles(filterColumns(pf, cols));
    }
    return filterByPredicates(pb.build(), predicates);
  }

  private PlanFile filterColumns(PlanFile pf, Set<String> cols) {
    PlanFile.Builder b = pf.toBuilder().clearColumns();
    for (var c : pf.getColumnsList()) {
      if (cols.contains(c.getColumnName())) {
        b.addColumns(c);
      }
    }
    return b.build();
  }

  private PlanDescriptor filterByPredicates(
      PlanDescriptor plan, List<ai.floedb.metacat.planning.rpc.Predicate> predicates) {
    if (predicates == null || predicates.isEmpty()) {
      return plan;
    }
    List<ai.floedb.metacat.planning.rpc.Predicate> equalsPreds =
        predicates.stream()
            .filter(p -> !p.getExpression().isBlank())
            .collect(Collectors.toList());
    if (equalsPreds.isEmpty()) {
      return plan;
    }
    PlanDescriptor.Builder pb = plan.toBuilder().clearDataFiles().clearDeleteFiles();
    for (PlanFile pf : plan.getDataFilesList()) {
      if (matches(pf, equalsPreds)) {
        pb.addDataFiles(pf);
      }
    }
    for (PlanFile pf : plan.getDeleteFilesList()) {
      if (matches(pf, equalsPreds)) {
        pb.addDeleteFiles(pf);
      }
    }
    return pb.build();
  }

  private boolean matches(PlanFile pf, List<ai.floedb.metacat.planning.rpc.Predicate> preds) {
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
