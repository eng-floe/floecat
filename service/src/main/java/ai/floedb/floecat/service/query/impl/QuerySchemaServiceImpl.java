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

package ai.floedb.floecat.service.query.impl;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.connector.common.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.DescribeInputsRequest;
import ai.floedb.floecat.query.rpc.DescribeInputsResponse;
import ai.floedb.floecat.query.rpc.ExpansionMap;
import ai.floedb.floecat.query.rpc.QuerySchemaService;
import ai.floedb.floecat.query.rpc.RelationPin;
import ai.floedb.floecat.query.rpc.RelationPinIdentity;
import ai.floedb.floecat.query.rpc.RelationPinSet;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.PinValidator;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.QueryPins;
import ai.floedb.floecat.service.query.catalog.UserObjectBundleUtils;
import ai.floedb.floecat.service.query.resolver.ObligationsResolver;
import ai.floedb.floecat.service.query.resolver.QueryInputResolver;
import ai.floedb.floecat.service.query.resolver.ViewExpansionResolver;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.jboss.logging.Logger;

/**
 * Implements DescribeInputs:
 *
 * <p>- resolves inputs - resolves snapshot pins - loads schemas (in order of inputs) - computes
 * expansions + obligations (stored, not returned)
 *
 * <p>Response: repeated SchemaDescriptor schemas (one per input, in order)
 */
@Singleton
@GrpcService
public class QuerySchemaServiceImpl extends BaseServiceImpl implements QuerySchemaService {

  private static final Logger LOG = Logger.getLogger(QuerySchemaServiceImpl.class);

  @Inject QueryInputResolver inputResolver;
  @Inject LogicalSchemaMapper schemaMapper;
  @Inject ObligationsResolver obligations;
  @Inject ViewExpansionResolver expansions;
  @Inject QueryContextStore queryStore;
  @Inject CatalogOverlay catalogOverlay;
  @Inject PinValidator pinValidator;
  @Inject Observability observability;

  @Override
  public Uni<DescribeInputsResponse> describeInputs(DescribeInputsRequest request) {
    var L = LogHelper.start(LOG, "DescribeInputs");

    return mapFailures(
            run(
                () -> {
                  PhaseDiagnostics diagnostics = diagnostics("describe_inputs");
                  long startedNanos = System.nanoTime();
                  String outcome = "completed";
                  String queryId = mustNonEmpty(request.getQueryId(), "query_id", correlationId());
                  diagnostics.put("query_id", queryId);
                  diagnostics.put("inputs", request.getInputsCount());

                  try {
                    var ctxOpt =
                        diagnostics.time("query_context_get", () -> queryStore.get(queryId));
                    if (ctxOpt.isEmpty()) {
                      throw GrpcErrors.notFound(
                          correlationId(), QUERY_NOT_FOUND, java.util.Map.of("query_id", queryId));
                    }
                    var ctx = ctxOpt.get();

                    var asOfDefault =
                        diagnostics.time(
                            "parse_asof_default", () -> ctx.parseAsOfDefault(correlationId()));

                    // Resolve inputs → resolved ids + snapshot pins (tables and/or view base
                    // tables)
                    var rr =
                        diagnostics.time(
                            "resolve_inputs",
                            () ->
                                inputResolver.resolveInputs(
                                    ctx.getQueryId(),
                                    correlationId(),
                                    request.getInputsList(),
                                    asOfDefault,
                                    Optional.of(ctx.getQueryDefaultCatalogId()),
                                    new java.util.LinkedHashMap<>(),
                                    diagnostics));
                    diagnostics.put("resolved_inputs", rr.resolved().size());

                    // Merge this resolution's pins into the live context FIRST, under the store's
                    // atomic update, and read back the winner. Schemas, obligations, and pin
                    // identities are then built from the pin that actually won under first-touch —
                    // not this call's candidate resolution — so a concurrent lazy resolver that
                    // pinned a different snapshot for a shared table can never make us describe a
                    // losing pin (an incompatible temporal intent still fails via the shared
                    // conflict rule inside mergeSets). Committing pins before any blob read also
                    // roots them before the schema read; the resolver already registered them as
                    // transient GC roots at resolution, covering the brief resolve→commit window.
                    QueryContext committed =
                        diagnostics.time(
                            "query_context_pin_merge",
                            () ->
                                queryStore
                                    .update(
                                        queryId,
                                        existing ->
                                            existing.toBuilder()
                                                .relationPins(
                                                    QueryPins.mergeSets(
                                                            existing.parseRelationPins(
                                                                correlationId()),
                                                            rr.relationPinSet(),
                                                            correlationId())
                                                        .toByteArray())
                                                .build())
                                    .orElseThrow(
                                        () ->
                                            GrpcErrors.notFound(
                                                correlationId(),
                                                QUERY_NOT_FOUND,
                                                java.util.Map.of("query_id", queryId))));

                    RelationPinSet winnerPins = committed.parseRelationPins(correlationId());
                    diagnostics.put("snapshot_pins", winnerPins.getPinsCount());
                    Map<ResourceId, TablePin> pinByTableId = new HashMap<>();
                    for (RelationPin pin : winnerPins.getPinsList()) {
                      if (pin.hasTablePin()) {
                        pinByTableId.put(pin.getTablePin().getTableId(), pin.getTablePin());
                      }
                    }

                    DescribeInputsResponse.Builder out = DescribeInputsResponse.newBuilder();

                    // One schema and one positional pin identity per input (resolved id), in order.
                    // A table carries its winner pin's opaque identity; a view carries a default
                    // (empty) identity so the two lists stay aligned with `schemas`.
                    try (var ignored = diagnostics.timer("schema_describe")) {
                      for (ResourceId rid : rr.resolved()) {
                        out.addSchemas(schemaForResolvedInput(correlationId(), rid, pinByTableId));
                        TablePin winner = pinByTableId.get(rid);
                        out.addRelationPins(
                            winner != null
                                ? QueryPins.identity(winner)
                                : RelationPinIdentity.getDefaultInstance());
                      }
                    }
                    diagnostics.put("schemas", out.getSchemasCount());

                    // Expansions (from resolved ids) and obligations (from the winner pins of the
                    // tables this request touched) are derived after the pin merge and stored.
                    ExpansionMap expansionMap =
                        diagnostics.time(
                            "compute_expansion",
                            () ->
                                expansions.computeExpansion(
                                    correlationId(), List.copyOf(rr.resolved()), diagnostics));
                    byte[] expansionBytes = expansionMap.toByteArray();
                    diagnostics.put("expansion_bytes", expansionBytes.length);

                    // Obligations cover the query's FULL committed pin set, not just the tables
                    // this call first-touched — a later DescribeInputs must not narrow what an
                    // earlier one obligated.
                    List<SnapshotPin> obligationPins = new ArrayList<>();
                    for (RelationPin pin : winnerPins.getPinsList()) {
                      if (pin.hasTablePin()) {
                        obligationPins.add(QueryPins.toSnapshotPin(pin.getTablePin()));
                      }
                    }
                    var obligationsResult =
                        diagnostics.time(
                            "resolve_obligations",
                            () ->
                                obligations.resolveObligations(
                                    correlationId(), obligationPins, diagnostics));
                    byte[] obligationsBytes = obligationsResult.bytes();
                    diagnostics.put("obligations", obligationsResult.obligations().size());
                    diagnostics.put("obligation_bytes", obligationsBytes.length);

                    // Store the derived expansion + obligations. Pins were committed above, so this
                    // is intentionally a separate (non-atomic-with-pins) update: expansion is only
                    // read as diagnostics by GetQuery and obligations have no server-side reader,
                    // so
                    // a brief window where pins are committed but these are not is harmless. Both
                    // are
                    // recomputed on every DescribeInputs, so a lost update self-heals.
                    diagnostics.time(
                        "query_context_update",
                        () -> {
                          var result =
                              queryStore.update(
                                  queryId,
                                  existing ->
                                      existing.toBuilder()
                                          .expansionMap(expansionBytes)
                                          .obligations(obligationsBytes)
                                          .build());
                          if (result.isEmpty()) {
                            throw GrpcErrors.notFound(
                                correlationId(),
                                QUERY_NOT_FOUND,
                                java.util.Map.of("query_id", queryId));
                          }
                        });

                    return out.build();
                  } catch (RuntimeException | Error e) {
                    outcome = "failed";
                    diagnostics.put("error", e.getClass().getSimpleName());
                    throw e;
                  } finally {
                    diagnostics.put("outcome", outcome);
                    diagnostics.nanos("total", System.nanoTime() - startedNanos);
                    diagnostics.emit("floecat.describe_inputs.summary");
                  }
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private SchemaDescriptor schemaForResolvedInput(
      String correlationId, ResourceId rid, Map<ResourceId, TablePin> pinByTableId) {

    return switch (rid.getKind()) {
      case RK_TABLE -> {
        TablePin pin = pinByTableId.get(rid);
        if (pin == null) {
          // Should never happen because resolveInputs attaches pins for every table input; treat as
          // an
          // internal invariant failure if it does.
          throw GrpcErrors.internal(
              correlationId, QUERY_TABLE_NOT_PINNED, java.util.Map.of("table_id", rid.getId()));
        }
        yield describeTable(correlationId, rid, pin);
      }
      case RK_VIEW -> describeView(correlationId, rid);
      default ->
          throw GrpcErrors.invalidArgument(
              correlationId, QUERY_INPUT_INVALID, java.util.Map.of("resource_id", rid.getId()));
    };
  }

  private SchemaDescriptor describeTable(String correlationId, ResourceId rid, TablePin pin) {
    // Fail hard on a missing/mismatched pinned blob rather than re-reading current catalog state.
    pinValidator.validate(correlationId, pin);
    SnapshotRef snapshotRef = SnapshotRef.newBuilder().setSnapshotId(pin.getSnapshotId()).build();
    CatalogOverlay.SchemaResolution resolved =
        catalogOverlay.schemaFor(
            correlationId, rid, snapshotRef, pin.getTableBlobUri(), pin.getSnapshotBlobUri());
    return UserObjectBundleUtils.qualifyNestedColumnNames(
        schemaMapper.map(resolved.table(), resolved.schemaJson()));
  }

  private SchemaDescriptor describeView(String correlationId, ResourceId rid) {
    ViewNode viewNode =
        catalogOverlay
            .resolve(rid)
            .filter(ViewNode.class::isInstance)
            .map(ViewNode.class::cast)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(correlationId, VIEW, java.util.Map.of("id", rid.getId())));

    return SchemaDescriptor.newBuilder().addAllColumns(viewNode.outputColumns()).build();
  }

  private PhaseDiagnostics diagnostics(String operation) {
    return observability == null
        ? PhaseDiagnostics.NOOP
        : observability.diagnostics("service", operation);
  }
}
