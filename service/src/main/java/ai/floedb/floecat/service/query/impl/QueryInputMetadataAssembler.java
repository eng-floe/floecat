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

import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.ExpansionMap;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.query.rpc.TableObligations;
import ai.floedb.floecat.service.query.resolver.ObligationsResolver;
import ai.floedb.floecat.service.query.resolver.QueryInputResolver;
import ai.floedb.floecat.service.query.resolver.ViewExpansionResolver;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class QueryInputMetadataAssembler {

  @Inject QueryInputResolver inputResolver;
  @Inject ViewExpansionResolver expansions;
  @Inject ObligationsResolver obligations;
  @Inject Observability observability;

  /**
   * Combines the existing resolvers to build the lifecycle metadata that BeginQuery should store
   * when inputs are supplied up front.
   */
  public QueryInputMetadata assemble(
      String correlationId,
      List<QueryInput> inputs,
      Optional<Timestamp> asOfDefault,
      ResourceId defaultCatalogId) {
    PhaseDiagnostics diagnostics = diagnostics("query_input_metadata");
    long startedNanos = System.nanoTime();
    String outcome = "completed";
    diagnostics.put("correlation_id", correlationId);
    diagnostics.put("inputs", inputs.size());
    if (inputs.isEmpty()) {
      try {
        outcome = "empty";
        return QueryInputMetadata.empty();
      } finally {
        diagnostics.put("outcome", outcome);
        diagnostics.nanos("total", System.nanoTime() - startedNanos);
        diagnostics.emit("floecat.query_input_metadata.summary");
      }
    }

    try {
      var resolution =
          diagnostics.time(
              "resolve_inputs",
              () ->
                  inputResolver.resolveInputs(
                      correlationId,
                      inputs,
                      asOfDefault,
                      Optional.of(defaultCatalogId),
                      new LinkedHashMap<>(),
                      diagnostics));
      diagnostics.put("resolved_inputs", resolution.resolved().size());
      SnapshotSet snapshotSet = resolution.snapshotSet();
      diagnostics.put("snapshot_pins", snapshotSet.getPinsCount());
      ExpansionMap expansionMap =
          diagnostics.time(
              "compute_expansion",
              () ->
                  expansions.computeExpansion(
                      correlationId, List.copyOf(resolution.resolved()), diagnostics));
      var obligationsResult =
          diagnostics.time(
              "resolve_obligations",
              () ->
                  obligations.resolveObligations(
                      correlationId, snapshotSet.getPinsList(), diagnostics));
      diagnostics.put("obligations", obligationsResult.obligations().size());
      diagnostics.put("obligation_bytes", obligationsResult.bytes().length);

      return new QueryInputMetadata(
          snapshotSet, expansionMap, obligationsResult.bytes(), obligationsResult.obligations());
    } catch (RuntimeException | Error e) {
      outcome = "failed";
      diagnostics.put("error", e.getClass().getSimpleName());
      throw e;
    } finally {
      diagnostics.put("outcome", outcome);
      diagnostics.nanos("total", System.nanoTime() - startedNanos);
      diagnostics.emit("floecat.query_input_metadata.summary");
    }
  }

  /**
   * Encapsulates the blobs stored inside {@link QueryContext} for resolved inputs.
   *
   * <p>Empty metadata uses the default instances so BeginQuery can still create contexts when no
   * inputs are supplied.
   */
  public record QueryInputMetadata(
      SnapshotSet snapshotSet,
      ExpansionMap expansionMap,
      byte[] obligationsBytes,
      List<TableObligations> obligations) {

    public static QueryInputMetadata empty() {
      return new QueryInputMetadata(
          SnapshotSet.getDefaultInstance(),
          ExpansionMap.getDefaultInstance(),
          new byte[0],
          Collections.emptyList());
    }
  }

  private PhaseDiagnostics diagnostics(String operation) {
    return observability == null
        ? PhaseDiagnostics.NOOP
        : observability.diagnostics("service", operation);
  }
}
