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
import ai.floedb.floecat.query.rpc.ExpansionMap;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.query.rpc.TableObligations;
import ai.floedb.floecat.service.query.resolver.ObligationsResolver;
import ai.floedb.floecat.service.query.resolver.QueryInputResolver;
import ai.floedb.floecat.service.query.resolver.ViewExpansionResolver;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class QueryInputMetadataAssembler {

  @Inject QueryInputResolver inputResolver;
  @Inject ViewExpansionResolver expansions;
  @Inject ObligationsResolver obligations;

  /**
   * Combines the existing resolvers to build the lifecycle metadata that BeginQuery should store
   * when inputs are supplied up front.
   */
  public QueryInputMetadata assemble(
      String correlationId, List<QueryInput> inputs, Optional<Timestamp> asOfDefault) {
    if (inputs.isEmpty()) {
      return QueryInputMetadata.empty();
    }

    var resolution = inputResolver.resolveInputs(correlationId, inputs, asOfDefault);
    SnapshotSet snapshotSet = resolution.snapshotSet();
    ExpansionMap expansionMap =
        expansions.computeExpansion(correlationId, List.copyOf(resolution.resolved()));
    var obligationsResult =
        obligations.resolveObligations(correlationId, snapshotSet.getPinsList());

    return new QueryInputMetadata(
        snapshotSet, expansionMap, obligationsResult.bytes(), obligationsResult.obligations());
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
}
