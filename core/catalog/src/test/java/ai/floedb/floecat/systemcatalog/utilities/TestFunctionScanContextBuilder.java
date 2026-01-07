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

package ai.floedb.floecat.systemcatalog.utilities;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineKey;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Test builder for function-based scanners (pg_catalog.pg_proc, aggregates, operators). */
public final class TestFunctionScanContextBuilder extends AbstractTestScanContextBuilder {

  private TestFunctionScanContextBuilder(ResourceId catalogId) {
    super(catalogId);
  }

  public static TestFunctionScanContextBuilder builder(String catalogName) {
    return new TestFunctionScanContextBuilder(
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_CATALOG)
            .setId(catalogName)
            .build());
  }

  public NamespaceNode addNamespace(String name) {
    ResourceId id =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId(name)
            .build();

    NamespaceNode ns =
        new NamespaceNode(
            id,
            1,
            Instant.EPOCH,
            catalogId,
            List.of(),
            name,
            GraphNodeOrigin.SYSTEM,
            Map.of(),
            Optional.empty(),
            Map.of());

    // Make it discoverable via overlay.listNamespaces()
    overlay.addNode(ns);
    return ns;
  }

  public FunctionNode addFunction(
      NamespaceNode ns,
      String name,
      boolean aggregate,
      boolean window,
      Map<EngineKey, EngineHint> hints) {

    FunctionNode fn =
        new FunctionNode(
            ResourceId.newBuilder()
                .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
                .setKind(ResourceKind.RK_FUNCTION)
                .setId(name)
                .build(),
            1,
            Instant.EPOCH,
            "15",
            name,
            List.of(),
            null,
            aggregate,
            window,
            hints);

    overlay.addFunction(ns.id(), fn);
    return fn;
  }

  @Override
  public SystemObjectScanContext build() {
    return super.build();
  }
}
