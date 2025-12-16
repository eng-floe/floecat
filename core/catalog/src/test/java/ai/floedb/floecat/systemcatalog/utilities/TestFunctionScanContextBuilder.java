package ai.floedb.floecat.systemcatalog.utilities;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineKey;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
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
            .setAccountId("_system")
            .setKind(ResourceKind.RK_CATALOG)
            .setId(catalogName)
            .build());
  }

  public NamespaceNode addNamespace(String name) {
    ResourceId id =
        ResourceId.newBuilder()
            .setAccountId("_system")
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
                .setAccountId("_system")
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
