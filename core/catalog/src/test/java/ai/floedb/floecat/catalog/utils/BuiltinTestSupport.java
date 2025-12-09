package ai.floedb.floecat.catalog.utils;

import ai.floedb.floecat.catalog.builtin.graph.BuiltinNodeRegistry;
import ai.floedb.floecat.catalog.builtin.graph.model.BuiltinAggregateNode;
import ai.floedb.floecat.catalog.builtin.graph.model.BuiltinCastNode;
import ai.floedb.floecat.catalog.builtin.graph.model.BuiltinCollationNode;
import ai.floedb.floecat.catalog.builtin.graph.model.BuiltinFunctionNode;
import ai.floedb.floecat.catalog.builtin.graph.model.BuiltinOperatorNode;
import ai.floedb.floecat.catalog.builtin.graph.model.BuiltinTypeNode;
import ai.floedb.floecat.catalog.builtin.hint.BuiltinCatalogHintProvider;
import ai.floedb.floecat.catalog.builtin.provider.StaticBuiltinCatalogProvider;
import ai.floedb.floecat.catalog.builtin.registry.BuiltinCatalogData;
import ai.floedb.floecat.catalog.builtin.registry.BuiltinDefinitionRegistry;
import ai.floedb.floecat.common.rpc.*;
import ai.floedb.floecat.metagraph.model.EngineHint;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

public final class BuiltinTestSupport {

  public static NameRef nr(String full) {
    int idx = full.indexOf('.');
    if (idx < 0) return NameRef.newBuilder().setName(full).build();
    return NameRef.newBuilder()
        .addPath(full.substring(0, idx))
        .setName(full.substring(idx + 1))
        .build();
  }

  public static BuiltinCatalogHintProvider providerFrom(String engine, BuiltinCatalogData data) {
    var registry =
        new BuiltinDefinitionRegistry(new StaticBuiltinCatalogProvider(Map.of(engine, data)));
    var nodeRegistry = new BuiltinNodeRegistry(registry);
    return new BuiltinCatalogHintProvider(nodeRegistry);
  }

  public static String json(EngineHint h) {
    return new String(h.payload(), StandardCharsets.UTF_8);
  }

  // --- Build function nodes ------------------------------------------------

  public static BuiltinFunctionNode functionNode(
      String engine, List<String> argTypes, String retType, String fullname) {

    List<ResourceId> argIds =
        argTypes.stream()
            .map(
                a ->
                    BuiltinNodeRegistry.resourceId(
                        engine, ResourceKind.RK_TYPE, NameRef.newBuilder().setName(a).build()))
            .toList();

    ResourceId retId =
        BuiltinNodeRegistry.resourceId(
            engine, ResourceKind.RK_TYPE, NameRef.newBuilder().setName(retType).build());

    NameRef fn = nr(fullname);

    ResourceId fnId = BuiltinNodeRegistry.resourceId(engine, ResourceKind.RK_FUNCTION, fn);

    return new BuiltinFunctionNode(
        fnId, 1L, Instant.EPOCH, "16.0", fullname, argIds, retId, false, false, Map.of());
  }

  // --- Build operator nodes -----------------------------------------------

  public static BuiltinOperatorNode operatorNode(
      String engine, String name, String left, String right, String ret) {

    return new BuiltinOperatorNode(
        BuiltinNodeRegistry.resourceId(engine, ResourceKind.RK_OPERATOR, nr(name)),
        1L,
        Instant.EPOCH,
        engine,
        name,
        BuiltinNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr(left)),
        BuiltinNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr(right)),
        BuiltinNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr(ret)),
        false,
        false,
        Map.of());
  }

  // --- Build cast nodes ----------------------------------------------------

  public static BuiltinCastNode castNode(
      String engine, String name, String srcType, String dstType, String method) {

    return new BuiltinCastNode(
        BuiltinNodeRegistry.resourceId(engine, ResourceKind.RK_CAST, nr(name)),
        1L,
        Instant.EPOCH,
        engine,
        BuiltinNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr(srcType)),
        BuiltinNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr(dstType)),
        method,
        Map.of());
  }

  // --- Build type nodes ----------------------------------------------------

  public static BuiltinTypeNode typeNode(String engine, String name) {
    return new BuiltinTypeNode(
        BuiltinNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr(name)),
        1L,
        Instant.EPOCH,
        engine,
        name,
        "",
        false,
        null,
        Map.of());
  }

  // --- Build collation nodes ----------------------------------------------

  public static BuiltinCollationNode collationNode(String engine, String name) {
    return new BuiltinCollationNode(
        BuiltinNodeRegistry.resourceId(engine, ResourceKind.RK_COLLATION, nr(name)),
        1L,
        Instant.EPOCH,
        engine,
        name,
        "en_US",
        Map.of());
  }

  // --- Build aggregate nodes ----------------------------------------------

  public static BuiltinAggregateNode aggregateNode(
      String engine, String name, List<String> args, String ret) {

    List<ResourceId> argIds =
        args.stream()
            .map(a -> BuiltinNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr(a)))
            .toList();

    return new BuiltinAggregateNode(
        BuiltinNodeRegistry.resourceId(engine, ResourceKind.RK_AGGREGATE, nr(name)),
        1L,
        Instant.EPOCH,
        engine,
        name,
        argIds,
        BuiltinNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr("state")),
        BuiltinNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr(ret)),
        Map.of());
  }
}
