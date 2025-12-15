package ai.floedb.floecat.systemcatalog.utils;

import ai.floedb.floecat.common.rpc.*;
import ai.floedb.floecat.metagraph.model.*;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.hint.SystemCatalogHintProvider;
import ai.floedb.floecat.systemcatalog.provider.StaticSystemCatalogProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemDefinitionRegistry;
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

  public static SystemCatalogHintProvider providerFrom(String engine, SystemCatalogData data) {
    var registry =
        new SystemDefinitionRegistry(new StaticSystemCatalogProvider(Map.of(engine, data)));
    var nodeRegistry = new SystemNodeRegistry(registry);
    return new SystemCatalogHintProvider(nodeRegistry);
  }

  public static String json(EngineHint h) {
    return new String(h.payload(), StandardCharsets.UTF_8);
  }

  // --- Build function nodes ------------------------------------------------

  public static FunctionNode functionNode(
      String engine, List<String> argTypes, String retType, String fullname) {

    List<ResourceId> argIds =
        argTypes.stream()
            .map(
                a ->
                    SystemNodeRegistry.resourceId(
                        engine, ResourceKind.RK_TYPE, NameRef.newBuilder().setName(a).build()))
            .toList();

    ResourceId retId =
        SystemNodeRegistry.resourceId(
            engine, ResourceKind.RK_TYPE, NameRef.newBuilder().setName(retType).build());

    NameRef fn = nr(fullname);

    ResourceId fnId = SystemNodeRegistry.resourceId(engine, ResourceKind.RK_FUNCTION, fn);

    return new FunctionNode(
        fnId, 1L, Instant.EPOCH, "16.0", fullname, argIds, retId, false, false, Map.of());
  }

  // --- Build operator nodes -----------------------------------------------

  public static OperatorNode operatorNode(
      String engine, String name, String left, String right, String ret) {

    return new OperatorNode(
        SystemNodeRegistry.resourceId(engine, ResourceKind.RK_OPERATOR, nr(name)),
        1L,
        Instant.EPOCH,
        engine,
        name,
        SystemNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr(left)),
        SystemNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr(right)),
        SystemNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr(ret)),
        false,
        false,
        Map.of());
  }

  // --- Build cast nodes ----------------------------------------------------

  public static CastNode castNode(
      String engine, String name, String srcType, String dstType, String method) {

    return new CastNode(
        SystemNodeRegistry.resourceId(engine, ResourceKind.RK_CAST, nr(name)),
        1L,
        Instant.EPOCH,
        engine,
        SystemNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr(srcType)),
        SystemNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr(dstType)),
        method,
        Map.of());
  }

  // --- Build type nodes ----------------------------------------------------

  public static TypeNode typeNode(String engine, String name) {
    return new TypeNode(
        SystemNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr(name)),
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

  public static CollationNode collationNode(String engine, String name) {
    return new CollationNode(
        SystemNodeRegistry.resourceId(engine, ResourceKind.RK_COLLATION, nr(name)),
        1L,
        Instant.EPOCH,
        engine,
        name,
        "en_US",
        Map.of());
  }

  // --- Build aggregate nodes ----------------------------------------------

  public static AggregateNode aggregateNode(
      String engine, String name, List<String> args, String ret) {

    List<ResourceId> argIds =
        args.stream()
            .map(a -> SystemNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr(a)))
            .toList();

    return new AggregateNode(
        SystemNodeRegistry.resourceId(engine, ResourceKind.RK_AGGREGATE, nr(name)),
        1L,
        Instant.EPOCH,
        engine,
        name,
        argIds,
        SystemNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr("state")),
        SystemNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr(ret)),
        Map.of());
  }
}
