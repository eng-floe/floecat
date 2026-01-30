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

package ai.floedb.floecat.systemcatalog.utils;

import ai.floedb.floecat.common.rpc.*;
import ai.floedb.floecat.metagraph.model.*;
import ai.floedb.floecat.systemcatalog.def.SystemAggregateDef;
import ai.floedb.floecat.systemcatalog.def.SystemCastDef;
import ai.floedb.floecat.systemcatalog.def.SystemCastMethod;
import ai.floedb.floecat.systemcatalog.def.SystemCollationDef;
import ai.floedb.floecat.systemcatalog.def.SystemFunctionDef;
import ai.floedb.floecat.systemcatalog.def.SystemOperatorDef;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.hint.SystemCatalogHintProvider;
import ai.floedb.floecat.systemcatalog.provider.FloecatInternalProvider;
import ai.floedb.floecat.systemcatalog.provider.StaticSystemCatalogProvider;
import ai.floedb.floecat.systemcatalog.provider.SystemObjectScannerProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemDefinitionRegistry;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import ai.floedb.floecat.systemcatalog.util.SignatureUtil;
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
    var nodeRegistry = new SystemNodeRegistry(registry, new FloecatInternalProvider(), providers());
    return new SystemCatalogHintProvider(nodeRegistry, registry);
  }

  public static String json(EngineHint h) {
    return new String(h.payload(), StandardCharsets.UTF_8);
  }

  // --- Build function nodes ------------------------------------------------

  public static FunctionNode functionNode(
      String engine, List<String> argTypes, String retType, String fullname) {

    List<NameRef> argRefs = argTypes.stream().map(BuiltinTestSupport::nr).toList();
    List<ResourceId> argIds =
        argRefs.stream()
            .map(a -> SystemNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, a))
            .toList();

    NameRef retRef = nr(retType);
    ResourceId retId = SystemNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, retRef);

    NameRef fn = nr(fullname);
    String namespaceCanonical = NameRefUtil.namespaceFromCanonical(NameRefUtil.canonical(fn));
    ResourceId namespaceId =
        namespaceCanonical.isEmpty()
            ? null
            : SystemNodeRegistry.resourceId(
                engine, ResourceKind.RK_NAMESPACE, NameRefUtil.fromCanonical(namespaceCanonical));

    SystemFunctionDef fnDef = new SystemFunctionDef(fn, argRefs, retRef, false, false, List.of());
    String signature = SignatureUtil.functionSignature(fnDef);
    ResourceId fnId = SystemNodeRegistry.resourceId(engine, ResourceKind.RK_FUNCTION, signature);

    return new FunctionNode(
        fnId,
        1L,
        Instant.EPOCH,
        "16.0",
        namespaceId,
        fullname,
        argIds,
        retId,
        false,
        false,
        Map.of());
  }

  // --- Build operator nodes -----------------------------------------------

  public static OperatorNode operatorNode(
      String engine, String name, String left, String right, String ret) {

    SystemOperatorDef opDef =
        new SystemOperatorDef(nr(name), nr(left), nr(right), nr(ret), false, false, List.of());

    return new OperatorNode(
        SystemNodeRegistry.resourceId(
            engine, ResourceKind.RK_OPERATOR, SignatureUtil.operatorSignature(opDef)),
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
      String engine, String name, String srcType, String dstType, SystemCastMethod method) {

    SystemCastDef castDef =
        new SystemCastDef(nr(name), nr(srcType), nr(dstType), method, List.of());

    return new CastNode(
        SystemNodeRegistry.resourceId(
            engine, ResourceKind.RK_CAST, SignatureUtil.castSignature(castDef)),
        1L,
        Instant.EPOCH,
        engine,
        SystemNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr(srcType)),
        SystemNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr(dstType)),
        method.wireValue(),
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

  public static CollationNode collationNode(String engine, String name, String locale) {
    SystemCollationDef collationDef = new SystemCollationDef(nr(name), locale, List.of());
    return new CollationNode(
        SystemNodeRegistry.resourceId(engine, collationDef),
        1L,
        Instant.EPOCH,
        engine,
        name,
        locale,
        Map.of());
  }

  // --- Build aggregate nodes ----------------------------------------------

  public static AggregateNode aggregateNode(
      String engine, String name, List<String> args, String state, String ret) {

    List<ResourceId> argIds =
        args.stream()
            .map(a -> SystemNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr(a)))
            .toList();

    List<NameRef> argRefs = args.stream().map(BuiltinTestSupport::nr).toList();

    SystemAggregateDef aggDef =
        new SystemAggregateDef(nr(name), argRefs, nr(state), nr(ret), List.of());

    return new AggregateNode(
        SystemNodeRegistry.resourceId(
            engine, ResourceKind.RK_AGGREGATE, SignatureUtil.aggregateSignature(aggDef)),
        1L,
        Instant.EPOCH,
        engine,
        name,
        argIds,
        SystemNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr(state)),
        SystemNodeRegistry.resourceId(engine, ResourceKind.RK_TYPE, nr(ret)),
        Map.of());
  }

  private static List<SystemObjectScannerProvider> providers() {
    return List.of();
  }
}
