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

package ai.floedb.floecat.systemcatalog.spi.types;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import ai.floedb.floecat.systemcatalog.utils.BuiltinTestSupport;
import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

final class TypeResolverTest {

  private final SystemObjectScanContext ctx =
      new SystemObjectScanContext(
          new TestCatalogOverlay(),
          NameRef.getDefaultInstance(),
          catalogId(),
          EngineContext.empty());

  @Test
  void resolve_returnsMappedTypeNode() {
    TypeNode type = type("pg_catalog.int4");
    TestCatalogOverlay overlay = (TestCatalogOverlay) ctx.graph();
    overlay.addNode(namespace("pg_catalog"));
    overlay.addNode(type);

    AtomicInteger invocations = new AtomicInteger();
    EngineTypeMapper mapper =
        (logicalType, lookup) -> {
          invocations.incrementAndGet();
          if (logicalType.kind() == LogicalKind.INT) {
            return lookup.findByName("pg_catalog", "int4");
          }
          return Optional.empty();
        };

    TypeResolver resolver = new TypeResolver(ctx, mapper);

    LogicalType logical = LogicalType.of(LogicalKind.INT);
    Optional<TypeNode> resolved = resolver.resolve(logical);

    assertThat(resolved).contains(type);
    assertThat(invokeOnce(resolver, logical)).contains(type);
    assertThat(invocations).hasValue(1);
  }

  @Test
  void resolve_returnsEmptyWhenMapperUnknown() {
    TestCatalogOverlay overlay = (TestCatalogOverlay) ctx.graph();
    overlay.addNode(namespace("pg_catalog"));
    overlay.addNode(type("pg_catalog.text"));

    EngineTypeMapper mapper = (logicalType, lookup) -> Optional.empty();
    TypeResolver resolver = new TypeResolver(ctx, mapper);

    LogicalType stringType = LogicalType.of(LogicalKind.STRING);
    assertThat(resolver.resolve(stringType)).isEmpty();
  }

  @Test
  void resolve_withOnlySystemTypesSkipsUserNamespaceResolution() {
    CountingOverlay overlay = new CountingOverlay();
    ResourceId currentCatalog = userCatalog("user-cat");
    SystemObjectScanContext userCtx =
        new SystemObjectScanContext(
            overlay, NameRef.getDefaultInstance(), currentCatalog, EngineContext.empty());
    NamespaceNode systemNamespace = namespace("pg_catalog");
    TypeNode systemType = type("pg_catalog.int4");
    overlay.addNode(systemNamespace);
    overlay.addNode(systemType);

    EngineTypeMapper mapper =
        (logicalType, lookup) ->
            logicalType.kind() == LogicalKind.INT
                ? lookup.findByName("pg_catalog", "int4")
                : Optional.empty();

    TypeResolver resolver = TypeResolver.forContext(userCtx, mapper);
    assertThat(resolver.resolve(LogicalType.of(LogicalKind.INT))).contains(systemType);
    assertThat(overlay.listTypesCount()).isEqualTo(1);
    assertThat(overlay.resolveCount()).isZero();
    assertThat(overlay.listNamespacesCount()).isZero();
  }

  @Test
  void forContext_reusesResolverForSameContextAndMapper() {
    CountingOverlay overlay = new CountingOverlay();
    SystemObjectScanContext userCtx =
        new SystemObjectScanContext(
            overlay, NameRef.getDefaultInstance(), userCatalog("user-cat"), EngineContext.empty());
    overlay.addNode(namespace("pg_catalog"));
    overlay.addNode(type("pg_catalog.int4"));

    EngineTypeMapper mapper =
        (logicalType, lookup) ->
            logicalType.kind() == LogicalKind.INT
                ? lookup.findByName("pg_catalog", "int4")
                : Optional.empty();

    TypeResolver first = TypeResolver.forContext(userCtx, mapper);
    TypeResolver second = TypeResolver.forContext(userCtx, mapper);

    assertThat(first).isSameAs(second);
    assertThat(overlay.listTypesCount()).isEqualTo(1);
  }

  @Test
  void resolve_prefersUserTypeOverSystemTypeWhenNamesCollide() {
    TestCatalogOverlay overlay = new TestCatalogOverlay();
    ResourceId currentCatalog = userCatalog("user-cat");
    SystemObjectScanContext userCtx =
        new SystemObjectScanContext(
            overlay, NameRef.getDefaultInstance(), currentCatalog, EngineContext.empty());

    NamespaceNode systemNamespace = namespace("pg_catalog");
    TypeNode systemType = type("pg_catalog.int4");
    NamespaceNode userNamespace = userNamespace("pg_catalog", currentCatalog, "user-ns-pg_catalog");
    TypeNode userType = userType("pg_catalog.int4", userNamespace.id());

    overlay.addNode(systemNamespace);
    overlay.addNode(systemType);
    overlay.addNode(userNamespace);
    overlay.addNode(userType);

    EngineTypeMapper mapper =
        (logicalType, lookup) ->
            logicalType.kind() == LogicalKind.INT
                ? lookup.findByName("pg_catalog", "int4")
                : Optional.empty();

    TypeResolver resolver = new TypeResolver(userCtx, mapper);
    assertThat(resolver.resolve(LogicalType.of(LogicalKind.INT))).contains(userType);
  }

  @Test
  void resolve_usesOnlyCurrentCatalogForUserTypes() {
    TestCatalogOverlay overlay = new TestCatalogOverlay();
    ResourceId currentCatalog = userCatalog("user-cat-a");
    SystemObjectScanContext userCtx =
        new SystemObjectScanContext(
            overlay, NameRef.getDefaultInstance(), currentCatalog, EngineContext.empty());

    NamespaceNode currentNamespace = userNamespace("pg_catalog", currentCatalog, "user-ns-a");
    NamespaceNode otherNamespace =
        userNamespace("pg_catalog", userCatalog("user-cat-b"), "user-ns-b");
    TypeNode currentType = userType("pg_catalog.int4", currentNamespace.id(), "user-type-a");
    TypeNode otherType = userType("pg_catalog.int4", otherNamespace.id(), "user-type-b");

    overlay.addNode(currentNamespace);
    overlay.addNode(otherNamespace);
    overlay.addNode(currentType);
    overlay.addNode(otherType);

    EngineTypeMapper mapper =
        (logicalType, lookup) ->
            logicalType.kind() == LogicalKind.INT
                ? lookup.findByName("pg_catalog", "int4")
                : Optional.empty();

    TypeResolver resolver = new TypeResolver(userCtx, mapper);
    assertThat(resolver.resolve(LogicalType.of(LogicalKind.INT))).contains(currentType);
  }

  @Test
  void resolve_throwsWhenDuplicateCanonicalSystemTypesExist() {
    TestCatalogOverlay overlay = new TestCatalogOverlay();
    SystemObjectScanContext systemCtx =
        new SystemObjectScanContext(
            overlay, NameRef.getDefaultInstance(), catalogId(), EngineContext.empty());

    NamespaceNode first = namespaceWithId("pg_catalog", "ns-1");
    NamespaceNode second = namespaceWithId("pg_catalog", "ns-2");
    TypeNode firstType = type("pg_catalog.int4");
    TypeNode secondType = typeWithId("pg_catalog.int4", second.id(), "sys-type-dup-2");
    TypeNode firstTypeRebound =
        new TypeNode(
            firstType.id(),
            1,
            Instant.EPOCH,
            "1.0",
            first.id(),
            BuiltinTestSupport.leafName("pg_catalog.int4"),
            "U",
            false,
            ResourceId.getDefaultInstance(),
            Map.of());

    overlay.addNode(first);
    overlay.addNode(second);
    overlay.addNode(firstTypeRebound);
    overlay.addNode(secondType);

    EngineTypeMapper mapper =
        (logicalType, lookup) ->
            logicalType.kind() == LogicalKind.INT
                ? lookup.findByName("pg_catalog", "int4")
                : Optional.empty();

    TypeResolver resolver = new TypeResolver(systemCtx, mapper);
    assertThatThrownBy(() -> resolver.resolve(LogicalType.of(LogicalKind.INT)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Duplicate canonical type in lookup scope");
  }

  private Optional<TypeNode> invokeOnce(TypeResolver resolver, LogicalType logical) {
    return resolver.resolve(logical);
  }

  private static ResourceId catalogId() {
    return SystemNodeRegistry.systemCatalogContainerId("floedb");
  }

  private static TypeNode type(String displayName) {
    return new TypeNode(
        typeId(displayName),
        1,
        Instant.EPOCH,
        "1.0",
        BuiltinTestSupport.namespaceIdForQualifiedName("floedb", displayName),
        BuiltinTestSupport.leafName(displayName),
        "U",
        false,
        ResourceId.getDefaultInstance(),
        Map.of());
  }

  private static TypeNode userType(String displayName, ResourceId namespaceId) {
    return userType(displayName, namespaceId, "user-type-" + BuiltinTestSupport.leafName(displayName));
  }

  private static TypeNode userType(String displayName, ResourceId namespaceId, String id) {
    return new TypeNode(
        ResourceId.newBuilder()
            .setAccountId("acct-user")
            .setKind(ResourceKind.RK_TYPE)
            .setId(id)
            .build(),
        1,
        Instant.EPOCH,
        "1.0",
        namespaceId,
        BuiltinTestSupport.leafName(displayName),
        "U",
        false,
        ResourceId.getDefaultInstance(),
        Map.of());
  }

  private static NamespaceNode namespace(String name) {
    return new NamespaceNode(
        SystemNodeRegistry.resourceId("floedb", ResourceKind.RK_NAMESPACE, asNameRef(name)),
        1,
        Instant.EPOCH,
        catalogId(),
        List.of(),
        name,
        GraphNodeOrigin.SYSTEM,
        Map.of(),
        Map.of());
  }

  private static NamespaceNode userNamespace(String name) {
    return userNamespace(name, userCatalog("user-cat"), "user-ns-" + name);
  }

  private static NamespaceNode userNamespace(String name, ResourceId catalogId, String namespaceId) {
    return new NamespaceNode(
        ResourceId.newBuilder()
            .setAccountId("acct-user")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId(namespaceId)
            .build(),
        1,
        Instant.EPOCH,
        catalogId,
        List.of(),
        name,
        GraphNodeOrigin.SYSTEM,
        Map.of(),
        Map.of());
  }

  private static ResourceId userCatalog(String id) {
    return ResourceId.newBuilder()
        .setAccountId("acct-user")
        .setKind(ResourceKind.RK_CATALOG)
        .setId(id)
        .build();
  }

  private static ResourceId typeId(String displayName) {
    return SystemNodeRegistry.resourceId("floedb", ResourceKind.RK_TYPE, asNameRef(displayName));
  }

  private static NameRef asNameRef(String qualified) {
    if (qualified == null || qualified.isBlank()) {
      return NameRef.getDefaultInstance();
    }
    String[] parts = qualified.split("\\.");
    return NameRefUtil.name(parts);
  }

  private static NamespaceNode namespaceWithId(String name, String id) {
    return new NamespaceNode(
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId(id)
            .build(),
        1,
        Instant.EPOCH,
        catalogId(),
        List.of(),
        name,
        GraphNodeOrigin.SYSTEM,
        Map.of(),
        Map.of());
  }

  private static TypeNode typeWithId(String qualifiedTypeName, ResourceId namespaceId, String id) {
    return new TypeNode(
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_TYPE)
            .setId(id)
            .build(),
        1,
        Instant.EPOCH,
        "1.0",
        namespaceId,
        BuiltinTestSupport.leafName(qualifiedTypeName),
        "U",
        false,
        ResourceId.getDefaultInstance(),
        Map.of());
  }

  private static final class CountingOverlay extends TestCatalogOverlay {
    private final AtomicInteger resolveCount = new AtomicInteger();
    private final AtomicInteger listTypesCount = new AtomicInteger();
    private final AtomicInteger listNamespacesCount = new AtomicInteger();

    @Override
    public Optional<ai.floedb.floecat.metagraph.model.GraphNode> resolve(ResourceId id) {
      resolveCount.incrementAndGet();
      return super.resolve(id);
    }

    @Override
    public List<TypeNode> listTypes(ResourceId catalogId) {
      listTypesCount.incrementAndGet();
      return super.listTypes(catalogId);
    }

    @Override
    public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
      listNamespacesCount.incrementAndGet();
      return super.listNamespaces(catalogId);
    }

    private int resolveCount() {
      return resolveCount.get();
    }

    private int listTypesCount() {
      return listTypesCount.get();
    }

    private int listNamespacesCount() {
      return listNamespacesCount.get();
    }
  }
}
