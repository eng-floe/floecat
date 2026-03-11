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
import ai.floedb.floecat.scanner.spi.MetadataResolutionContext;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import ai.floedb.floecat.systemcatalog.utils.BuiltinTestSupport;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class SystemTypeLookupTest {

  @Test
  void findByName_usesNamespaceIdentity_notDisplayLabel() {
    NamespaceNode pgCatalog = namespace("pg_catalog");
    TypeNode int4 = type("pg_catalog.int4", pgCatalog.id());
    SystemTypeLookup lookup = lookup(List.of(pgCatalog), List.of(int4));

    assertThat(lookup.findByName("pg_catalog", "int4")).contains(int4);
  }

  @Test
  void typeNode_throwsWhenNamespaceIdHasNoIdentity() {
    assertThatThrownBy(
            () ->
                new TypeNode(
                    typeId("pg_catalog.int4"),
                    1,
                    Instant.EPOCH,
                    "1.0",
                    ResourceId.getDefaultInstance(),
                    "int4",
                    "U",
                    false,
                    ResourceId.getDefaultInstance(),
                    Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("namespaceId");
  }

  @Test
  void findByName_distinguishesNamespaces_withSameLeafTypeName() {
    NamespaceNode pgCatalog = namespace("pg_catalog");
    NamespaceNode other = namespace("other");
    TypeNode pgInt4 = type("pg_catalog.int4", pgCatalog.id());
    TypeNode otherInt4 = type("other.int4", other.id());
    SystemTypeLookup lookup = lookup(List.of(pgCatalog, other), List.of(pgInt4, otherInt4));

    assertThat(lookup.findByName("pg_catalog", "int4")).contains(pgInt4);
    assertThat(lookup.findByName("other", "int4")).contains(otherInt4);
  }

  @Test
  void constructor_throwsWhenNamespaceNameMapsToMultipleIds() {
    NamespaceNode first = namespaceWithId("pg_catalog", "ns-1");
    NamespaceNode second = namespaceWithId("pg_catalog", "ns-2");

    assertThatThrownBy(() -> lookup(List.of(first, second), List.of()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Duplicate namespace name in lookup scope");
  }

  private static SystemTypeLookup lookup(List<NamespaceNode> namespaces, List<TypeNode> types) {
    TestCatalogOverlay overlay = new TestCatalogOverlay();
    namespaces.forEach(overlay::addNode);
    types.forEach(overlay::addNode);
    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            overlay, NameRef.getDefaultInstance(), catalogId(), EngineContext.empty());
    MetadataResolutionContext mrc =
        MetadataResolutionContext.of(ctx.graph(), ctx.catalogId(), ctx.engineContext());
    return new SystemTypeLookup(mrc);
  }

  private static NamespaceNode namespace(String name) {
    return new NamespaceNode(
        namespaceId(name),
        1,
        Instant.EPOCH,
        catalogId(),
        List.of(),
        name,
        GraphNodeOrigin.SYSTEM,
        Map.of(),
        Map.of());
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

  private static TypeNode type(String qualifiedTypeName, ResourceId namespaceId) {
    return new TypeNode(
        typeId(qualifiedTypeName),
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

  private static ResourceId catalogId() {
    return SystemNodeRegistry.systemCatalogContainerId("floedb");
  }

  private static ResourceId namespaceId(String qualifiedNamespaceName) {
    return SystemNodeRegistry.resourceId(
        "floedb", ResourceKind.RK_NAMESPACE, asNameRef(qualifiedNamespaceName));
  }

  private static ResourceId typeId(String qualifiedTypeName) {
    return SystemNodeRegistry.resourceId("floedb", ResourceKind.RK_TYPE, asNameRef(qualifiedTypeName));
  }

  private static NameRef asNameRef(String qualified) {
    if (qualified == null || qualified.isBlank()) {
      return NameRef.getDefaultInstance();
    }
    String[] parts = qualified.split("\\.");
    return NameRefUtil.name(parts);
  }
}
