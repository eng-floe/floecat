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

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import java.time.Instant;
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
    overlay.addNode(type("pg_catalog.text"));

    EngineTypeMapper mapper = (logicalType, lookup) -> Optional.empty();
    TypeResolver resolver = new TypeResolver(ctx, mapper);

    LogicalType stringType = LogicalType.of(LogicalKind.STRING);
    assertThat(resolver.resolve(stringType)).isEmpty();
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
        displayName,
        "U",
        false,
        ResourceId.getDefaultInstance(),
        Map.of());
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
}
