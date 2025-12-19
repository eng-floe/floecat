package ai.floedb.floecat.systemcatalog.spi.types;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
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
          new TestCatalogOverlay(), NameRef.getDefaultInstance(), catalogId());

  @Test
  void resolve_returnsMappedTypeNode() {
    TypeNode type = type("pg_catalog.int4");
    TestCatalogOverlay overlay = (TestCatalogOverlay) ctx.graph();
    overlay.addNode(type);

    AtomicInteger invocations = new AtomicInteger();
    EngineTypeMapper mapper =
        (logicalType, lookup) -> {
          invocations.incrementAndGet();
          if (logicalType.kind() == LogicalKind.INT32) {
            return lookup.findByName("pg_catalog", "int4");
          }
          return Optional.empty();
        };

    TypeResolver resolver = new TypeResolver(ctx, mapper);

    LogicalType logical = LogicalType.of(LogicalKind.INT32);
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
    return ResourceId.newBuilder()
        .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
        .setKind(ResourceKind.RK_CATALOG)
        .setId("pg")
        .build();
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
    return ResourceId.newBuilder()
        .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
        .setKind(ResourceKind.RK_TYPE)
        .setId("test:" + displayName)
        .build();
  }
}
