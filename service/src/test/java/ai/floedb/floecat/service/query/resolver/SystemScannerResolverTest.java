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

package ai.floedb.floecat.service.query.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import ai.floedb.floecat.scanner.utils.EngineCatalogNames;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.provider.SystemObjectScannerProvider;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import io.grpc.Context;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class SystemScannerResolverTest {

  private static final String ACCOUNT_ID = "acct";
  private static final ResourceId NAMESPACE_ID =
      ResourceId.newBuilder()
          .setAccountId(ACCOUNT_ID)
          .setKind(ResourceKind.RK_NAMESPACE)
          .setId("pg_catalog")
          .build();
  private static final EngineContext ENGINE_CTX = EngineContext.of("pg", "1.0");

  @Test
  void resolvesEngineTableEvenWhenFallbackExists() {
    ResourceId pgId = systemTableId("pg", "foo");
    ResourceId fallbackId = systemTableId(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG, "foo");
    SystemTableNode.FloeCatSystemTableNode pgNode = tableNode(pgId, "pg-scanner");
    SystemTableNode.FloeCatSystemTableNode fallbackNode = tableNode(fallbackId, "internal-scanner");

    SystemObjectScanner pgScanner = new TestSystemObjectScanner("pg-scanner");
    SystemObjectScanner internalScanner = new TestSystemObjectScanner("internal-scanner");
    SystemScannerResolver resolver =
        buildResolver(
            new TestCatalogOverlay().addNode(pgNode).addNode(fallbackNode),
            Map.of(
                "pg-scanner", pgScanner,
                "internal-scanner", internalScanner));

    assertThat(withEngineContext(ENGINE_CTX, () -> resolver.resolve("corr", pgId)))
        .isSameAs(pgScanner);
  }

  @Test
  void fallsBackWhenEngineTableMissing() {
    ResourceId pgId = systemTableId("pg", "missing");
    ResourceId fallbackId = systemTableId(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG, "missing");
    SystemTableNode.FloeCatSystemTableNode fallbackNode = tableNode(fallbackId, "internal-scanner");

    SystemObjectScanner internalScanner = new TestSystemObjectScanner("internal-scanner");
    SystemScannerResolver resolver =
        buildResolver(
            new TestCatalogOverlay().addNode(fallbackNode),
            Map.of("internal-scanner", internalScanner));

    assertThat(withEngineContext(ENGINE_CTX, () -> resolver.resolve("corr", pgId)))
        .isSameAs(internalScanner);
  }

  private static SystemScannerResolver buildResolver(
      CatalogOverlay overlay, Map<String, SystemObjectScanner> scanners) {
    SystemScannerResolver resolver = new SystemScannerResolver();
    resolver.graph = overlay;
    resolver.engine = new EngineContextProvider();
    resolver.providers = List.of(new TestScannerProvider(scanners));
    return resolver;
  }

  private static <T> T withEngineContext(EngineContext ctx, Supplier<T> action) {
    Context context =
        Context.current().withValue(InboundContextInterceptor.ENGINE_CONTEXT_KEY, ctx);
    Context previous = context.attach();
    try {
      return action.get();
    } finally {
      context.detach(previous);
    }
  }

  private static ResourceId systemTableId(String engineKind, String... segments) {
    return SystemNodeRegistry.resourceId(
        engineKind, ResourceKind.RK_TABLE, NameRefUtil.name(segments));
  }

  private static SystemTableNode.FloeCatSystemTableNode tableNode(ResourceId id, String scannerId) {
    return new SystemTableNode.FloeCatSystemTableNode(
        id,
        1,
        Instant.EPOCH,
        "1.0",
        id.getId(),
        NAMESPACE_ID,
        List.of(),
        Map.of(),
        Map.of(),
        scannerId);
  }

  private static final class TestScannerProvider implements SystemObjectScannerProvider {

    private final Map<String, SystemObjectScanner> scanners;

    private TestScannerProvider(Map<String, SystemObjectScanner> scanners) {
      this.scanners = Map.copyOf(scanners);
    }

    @Override
    public List<SystemObjectDef> definitions() {
      return List.of();
    }

    @Override
    public boolean supportsEngine(String engineKind) {
      return true;
    }

    @Override
    public boolean supports(NameRef name, String engineKind) {
      return true;
    }

    @Override
    public Optional<SystemObjectScanner> provide(
        String scannerId, String engineKind, String engineVersion) {
      return Optional.ofNullable(scanners.get(scannerId));
    }
  }

  private static final class TestSystemObjectScanner implements SystemObjectScanner {

    private final String id;

    private TestSystemObjectScanner(String id) {
      this.id = id;
    }

    @Override
    public List<SchemaColumn> schema() {
      return List.of();
    }

    @Override
    public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
      return Stream.empty();
    }

    @Override
    public String toString() {
      return "test-scanner-" + id;
    }
  }
}
