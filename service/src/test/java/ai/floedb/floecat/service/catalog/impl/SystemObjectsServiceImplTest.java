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

package ai.floedb.floecat.service.catalog.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.query.rpc.GetSystemObjectsRequest;
import ai.floedb.floecat.query.rpc.GetSystemObjectsResponse;
import ai.floedb.floecat.query.rpc.SystemObjectsRegistry;
import ai.floedb.floecat.query.rpc.TableBackendKind;
import ai.floedb.floecat.scanner.utils.EngineCatalogNames;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.def.SystemViewDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.provider.FloecatInternalProvider;
import ai.floedb.floecat.systemcatalog.provider.StaticSystemCatalogProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemDefinitionRegistry;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import io.grpc.Context;
import io.grpc.Status;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SystemObjectsServiceImplTest {

  @Test
  void getSystemObjectsSanitizesRelations() {
    SystemCatalogData catalog = catalogWithRelations();
    SystemNodeRegistry.BuiltinNodes builtin =
        new SystemNodeRegistry.BuiltinNodes(
            "pg",
            "1.0",
            "fingerprint",
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of(),
            catalog);

    SystemNodeRegistry nodeRegistry =
        new SystemNodeRegistry(
            new SystemDefinitionRegistry(
                new StaticSystemCatalogProvider(
                    Map.of(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG, SystemCatalogData.empty()))),
            new FloecatInternalProvider(),
            List.of()) {

          @Override
          public BuiltinNodes nodesFor(EngineContext ctx) {
            return builtin;
          }
        };
    SystemObjectsServiceImpl service = createService(nodeRegistry);

    EngineContext ctx = EngineContext.of("pg", "1.0");
    PrincipalContext principal =
        PrincipalContext.newBuilder()
            .setAccountId("acct-1")
            .setSubject("tester")
            .addPermissions("system-objects.read")
            .build();
    Context context =
        Context.current()
            .withValue(InboundContextInterceptor.ENGINE_CONTEXT_KEY, ctx)
            .withValue(PrincipalProvider.KEY, principal);
    Context previous = context.attach();
    try {
      GetSystemObjectsResponse response =
          service
              .getSystemObjects(GetSystemObjectsRequest.getDefaultInstance())
              .await()
              .indefinitely();
      SystemObjectsRegistry registry = response.getRegistry();
      assertThat(registry.getSystemNamespacesCount()).isZero();
      assertThat(registry.getSystemTablesCount()).isZero();
      assertThat(registry.getSystemViewsCount()).isZero();
      assertThat(registry.getEngineSpecificList()).hasSize(1);
    } finally {
      context.detach(previous);
    }
  }

  @Test
  void getSystemObjectsRequiresSystemObjectsReadPermission() {
    SystemObjectsServiceImpl service =
        createService(
            new SystemNodeRegistry(
                new SystemDefinitionRegistry(
                    new StaticSystemCatalogProvider(
                        Map.of(
                            EngineCatalogNames.FLOECAT_DEFAULT_CATALOG,
                            SystemCatalogData.empty()))),
                new FloecatInternalProvider(),
                List.of()));

    EngineContext ctx = EngineContext.of("pg", "1.0");
    PrincipalContext principal =
        PrincipalContext.newBuilder().setAccountId("acct-1").setSubject("tester").build();
    Context context =
        Context.current()
            .withValue(InboundContextInterceptor.ENGINE_CONTEXT_KEY, ctx)
            .withValue(PrincipalProvider.KEY, principal);
    Context previous = context.attach();
    try {
      assertThatThrownBy(
              () ->
                  service
                      .getSystemObjects(GetSystemObjectsRequest.getDefaultInstance())
                      .await()
                      .indefinitely())
          .isInstanceOf(io.grpc.StatusRuntimeException.class)
          .extracting(ex -> ((io.grpc.StatusRuntimeException) ex).getStatus().getCode())
          .isEqualTo(Status.Code.PERMISSION_DENIED);
    } finally {
      context.detach(previous);
    }
  }

  private static SystemObjectsServiceImpl createService(SystemNodeRegistry nodeRegistry) {
    SystemObjectsServiceImpl service = new SystemObjectsServiceImpl();
    service.principal = new PrincipalProvider();
    service.authz = new Authorizer();
    service.nodeRegistry = nodeRegistry;
    service.engineContextProvider = new EngineContextProvider();
    return service;
  }

  private static SystemCatalogData catalogWithRelations() {
    SystemNamespaceDef namespace =
        new SystemNamespaceDef(NameRefUtil.name("sanitized"), "sanitized", List.of());
    SystemTableDef table =
        new SystemTableDef(
            NameRefUtil.name("sanitized", "table"),
            "table",
            List.of(),
            TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
            "scanner",
            "",
            List.of(),
            null);
    SystemViewDef view =
        new SystemViewDef(
            NameRefUtil.name("sanitized", "view"), "view", "select 1", "", List.of(), List.of());

    EngineSpecificRule registryHint =
        new EngineSpecificRule("pg", "", "", "registry", new byte[] {1}, Map.of("mode", "test"));

    return new SystemCatalogData(
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(namespace),
        List.of(table),
        List.of(view),
        List.of(registryHint));
  }
}
