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

package ai.floedb.floecat.service.directory;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.catalog.rpc.DirectoryService;
import ai.floedb.floecat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.floecat.catalog.rpc.LookupCatalogResponse;
import ai.floedb.floecat.catalog.rpc.LookupNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.LookupNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Map;
import org.jboss.logging.Logger;

@GrpcService
public class DirectoryServiceImpl extends BaseServiceImpl implements DirectoryService {
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject CatalogGraphView graphView;

  private static final Logger LOG = Logger.getLogger(DirectoryService.class);

  @Override
  public Uni<ResolveCatalogResponse> resolveCatalog(ResolveCatalogRequest request) {
    var L = LogHelper.start(LOG, "ResolveCatalog");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();

                  authz.require(principalContext, "catalog.read");

                  ResourceId resourceId =
                      graphView
                          .resolveCatalog(
                              correlationId(), request.getRef().getCatalog(), catalogContext())
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      correlationId(),
                                      CATALOG,
                                      Map.of("id", request.getRef().getCatalog())));

                  return ResolveCatalogResponse.newBuilder().setResourceId(resourceId).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<LookupCatalogResponse> lookupCatalog(LookupCatalogRequest request) {
    var L = LogHelper.start(LOG, "LookupCatalog");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();

                  authz.require(principalContext, "catalog.read");

                  var catalogNode = graphView.catalog(request.getResourceId(), catalogContext());
                  if (catalogNode.isEmpty()) {
                    return LookupCatalogResponse.newBuilder().build();
                  }

                  return LookupCatalogResponse.newBuilder()
                      .setDisplayName(catalogNode.get().displayName())
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<ResolveNamespaceResponse> resolveNamespace(ResolveNamespaceRequest request) {
    var L = LogHelper.start(LOG, "ResolveNamespace");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();

                  authz.require(principalContext, "catalog.read");

                  var ref = request.getRef();
                  validateNameRefOrThrow(ref);

                  var namespacePath = String.join(".", NameRefUtil.namespacePath(ref));
                  ResourceId namespaceId =
                      graphView
                          .resolveNamespace(correlationId(), ref, catalogContext())
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      correlationId(),
                                      NAMESPACE_BY_PATH_MISSING,
                                      Map.of("catalog", ref.getCatalog(), "path", namespacePath)));

                  return ResolveNamespaceResponse.newBuilder().setResourceId(namespaceId).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<LookupNamespaceResponse> lookupNamespace(LookupNamespaceRequest request) {
    var L = LogHelper.start(LOG, "LookupNamespace");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();

                  authz.require(principalContext, "catalog.read");

                  var namespaceName =
                      graphView.namespaceName(request.getResourceId(), catalogContext());
                  if (namespaceName == null || namespaceName.isEmpty()) {
                    return LookupNamespaceResponse.newBuilder().build();
                  }

                  return LookupNamespaceResponse.newBuilder().setRef(namespaceName.get()).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private void validateNameRefOrThrow(NameRef ref) {
    if (ref.getCatalog() == null || ref.getCatalog().isBlank()) {
      throw GrpcErrors.invalidArgument(correlationId(), CATALOG_MISSING, Map.of());
    }
    for (String pathSegment : ref.getPathList()) {
      if (pathSegment == null || pathSegment.isBlank()) {
        throw GrpcErrors.invalidArgument(correlationId(), PATH_SEGMENT_BLANK, Map.of());
      }
    }
  }
}
