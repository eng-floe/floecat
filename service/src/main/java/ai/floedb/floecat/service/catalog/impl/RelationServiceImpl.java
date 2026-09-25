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

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.KIND;

import ai.floedb.floecat.catalog.rpc.CurrentSnapshotPointer;
import ai.floedb.floecat.catalog.rpc.GetRelationRequest;
import ai.floedb.floecat.catalog.rpc.GetRelationResponse;
import ai.floedb.floecat.catalog.rpc.ListRelationsRequest;
import ai.floedb.floecat.catalog.rpc.ListRelationsResponse;
import ai.floedb.floecat.catalog.rpc.RelationService;
import ai.floedb.floecat.catalog.rpc.ResolveRelationsRequest;
import ai.floedb.floecat.catalog.rpc.ResolveRelationsResponse;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.service.catalog.impl.surface.CatalogSurfaceRelations;
import ai.floedb.floecat.service.catalog.impl.surface.RelationScope;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@GrpcService
public class RelationServiceImpl extends BaseServiceImpl implements RelationService {

  @Inject TableRepository tableRepo;
  @Inject ViewRepository viewRepo;
  @Inject SnapshotRepository snapshotRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject CatalogGraphView graphView;

  @ConfigProperty(name = "floecat.relation.resolve.max-names", defaultValue = "1000")
  int maxResolveNames;

  @ConfigProperty(name = "floecat.relation.list.max-page-size", defaultValue = "1000")
  int maxListPageSize;

  private static final Logger LOG = Logger.getLogger(RelationService.class);

  private CatalogSurfaceRelations catalogSurfaceRelations() {
    return new CatalogSurfaceRelations(
        tableRepo,
        viewRepo,
        tableId ->
            snapshotRepo
                .getCommittedCurrentSnapshotPointer(tableId)
                .map(CurrentSnapshotPointer::getSnapshotId),
        graphView,
        catalogContext(),
        maxListPageSize);
  }

  @Override
  public Uni<ListRelationsResponse> listRelations(ListRelationsRequest request) {
    var L = LogHelper.start(LOG, "ListRelations");

    return mapFailures(
            run(
                () -> {
                  var pc = principal.get();
                  requireListRead(pc, request);

                  return catalogSurfaceRelations()
                      .listRelations(request, pc.getAccountId(), pc.getCorrelationId());
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<ResolveRelationsResponse> resolveRelations(ResolveRelationsRequest request) {
    var L = LogHelper.start(LOG, "ResolveRelations");

    return mapFailures(
            run(
                () -> {
                  var pc = principal.get();
                  // ResolveRelations is kind-neutral, so it needs read on every kind it can return.
                  requireReadForKinds(pc, RelationScope.KIND_ORDER);

                  return catalogSurfaceRelations()
                      .resolveRelations(request, maxResolveNames, pc.getCorrelationId());
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<GetRelationResponse> getRelation(GetRelationRequest request) {
    var L = LogHelper.start(LOG, "GetRelation");

    return mapFailures(
            run(
                () -> {
                  var pc = principal.get();
                  requireGetRead(pc, request);

                  return catalogSurfaceRelations().getRelation(request, pc.getCorrelationId());
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private void requireListRead(PrincipalContext pc, ListRelationsRequest request) {
    requireReadForKinds(
        pc, RelationScope.requestedKinds(request.getKindsList(), pc.getCorrelationId()));
  }

  private void requireGetRead(PrincipalContext pc, GetRelationRequest request) {
    requireReadForKinds(pc, List.of(request.getRelationId().getKind()));
  }

  private void requireReadForKinds(PrincipalContext pc, List<ResourceKind> kinds) {
    for (ResourceKind kind : kinds) {
      switch (kind) {
        case RK_TABLE -> authz.require(pc, "table.read");
        case RK_VIEW -> authz.require(pc, "view.read");
        default ->
            throw GrpcErrors.invalidArgument(pc.getCorrelationId(), KIND, Map.of("field", "kinds"));
      }
    }
  }
}
