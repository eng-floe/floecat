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
package ai.floedb.floecat.service.catalog.impl.surface;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.scanner.utils.CatalogContext;
import ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.systemcatalog.graph.SystemResourceIdGenerator;
import java.util.List;
import java.util.Map;

final class CatalogSurfaceTablePageSource implements CatalogSurfaceRelationPager.RefSource {

  static final String TOKEN_PREFIX = "tbl:";

  private final TableRepository repo;
  private final CatalogGraphView graphView;
  private final String accountId;
  private final ResourceId namespaceId;
  private final ResourceId catalogId;
  private final boolean userNamespace;
  private final CatalogContext context;

  CatalogSurfaceTablePageSource(
      TableRepository repo,
      CatalogGraphView graphView,
      String accountId,
      NamespaceNode namespace,
      ResourceId namespaceId,
      CatalogContext context) {
    this.repo = repo;
    this.graphView = graphView;
    this.accountId = accountId;
    this.namespaceId = namespaceId;
    this.catalogId = namespace.catalogId();
    this.userNamespace = namespace.origin() != GraphNodeOrigin.SYSTEM;
    this.context = context;
  }

  CatalogSurfaceTablePageSource(
      TableRepository repo,
      CatalogGraphView graphView,
      String accountId,
      CatalogGraphView.NamespaceRef namespace,
      CatalogContext context) {
    this.repo = repo;
    this.graphView = graphView;
    this.accountId = accountId;
    this.namespaceId = namespace.id();
    this.catalogId = namespace.catalogId();
    this.userNamespace = !SystemResourceIdGenerator.isSystemId(namespace.id());
    this.context = context;
  }

  @Override
  public String tokenPrefix() {
    return TOKEN_PREFIX;
  }

  @Override
  public boolean hasUserRelations() {
    return userNamespace;
  }

  @Override
  public List<CatalogGraphView.RelationRef> listUserRelations(
      int limit, String cursor, StringBuilder next) {
    return repo.listRefs(accountId, catalogId.getId(), namespaceId.getId(), limit, cursor, next);
  }

  @Override
  public int countUserRelations() {
    return repo.count(accountId, catalogId.getId(), namespaceId.getId());
  }

  @Override
  public List<CatalogGraphView.RelationRef> systemRelations() {
    return graphView.listSystemRelationsInNamespace(catalogId, namespaceId, context).stream()
        .filter(TableNode.class::isInstance)
        .map(TableNode.class::cast)
        .map(
            node ->
                new CatalogGraphView.RelationRef(
                    node.id(), node.displayName(), ResourceKind.RK_TABLE))
        .toList();
  }

  Table hydrate(CatalogGraphView.RelationRef ref, String corr) {
    var resolved = graphView.resolve(ref.id(), context);
    if (SystemResourceIdGenerator.isSystemId(ref.id())
        || resolved.map(node -> node.origin() == GraphNodeOrigin.SYSTEM).orElse(false)) {
      return resolved
          .filter(TableNode.class::isInstance)
          .map(TableNode.class::cast)
          .map(node -> node.toTableProtoBuilder().setCatalogId(catalogId).build())
          .orElseThrow(
              () -> GrpcErrors.notFound(corr, MessageKey.TABLE, Map.of("id", ref.id().getId())));
    }
    return repo.getById(ref.id())
        .orElseThrow(
            () -> GrpcErrors.notFound(corr, MessageKey.TABLE, Map.of("id", ref.id().getId())));
  }
}
