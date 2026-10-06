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

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.ListTablesRequest;
import ai.floedb.floecat.catalog.rpc.ListTablesResponse;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.scanner.spi.CatalogGraphView.NamespaceRef;
import ai.floedb.floecat.scanner.utils.CatalogContext;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import java.util.Map;
import java.util.Objects;

/** Catalog Surface policy for table RPCs. */
public final class CatalogSurfaceTables {

  private final TableRepository tableRepo;
  private final CatalogGraphView graphView;
  private final CatalogSurfaceWritePolicy writePolicy;
  private final CatalogContext context;

  public CatalogSurfaceTables(
      TableRepository tableRepo, CatalogGraphView graphView, CatalogContext context) {
    this.tableRepo = Objects.requireNonNull(tableRepo, "table repository is required");
    this.graphView = graphView;
    this.writePolicy = new CatalogSurfaceWritePolicy(graphView, context);
    this.context = context;
  }

  public ListTablesResponse listTables(ListTablesRequest request, String accountId, String corr) {
    var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
    final int want = Math.max(1, pageIn.limit);

    var namespaceId = request.getNamespaceId();
    NamespaceNode nsNode = writePolicy.requireVisibleNamespace(namespaceId, corr);

    var source =
        new CatalogSurfaceTablePageSource(
            tableRepo, graphView, accountId, nsNode, namespaceId, writePolicy.context());
    var result = CatalogSurfaceRelationPager.listRefs(want, pageIn.token, source, corr);

    var tables = result.relations().stream().map(ref -> source.hydrate(ref, corr)).toList();
    var page = MutationOps.pageOut(result.nextToken(), CatalogSurfaceRelationPager.total(source));
    return ListTablesResponse.newBuilder().addAllTables(tables).setPage(page).build();
  }

  CatalogSurfaceTablePageSource pageSource(NamespaceRef namespace, String accountId) {
    return new CatalogSurfaceTablePageSource(tableRepo, graphView, accountId, namespace, context);
  }

  /** The visible table, without the wire envelope, for in-process callers. */
  public Table byId(ResourceId tableId, String corr) {
    TableNode node = writePolicy.requireVisibleTable(tableId, corr);
    return tableFromGraphNodeOrRepo(node, tableId, corr);
  }

  public GetTableResponse getTable(GetTableRequest request, String corr) {
    ResourceId tableId = request.getTableId();
    TableNode node = writePolicy.requireVisibleTable(tableId, corr);
    Table table = tableFromGraphNodeOrRepo(node, tableId, corr);
    MutationMeta meta =
        node.origin() == GraphNodeOrigin.SYSTEM
            ? MutationMeta.getDefaultInstance()
            : tableRepo.metaForSafe(tableId);

    return GetTableResponse.newBuilder().setTable(table).setMeta(meta).build();
  }

  private Table tableFromGraphNodeOrRepo(TableNode node, ResourceId tableId, String corr) {
    if (node.origin() == GraphNodeOrigin.SYSTEM) {
      return node.toTableProtoTable();
    }

    return tableRepo
        .getById(tableId)
        .orElseThrow(() -> GrpcErrors.notFound(corr, TABLE, Map.of("id", tableId.getId())));
  }
}
