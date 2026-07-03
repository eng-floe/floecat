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
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import java.util.Map;
import java.util.Objects;

/** Catalog Surface policy for table RPCs. */
public final class CatalogSurfaceTables {

  private final TableRepository tableRepo;
  private final CatalogOverlay overlay;
  private final CatalogSurfaceWritePolicy writePolicy;

  public CatalogSurfaceTables(TableRepository tableRepo, CatalogOverlay overlay) {
    this.tableRepo = Objects.requireNonNull(tableRepo, "table repository is required");
    this.overlay = overlay;
    this.writePolicy = new CatalogSurfaceWritePolicy(overlay);
  }

  public ListTablesResponse listTables(ListTablesRequest request, String accountId, String corr) {
    var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
    final int want = Math.max(1, pageIn.limit);

    var namespaceId = request.getNamespaceId();
    NamespaceNode nsNode = writePolicy.requireVisibleNamespace(namespaceId, corr);

    var result =
        CatalogSurfaceRelationPager.list(
            want,
            pageIn.token,
            new CatalogSurfaceTablePageSource(tableRepo, overlay, accountId, nsNode, namespaceId),
            corr);

    var page = MutationOps.pageOut(result.nextToken(), result.totalSize());
    return ListTablesResponse.newBuilder().addAllTables(result.items()).setPage(page).build();
  }

  public GetTableResponse getTable(GetTableRequest request, String corr) {
    TableNode node = writePolicy.requireVisibleTable(request.getTableId(), corr);
    Table table = tableFromOverlayNodeOrRepo(node, request.getTableId(), corr);
    MutationMeta meta =
        node.origin() == GraphNodeOrigin.SYSTEM
            ? MutationMeta.getDefaultInstance()
            : tableRepo.metaForSafe(request.getTableId());

    return GetTableResponse.newBuilder().setTable(table).setMeta(meta).build();
  }

  private Table tableFromOverlayNodeOrRepo(TableNode node, ResourceId tableId, String corr) {
    if (node.origin() == GraphNodeOrigin.SYSTEM) {
      return node.toTableProtoTable();
    }

    return tableRepo
        .getById(tableId)
        .orElseThrow(() -> GrpcErrors.notFound(corr, TABLE, Map.of("id", tableId.getId())));
  }
}
