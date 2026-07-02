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

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import java.util.Map;

/** Overlay visibility and mutability policy for table-like RPCs. */
public final class CatalogSurfaceTablePolicy {

  private final CatalogOverlay overlay;

  public CatalogSurfaceTablePolicy(CatalogOverlay overlay) {
    this.overlay = overlay;
  }

  public TableNode requireVisibleTable(ResourceId tableId, String corr) {
    if (tableId == null) {
      throw GrpcErrors.notFound(corr, TABLE, Map.of("id", "<missing_table_id>"));
    }
    CatalogSurfaceSupport.ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", corr);
    return overlay
        .resolve(tableId)
        .filter(TableNode.class::isInstance)
        .map(TableNode.class::cast)
        .orElseThrow(() -> GrpcErrors.notFound(corr, TABLE, Map.of("id", tableId.getId())));
  }

  public TableNode requireWritableTable(ResourceId tableId, String corr) {
    TableNode node = requireVisibleTable(tableId, corr);
    enforceWritableTableNode(node, tableId, corr);
    return node;
  }

  public void requireWritableTableForDelete(ResourceId tableId, String corr, boolean callerCares) {
    GraphNode node = resolveTableNode(tableId, corr, callerCares);

    if (node == null) {
      return;
    }

    enforceWritableTableNode(node, tableId, corr);
  }

  private GraphNode resolveTableNode(ResourceId tableId, String corr, boolean throwOnError) {
    if (throwOnError) {
      return requireVisibleTable(tableId, corr);
    }
    if (tableId == null) {
      throw GrpcErrors.notFound(corr, TABLE, Map.of("id", "<missing_table_id>"));
    }
    CatalogSurfaceSupport.ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", corr);

    try {
      return overlay.resolve(tableId).orElse(null);
    } catch (RuntimeException e) {
      return null;
    }
  }

  private void enforceWritableTableNode(GraphNode node, ResourceId tableId, String corr) {
    if (node instanceof UserTableNode) {
      return;
    }

    if (node != null && node.origin() == GraphNodeOrigin.SYSTEM) {
      throw GrpcErrors.permissionDenied(
          corr, SYSTEM_OBJECT_IMMUTABLE, Map.of("id", tableId.getId(), "kind", "table"));
    }

    throw GrpcErrors.notFound(corr, TABLE, Map.of("id", tableId.getId()));
  }
}
