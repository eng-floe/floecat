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
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import java.util.List;

final class CatalogSurfaceTablePageSource
    implements CatalogSurfaceRelationPager.Source<Table, TableNode> {

  static final String TOKEN_PREFIX = "tbl:";

  private final TableRepository repo;
  private final CatalogOverlay overlay;
  private final String accountId;
  private final NamespaceNode namespace;
  private final ResourceId namespaceId;
  private final ResourceId catalogId;

  CatalogSurfaceTablePageSource(
      TableRepository repo,
      CatalogOverlay overlay,
      String accountId,
      NamespaceNode namespace,
      ResourceId namespaceId) {
    this.repo = repo;
    this.overlay = overlay;
    this.accountId = accountId;
    this.namespace = namespace;
    this.namespaceId = namespaceId;
    this.catalogId = namespace.catalogId();
  }

  @Override
  public NamespaceNode namespace() {
    return namespace;
  }

  @Override
  public String tokenPrefix() {
    return TOKEN_PREFIX;
  }

  @Override
  public List<Table> listRepo(int limit, String cursor, StringBuilder next) {
    return repo.list(accountId, catalogId.getId(), namespaceId.getId(), limit, cursor, next);
  }

  @Override
  public int countRepo() {
    return repo.count(accountId, catalogId.getId(), namespaceId.getId());
  }

  @Override
  public List<TableNode> systemNodes() {
    return overlay.listSystemRelationsInNamespace(catalogId, namespaceId).stream()
        .filter(TableNode.class::isInstance)
        .map(TableNode.class::cast)
        .toList();
  }

  @Override
  public String systemRelativeKey(TableNode node) {
    if (node == null) {
      return "";
    }
    String name = node.displayName();
    if (name == null) {
      name = "";
    }
    return CatalogSurfaceSupport.normalizeName(name);
  }

  @Override
  public Table mapSystemNode(TableNode node) {
    return node.toTableProtoBuilder().setCatalogId(catalogId).build();
  }
}
