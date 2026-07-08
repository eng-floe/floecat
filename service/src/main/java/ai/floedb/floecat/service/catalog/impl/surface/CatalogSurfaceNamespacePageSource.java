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

import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import java.util.ArrayList;
import java.util.List;

final class CatalogSurfaceNamespacePageSource implements CatalogSurfaceNamespacePager.Source {

  private final NamespaceRepository repo;
  private final String accountId;
  private final ResourceId catalogId;
  private final List<String> parentPath;
  private final String namePrefix;
  private final boolean recursive;
  private final List<NamespaceNode> systemNamespaces;

  CatalogSurfaceNamespacePageSource(
      NamespaceRepository repo,
      String accountId,
      ResourceId catalogId,
      List<String> parentPath,
      String namePrefix,
      boolean recursive,
      List<NamespaceNode> systemNamespaces) {
    this.repo = repo;
    this.accountId = accountId;
    this.catalogId = catalogId;
    this.parentPath = parentPath;
    this.namePrefix = namePrefix;
    this.recursive = recursive;
    this.systemNamespaces = systemNamespaces;
  }

  @Override
  public List<String> parentPath() {
    return parentPath;
  }

  @Override
  public String namePrefix() {
    return namePrefix;
  }

  @Override
  public boolean recursive() {
    return recursive;
  }

  @Override
  public List<Namespace> listRepo(int limit, String cursor, StringBuilder next) {
    return repo.list(accountId, catalogId.getId(), parentPath, limit, cursor, next);
  }

  @Override
  public String cursorAfter(Namespace namespace) {
    var fullPath = new ArrayList<>(namespace.getParentsList());
    fullPath.add(namespace.getDisplayName());
    return repo.listTokenAfter(accountId, catalogId.getId(), fullPath);
  }

  @Override
  public List<NamespaceNode> systemNamespaces() {
    return systemNamespaces;
  }

  @Override
  public Namespace mapSystemNode(NamespaceNode namespace) {
    return CatalogSurfaceNamespaces.toProto(namespace);
  }
}
