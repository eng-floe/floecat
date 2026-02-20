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

package ai.floedb.floecat.systemcatalog.spi.decorator;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.scanner.spi.MetadataResolutionContext;
import java.util.Objects;

/** Mutable holder describing a namespace (catalog/schema) during bundle decoration. */
public final class NamespaceDecoration extends AbstractDecoration {

  private final NameRef name;
  private final ResourceId namespaceId; // nullable if you don't have stable namespace ids
  private final GraphNode node; // optional

  public NamespaceDecoration(
      NameRef name,
      ResourceId namespaceId,
      GraphNode node,
      MetadataResolutionContext resolutionContext) {
    super(resolutionContext);
    this.name = Objects.requireNonNull(name, "name");
    this.namespaceId = namespaceId;
    this.node = node;
  }

  public NameRef name() {
    return name;
  }

  public ResourceId namespaceId() {
    return namespaceId;
  }

  public GraphNode node() {
    return node;
  }
}
