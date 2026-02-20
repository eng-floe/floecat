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

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.ViewDefinition;
import ai.floedb.floecat.scanner.spi.MetadataResolutionContext;
import java.util.Objects;

/**
 * Mutable holder describing a view during bundle decoration.
 *
 * <p>Views have two independent sinks: - RelationInfo.engine_specific -
 * ViewDefinition.engine_specific
 */
public final class ViewDecoration extends AbstractDecoration {

  private final RelationInfo.Builder relationBuilder;
  private final ViewDefinition.Builder viewBuilder;
  private final ResourceId viewId;
  private final GraphNode node;

  public ViewDecoration(
      RelationInfo.Builder relationBuilder,
      ViewDefinition.Builder viewBuilder,
      ResourceId viewId,
      GraphNode node,
      MetadataResolutionContext resolutionContext) {
    super(resolutionContext);
    this.relationBuilder = Objects.requireNonNull(relationBuilder, "relationBuilder");
    this.viewBuilder = Objects.requireNonNull(viewBuilder, "viewBuilder");
    this.viewId = Objects.requireNonNull(viewId, "viewId");
    this.node = node;
  }

  /** Relation sink (RelationInfo.engine_specific). */
  public RelationInfo.Builder relationBuilder() {
    return relationBuilder;
  }

  /** View-definition sink (ViewDefinition.engine_specific). */
  public ViewDefinition.Builder viewBuilder() {
    return viewBuilder;
  }

  public ResourceId viewId() {
    return viewId;
  }

  public GraphNode node() {
    return node;
  }
}
