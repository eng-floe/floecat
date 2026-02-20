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
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.spi.MetadataResolutionContext;
import java.util.List;
import java.util.Objects;

/** Mutable holder describing a relation during bundle decoration. */
public final class RelationDecoration extends AbstractDecoration {

  private final RelationInfo.Builder builder;
  private final ResourceId relationId;
  private final RelationNode node;
  private final List<SchemaColumn> schema;
  private final List<SchemaColumn> emitted;

  public RelationDecoration(
      RelationInfo.Builder builder,
      ResourceId relationId,
      RelationNode node,
      List<SchemaColumn> schema,
      List<SchemaColumn> emitted,
      MetadataResolutionContext resolutionContext) {
    super(resolutionContext);
    this.builder = Objects.requireNonNull(builder, "builder");
    this.relationId = Objects.requireNonNull(relationId, "relationId");
    this.node = node;
    this.schema = List.copyOf(schema);
    this.emitted = List.copyOf(emitted);
  }

  public RelationInfo.Builder builder() {
    return builder;
  }

  public ResourceId relationId() {
    return relationId;
  }

  public RelationNode node() {
    return node;
  }

  public List<SchemaColumn> schema() {
    return schema;
  }

  public List<SchemaColumn> emitted() {
    return emitted;
  }
}
