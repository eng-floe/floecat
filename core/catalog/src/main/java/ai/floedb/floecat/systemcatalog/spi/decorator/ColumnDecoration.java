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

import ai.floedb.floecat.query.rpc.ColumnInfo;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.types.LogicalType;

/** Mutable view over a column while decorators add engine metadata. */
public final class ColumnDecoration extends AbstractDecoration {

  private final ColumnInfo.Builder builder;
  private final SchemaColumn schemaColumn;
  private final LogicalType logicalType;
  private final int ordinal;
  private final RelationDecoration relation;

  public ColumnDecoration(
      ColumnInfo.Builder builder,
      SchemaColumn schemaColumn,
      LogicalType logicalType,
      int ordinal,
      RelationDecoration relation) {
    super(relation.resolutionContext());
    this.builder = builder;
    this.schemaColumn = schemaColumn;
    this.logicalType = logicalType;
    this.ordinal = ordinal;
    this.relation = relation;
  }

  public ColumnInfo.Builder builder() {
    return builder;
  }

  public SchemaColumn schemaColumn() {
    return schemaColumn;
  }

  public LogicalType logicalType() {
    return logicalType;
  }

  public int ordinal() {
    return ordinal;
  }

  public RelationDecoration relation() {
    return relation;
  }
}
