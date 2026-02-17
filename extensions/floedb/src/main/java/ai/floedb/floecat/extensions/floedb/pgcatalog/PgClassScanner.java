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

package ai.floedb.floecat.extensions.floedb.pgcatalog;

import static ai.floedb.floecat.extensions.floedb.utils.ScannerUtils.col;

import ai.floedb.floecat.extensions.floedb.hints.FloeHintResolver;
import ai.floedb.floecat.extensions.floedb.proto.FloeRelationSpecific;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import java.util.List;
import java.util.stream.Stream;

public final class PgClassScanner implements SystemObjectScanner {

  public static final List<SchemaColumn> SCHEMA =
      List.of(
          col("oid", "INT"),
          col("relname", "VARCHAR"),
          col("relnamespace", "INT"),
          col("relkind", "CHAR"),
          col("reltuples", "FLOAT"));

  @Override
  public List<SchemaColumn> schema() {
    return SCHEMA;
  }

  @Override
  public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
    return ctx.listNamespaces().stream()
        .flatMap(ns -> ctx.listRelations(ns.id()).stream())
        .filter(this::supportedRelation)
        .map(node -> row(ctx, node));
  }

  private boolean supportedRelation(GraphNode node) {
    return node instanceof RelationNode;
  }

  private SystemObjectRow row(SystemObjectScanContext ctx, RelationNode node) {
    FloeRelationSpecific spec = FloeHintResolver.relationSpecific(ctx, node);
    return new SystemObjectRow(
        new Object[] {
          spec.getOid(),
          spec.getRelname(),
          spec.getRelnamespace(),
          spec.getRelkind(),
          (float) spec.getReltuples()
        });
  }
}
