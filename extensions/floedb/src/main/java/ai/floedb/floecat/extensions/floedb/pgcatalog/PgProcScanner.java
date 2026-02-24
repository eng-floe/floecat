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

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.extensions.floedb.hints.FloeHintResolver;
import ai.floedb.floecat.extensions.floedb.proto.FloeFunctionSpecific;
import ai.floedb.floecat.extensions.floedb.utils.ScannerUtils;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import java.util.List;
import java.util.stream.Stream;

public final class PgProcScanner implements SystemObjectScanner {

  public static final List<SchemaColumn> SCHEMA =
      List.of(
          ScannerUtils.col("oid", "INT"),
          ScannerUtils.col("proname", "VARCHAR"),
          ScannerUtils.col("pronamespace", "INT"),
          ScannerUtils.col("prorettype", "INT"),
          ScannerUtils.col("proargtypes", "INT[]"),
          ScannerUtils.col("proisagg", "BOOLEAN"),
          ScannerUtils.col("proiswindow", "BOOLEAN"));

  @Override
  public List<SchemaColumn> schema() {
    return SCHEMA;
  }

  @Override
  public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
    return ctx.listNamespaces().stream()
        .flatMap(
            ns -> ctx.listFunctions(ns.id()).stream().map(fn -> new NamespaceFunction(ns.id(), fn)))
        .map(nf -> row(ctx, nf.namespaceId(), nf.function()));
  }

  // ----------------------------------------------------------------------
  // Row construction
  // ----------------------------------------------------------------------

  private static SystemObjectRow row(
      SystemObjectScanContext ctx, ResourceId namespaceId, FunctionNode fn) {

    FloeFunctionSpecific spec = FloeHintResolver.functionSpecific(ctx, namespaceId, fn);
    int[] argTypeOids = spec.getProargtypesList().stream().mapToInt(Integer::intValue).toArray();
    return new SystemObjectRow(
        new Object[] {
          spec.getOid(),
          fn.displayName(),
          spec.getPronamespace(),
          spec.getProrettype(),
          argTypeOids,
          fn.aggregate(),
          fn.window()
        });
  }

  private record NamespaceFunction(ResourceId namespaceId, FunctionNode function) {}
}
