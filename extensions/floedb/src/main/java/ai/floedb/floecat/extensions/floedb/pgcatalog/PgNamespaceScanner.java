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

import ai.floedb.floecat.extensions.floedb.hints.FloeHintResolver;
import ai.floedb.floecat.extensions.floedb.proto.FloeNamespaceSpecific;
import ai.floedb.floecat.extensions.floedb.utils.ScannerUtils;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import java.util.List;
import java.util.stream.Stream;

/** pg_catalog.pg_namespace */
public final class PgNamespaceScanner implements SystemObjectScanner {

  public static final List<SchemaColumn> SCHEMA =
      List.of(
          ScannerUtils.col("oid", "INT"),
          ScannerUtils.col("nspname", "VARCHAR"),
          ScannerUtils.col("nspowner", "INT"),
          ScannerUtils.col("nspacl", "VARCHAR[]"));

  @Override
  public List<SchemaColumn> schema() {
    return SCHEMA;
  }

  @Override
  public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
    return ctx.listNamespaces().stream().map(ns -> toRow(ctx, ns));
  }

  private SystemObjectRow toRow(SystemObjectScanContext ctx, NamespaceNode ns) {
    FloeNamespaceSpecific spec = FloeHintResolver.namespaceSpecific(ctx, ns);
    String[] acl =
        spec.getNspaclCount() > 0 ? spec.getNspaclList().toArray(String[]::new) : new String[0];
    return new SystemObjectRow(
        new Object[] {spec.getOid(), spec.getNspname(), spec.getNspowner(), acl});
  }
}
