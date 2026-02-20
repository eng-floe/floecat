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
import ai.floedb.floecat.extensions.floedb.proto.FloeTypeSpecific;
import ai.floedb.floecat.extensions.floedb.utils.ScannerUtils;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import java.util.List;
import java.util.stream.Stream;

/**
 * pg_catalog.pg_type
 *
 * <p>PostgreSQL-compatible type metadata. Types are catalog-scoped; typnamespace is metadata only.
 */
public final class PgTypeScanner implements SystemObjectScanner {

  public static final List<SchemaColumn> SCHEMA =
      List.of(
          ScannerUtils.col("oid", "INT"),
          ScannerUtils.col("typname", "VARCHAR"),
          ScannerUtils.col("typnamespace", "INT"),
          ScannerUtils.col("typlen", "INT"),
          ScannerUtils.col("typbyval", "BOOLEAN"),
          ScannerUtils.col("typdelim", "CHAR"),
          ScannerUtils.col("typelem", "INT"),
          ScannerUtils.col("typarray", "INT"),
          ScannerUtils.col("typalign", "CHAR"));

  @Override
  public List<SchemaColumn> schema() {
    return SCHEMA;
  }

  @Override
  public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
    return ctx.listTypes().stream().map(type -> row(ctx, type));
  }

  // ----------------------------------------------------------------------
  // Row construction
  // ----------------------------------------------------------------------

  private static SystemObjectRow row(SystemObjectScanContext ctx, TypeNode type) {
    FloeTypeSpecific spec = FloeHintResolver.typeSpecific(ctx, type);
    return new SystemObjectRow(
        new Object[] {
          spec.getOid(),
          spec.getTypname(),
          spec.getTypnamespace(),
          spec.getTyplen(),
          spec.getTypbyval(),
          spec.getTypdelim(),
          spec.getTypelem(),
          spec.getTyparray(),
          spec.getTypalign()
        });
  }
}
