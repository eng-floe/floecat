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

import static ai.floedb.floecat.extensions.floedb.utils.FloePayloads.TYPE;

import ai.floedb.floecat.extensions.floedb.proto.FloeTypeSpecific;
import ai.floedb.floecat.extensions.floedb.utils.ScannerUtils;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import java.util.List;
import java.util.Optional;
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
          ScannerUtils.col("typtype", "CHAR"),
          ScannerUtils.col("typcategory", "CHAR"),
          ScannerUtils.col("typowner", "INT"));

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

    Optional<FloeTypeSpecific> spec = ScannerUtils.payload(ctx, type.id(), TYPE);

    int oid =
        spec.map(FloeTypeSpecific::getOid)
            .filter(v -> v > 0)
            .orElseGet(() -> ScannerUtils.fallbackOid(type.id()));

    String typname = spec.map(FloeTypeSpecific::getTypname).orElse(type.displayName());

    int typnamespace =
        spec.map(FloeTypeSpecific::getTypnamespace)
            .filter(v -> v > 0)
            .orElse(PgCatalogProvider.PG_CATALOG_OID);

    int typlen = spec.map(FloeTypeSpecific::getTyplen).orElse(-1);

    boolean typbyval = spec.map(FloeTypeSpecific::getTypbyval).orElse(false);

    String typtype = spec.map(FloeTypeSpecific::getTyptype).orElse("b");

    String typcategory = spec.map(FloeTypeSpecific::getTypcategory).orElse("U");

    int typowner =
        spec.map(FloeTypeSpecific::getTypowner)
            .filter(v -> v > 0)
            .orElseGet(ScannerUtils::defaultOwnerOid);

    return new SystemObjectRow(
        new Object[] {
          oid, typname, typnamespace, typlen, typbyval, typtype, typcategory, typowner
        });
  }
}
