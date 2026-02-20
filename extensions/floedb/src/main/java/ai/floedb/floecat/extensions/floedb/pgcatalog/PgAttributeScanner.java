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

import static ai.floedb.floecat.extensions.floedb.utils.FloePayloads.Descriptor.*;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.extensions.floedb.hints.FloeHintResolver;
import ai.floedb.floecat.extensions.floedb.proto.FloeColumnSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeRelationSpecific;
import ai.floedb.floecat.extensions.floedb.utils.ScannerUtils;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import ai.floedb.floecat.systemcatalog.spi.types.EngineTypeMapper;
import ai.floedb.floecat.systemcatalog.spi.types.TypeResolver;
import ai.floedb.floecat.types.LogicalType;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * pg_catalog.pg_attribute
 *
 * <p>PostgreSQL-compatible column catalog.
 */
public final class PgAttributeScanner implements SystemObjectScanner {

  private final EngineTypeMapper typeMapper;

  public PgAttributeScanner(EngineTypeMapper typeMapper) {
    this.typeMapper = typeMapper == null ? EngineTypeMapper.EMPTY : typeMapper;
  }

  // ----------------------------------------------------------------------
  // Schema
  // ----------------------------------------------------------------------

  public static final List<SchemaColumn> SCHEMA =
      List.of(
          ScannerUtils.col("attrelid", "INT"),
          ScannerUtils.col("attname", "VARCHAR"),
          ScannerUtils.col("atttypid", "INT"),
          ScannerUtils.col("atttypmod", "INT"),
          ScannerUtils.col("attnum", "INT"),
          ScannerUtils.col("attlen", "INT"),
          ScannerUtils.col("attbyval", "BOOLEAN"),
          ScannerUtils.col("attnotnull", "BOOLEAN"),
          ScannerUtils.col("attisdropped", "BOOLEAN"),
          ScannerUtils.col("attalign", "CHAR"),
          ScannerUtils.col("attstorage", "CHAR"),
          ScannerUtils.col("attndims", "INT"),
          ScannerUtils.col("attcollation", "INT"));

  @Override
  public List<SchemaColumn> schema() {
    return SCHEMA;
  }

  // ----------------------------------------------------------------------
  // Scan
  // ----------------------------------------------------------------------

  @Override
  public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {

    TypeResolver resolver = new TypeResolver(ctx, typeMapper);

    // Enumerate all relations (tables + views)
    return ctx.listNamespaces().stream()
        .flatMap(ns -> ctx.listRelations(ns.id()).stream())
        .filter(TableNode.class::isInstance)
        .map(TableNode.class::cast)
        .flatMap(table -> scanTable(ctx, resolver, table));
  }

  // ----------------------------------------------------------------------
  // Table scan
  // ----------------------------------------------------------------------

  private Stream<SystemObjectRow> scanTable(
      SystemObjectScanContext ctx, TypeResolver resolver, TableNode table) {

    // Resolve table OID (stable, deterministic)
    int relOid =
        ScannerUtils.oid(
            ctx, table.id(), RELATION, FloeRelationSpecific.class, FloeRelationSpecific::getOid);

    // Get logical schema (already normalized by LogicalSchemaMapper)
    List<SchemaColumn> columns = ctx.graph().tableSchema(table.id());

    AtomicInteger attnum = new AtomicInteger(1);

    return columns.stream()
        .map(col -> row(ctx, resolver, table.id(), relOid, attnum.getAndIncrement(), col));
  }

  // ----------------------------------------------------------------------
  // Row construction
  // ----------------------------------------------------------------------

  private SystemObjectRow row(
      SystemObjectScanContext ctx,
      TypeResolver resolver,
      ResourceId tableId,
      int relOid,
      int attnum,
      SchemaColumn column) {
    LogicalType logicalType = FloeHintResolver.parseLogicalType(column);
    FloeHintResolver.ColumnMetadata metadata =
        FloeHintResolver.columnMetadata(ctx, resolver, column, logicalType);
    FloeColumnSpecific attribute =
        FloeHintResolver.columnSpecific(
            ctx, resolver, tableId, relOid, attnum, column, logicalType);
    return new SystemObjectRow(
        new Object[] {
          relOid,
          attribute.getAttname(),
          (int) attribute.getAtttypid(),
          attribute.getAtttypmod(),
          attribute.getAttnum(),
          metadata.attlen(),
          metadata.attbyval(),
          attribute.getAttnotnull(),
          attribute.getAttisdropped(),
          metadata.attalign(),
          metadata.attstorage(),
          metadata.attndims(),
          (int) attribute.getAttcollation()
        });
  }
}
