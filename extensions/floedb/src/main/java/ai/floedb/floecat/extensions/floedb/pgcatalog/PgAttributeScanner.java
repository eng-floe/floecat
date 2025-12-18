package ai.floedb.floecat.extensions.floedb.pgcatalog;

import ai.floedb.floecat.extensions.floedb.proto.FloeRelationSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeTypeSpecific;
import ai.floedb.floecat.extensions.floedb.utils.FloePayloads;
import ai.floedb.floecat.extensions.floedb.utils.ScannerUtils;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import ai.floedb.floecat.systemcatalog.spi.types.EngineTypeMapper;
import ai.floedb.floecat.systemcatalog.spi.types.TypeResolver;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeFormat;
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
          ScannerUtils.col("attnum", "INT"),
          ScannerUtils.col("attlen", "INT"),
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
        ScannerUtils.oid(ctx, table.id(), FloePayloads.RELATION, FloeRelationSpecific::getOid);

    // Get logical schema (already normalized by LogicalSchemaMapper)
    List<SchemaColumn> columns = ctx.graph().tableSchema(table.id());

    AtomicInteger attnum = new AtomicInteger(1);

    return columns.stream().map(col -> row(ctx, resolver, relOid, attnum.getAndIncrement(), col));
  }

  // ----------------------------------------------------------------------
  // Row construction
  // ----------------------------------------------------------------------

  private SystemObjectRow row(
      SystemObjectScanContext ctx,
      TypeResolver resolver,
      int relOid,
      int attnum,
      SchemaColumn column) {

    LogicalType logical = LogicalTypeFormat.parse(column.getLogicalType());

    TypeNode type = resolver.resolveOrThrow(logical);

    int typeOid = ScannerUtils.oid(ctx, type.id(), FloePayloads.TYPE, FloeTypeSpecific::getOid);

    var typeSpec = ScannerUtils.payload(ctx, type.id(), FloePayloads.TYPE);

    int attlen = typeSpec.map(FloeTypeSpecific::getTyplen).orElse(-1);
    String attalign =
        typeSpec.map(FloeTypeSpecific::getTypalign).filter(s -> !s.isBlank()).orElse("i");
    String attstorage =
        typeSpec.map(FloeTypeSpecific::getTypstorage).filter(s -> !s.isBlank()).orElse("p");
    int attndims = typeSpec.map(FloeTypeSpecific::getTypndims).orElse(0);
    int attcollation = typeSpec.map(FloeTypeSpecific::getTypcollation).orElse(0);

    return new SystemObjectRow(
        new Object[] {
          relOid,
          column.getName(),
          typeOid,
          attnum,
          attlen,
          !column.getNullable(),
          false,
          attalign,
          attstorage,
          attndims,
          attcollation
        });
  }
}
