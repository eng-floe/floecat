package ai.floedb.floecat.extensions.floedb.pgcatalog;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.extensions.floedb.proto.FloeFunctionSpecific;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import java.util.List;
import java.util.stream.Stream;

/**
 * pg_catalog.pg_proc
 *
 * <p>Exposes all visible functions (system + user) through PostgreSQL-compatible metadata rows.
 * Values are sourced from engine payloads when available and defaulted otherwise.
 */
public final class PgProcScanner implements SystemObjectScanner {

  public static final List<SchemaColumn> SCHEMA =
      List.of(
          col("oid", "INT"),
          col("proname", "VARCHAR"),
          col("pronamespace", "INT"),
          col("prorettype", "INT"),
          col("proargtypes", "INT[]"),
          col("proisagg", "BOOLEAN"),
          col("proiswindow", "BOOLEAN"));

  @Override
  public List<SchemaColumn> schema() {
    return SCHEMA;
  }

  @Override
  public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
    return ctx.listNamespaces().stream()
        .flatMap(
            ns -> ctx.listFunctions(ns.id()).stream().map(fn -> new NamespaceFunction(ns.id(), fn)))
        .map(
            nf -> {
              FunctionNode fn = nf.function();
              ResourceId namespaceId = nf.namespaceId();

              FloeFunctionSpecific spec = resolveSpec(fn);

              int oid = spec != null && spec.hasOid() ? spec.getOid() : fallbackOid(fn);

              int namespaceOid =
                  spec != null && spec.hasPronamespace()
                      ? spec.getPronamespace()
                      : Math.abs(namespaceId.hashCode());

              int returnTypeOid = spec != null && spec.hasProrettype() ? spec.getProrettype() : 0;

              int[] argTypeOids =
                  spec != null && spec.getProargtypesCount() > 0
                      ? spec.getProargtypesList().stream().mapToInt(Integer::intValue).toArray()
                      : new int[0];

              return new SystemObjectRow(
                  new Object[] {
                    oid,
                    fn.displayName(),
                    namespaceOid,
                    returnTypeOid,
                    argTypeOids,
                    fn.aggregate(),
                    fn.window()
                  });
            });
  }

  // ----------------------------------------------------------------------
  // Helpers
  // ----------------------------------------------------------------------

  private static int fallbackOid(FunctionNode fn) {
    // TODO: improve to use Snowflake ID once available
    return Math.abs(fn.id().hashCode());
  }

  private static SchemaColumn col(String name, String type) {
    return SchemaColumn.newBuilder().setName(name).setLogicalType(type).setNullable(false).build();
  }

  private static FloeFunctionSpecific decodeSpec(EngineHint hint) {
    if (hint == null) {
      return null;
    }
    if (!"floe.function+proto".equals(hint.contentType())) {
      return null;
    }
    try {
      return FloeFunctionSpecific.parseFrom(hint.payload());
    } catch (Exception e) {
      return null; // or log if you want
    }
  }

  private static FloeFunctionSpecific resolveSpec(FunctionNode fn) {
    for (EngineHint hint : fn.engineHints().values()) {
      FloeFunctionSpecific spec = decodeSpec(hint);
      if (spec != null) {
        return spec;
      }
    }
    return null;
  }

  private record NamespaceFunction(ResourceId namespaceId, FunctionNode function) {}
}
