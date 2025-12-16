package ai.floedb.floecat.extensions.floedb.pgcatalog;

import ai.floedb.floecat.extensions.floedb.proto.FloeNamespaceSpecific;
import ai.floedb.floecat.metagraph.model.EngineHint;
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
    return ctx.listNamespaces().stream().map(this::toRow);
  }

  private SystemObjectRow toRow(NamespaceNode ns) {
    FloeNamespaceSpecific spec = resolveSpec(ns);

    int oid = spec != null && spec.hasOid() ? spec.getOid() : ScannerUtils.getNodeOid(ns);

    String name = spec != null && spec.hasNspname() ? spec.getNspname() : ns.displayName();

    int owner =
        spec != null && spec.hasNspowner() ? spec.getNspowner() : ScannerUtils.defaultOwnerOid(ns);

    String[] acl =
        spec != null && spec.getNspaclCount() > 0
            ? spec.getNspaclList().toArray(String[]::new)
            : new String[0];

    return new SystemObjectRow(new Object[] {oid, name, owner, acl});
  }

  // ----------------------------------------------------------------------
  // Helpers
  // ----------------------------------------------------------------------

  private static FloeNamespaceSpecific decodeSpec(EngineHint hint) {
    if (hint == null) {
      return null;
    }
    if (!"floe.namespace+proto".equals(hint.contentType())) {
      return null;
    }
    try {
      return FloeNamespaceSpecific.parseFrom(hint.payload());
    } catch (Exception e) {
      return null;
    }
  }

  private static FloeNamespaceSpecific resolveSpec(NamespaceNode ns) {
    for (EngineHint hint : ns.engineHints().values()) {
      FloeNamespaceSpecific spec = decodeSpec(hint);
      if (spec != null) {
        return spec;
      }
    }
    return null;
  }
}
