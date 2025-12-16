package ai.floedb.floecat.extensions.floedb.pgcatalog;

import ai.floedb.floecat.extensions.floedb.proto.FloeNamespaceSpecific;
import ai.floedb.floecat.extensions.utils.FloePayloads;
import ai.floedb.floecat.extensions.utils.ScannerUtils;
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
    var specOpt = ScannerUtils.payload(ctx, ns.id(), FloePayloads.NAMESPACE);
    FloeNamespaceSpecific spec = specOpt.orElse(null);

    int oid = spec != null && spec.hasOid() ? spec.getOid() : ScannerUtils.fallbackOid(ns.id());

    String name = spec != null && spec.hasNspname() ? spec.getNspname() : ns.displayName();

    int owner =
        spec != null && spec.hasNspowner() ? spec.getNspowner() : ScannerUtils.defaultOwnerOid();

    String[] acl =
        spec != null && spec.getNspaclCount() > 0
            ? spec.getNspaclList().toArray(String[]::new)
            : new String[0];

    return new SystemObjectRow(new Object[] {oid, name, owner, acl});
  }
}
