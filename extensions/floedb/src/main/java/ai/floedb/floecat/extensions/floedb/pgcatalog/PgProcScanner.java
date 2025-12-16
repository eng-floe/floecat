package ai.floedb.floecat.extensions.floedb.pgcatalog;

import static ai.floedb.floecat.extensions.utils.FloePayloads.FUNCTION;
import static ai.floedb.floecat.extensions.utils.FloePayloads.NAMESPACE;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.extensions.floedb.proto.FloeFunctionSpecific;
import ai.floedb.floecat.extensions.utils.ScannerUtils;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import java.util.List;
import java.util.Optional;
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

    Optional<FloeFunctionSpecific> spec = ScannerUtils.payload(ctx, fn.id(), FUNCTION);

    int oid =
        spec.map(FloeFunctionSpecific::getOid)
            .orElse(ScannerUtils.oid(ctx, fn.id(), FUNCTION, s -> s.getOid()));

    int namespaceOid = ScannerUtils.oid(ctx, namespaceId, NAMESPACE, spec1 -> spec1.getOid());

    int returnTypeOid =
        spec.map(FloeFunctionSpecific::getProrettype)
            .orElse(ScannerUtils.oid(ctx, fn.id(), FUNCTION, s -> s.getProrettype()));

    int[] argTypeOids =
        spec.map(s -> s.getProargtypesList().stream().mapToInt(Integer::intValue).toArray())
            .orElseGet(
                () ->
                    ScannerUtils.array(
                        ctx,
                        fn.id(),
                        FUNCTION,
                        s ->
                            s.getProargtypesList().stream().mapToInt(Integer::intValue).toArray()));

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
  }

  private record NamespaceFunction(ResourceId namespaceId, FunctionNode function) {}
}
