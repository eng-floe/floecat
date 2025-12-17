package ai.floedb.floecat.extensions.floedb.pgcatalog;

import static ai.floedb.floecat.extensions.utils.FloePayloads.RELATION;
import static ai.floedb.floecat.extensions.utils.ScannerUtils.col;

import ai.floedb.floecat.extensions.floedb.proto.FloeRelationSpecific;
import ai.floedb.floecat.extensions.utils.ScannerUtils;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public final class PgClassScanner implements SystemObjectScanner {

  public static final List<SchemaColumn> SCHEMA =
      List.of(
          col("oid", "INT"),
          col("relname", "VARCHAR"),
          col("relnamespace", "INT"),
          col("relkind", "CHAR"),
          col("relowner", "INT"),
          col("relhasindex", "BOOLEAN"),
          col("relisshared", "BOOLEAN"),
          col("reltuples", "FLOAT"),
          col("relpages", "INT"));

  @Override
  public List<SchemaColumn> schema() {
    return SCHEMA;
  }

  @Override
  public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
    return ctx.listNamespaces().stream()
        .flatMap(ns -> ctx.listRelations(ns.id()).stream())
        .filter(this::supportedRelation)
        .map(node -> row(ctx, node));
  }

  private boolean supportedRelation(GraphNode node) {
    return node instanceof TableNode || node instanceof ViewNode;
  }

  private SystemObjectRow row(SystemObjectScanContext ctx, GraphNode node) {

    Optional<FloeRelationSpecific> spec = ScannerUtils.payload(ctx, node.id(), RELATION);

    int oid =
        spec.map(FloeRelationSpecific::getOid)
            .filter(v -> v > 0)
            .orElseGet(() -> ScannerUtils.fallbackOid(node.id()));

    String relname = spec.map(FloeRelationSpecific::getRelname).orElseGet(node::displayName);

    int relnamespace =
        spec.map(FloeRelationSpecific::getRelnamespace)
            .filter(v -> v > 0)
            .orElse(PgCatalogProvider.PG_CATALOG_OID);

    String relkind =
        spec.map(FloeRelationSpecific::getRelkind).orElseGet(() -> defaultRelKind(node));

    int relowner =
        spec.map(FloeRelationSpecific::getRelowner)
            .filter(v -> v > 0)
            .orElseGet(ScannerUtils::defaultOwnerOid);

    boolean relhasindex = spec.map(FloeRelationSpecific::getRelhasindex).orElse(false);

    boolean relisshared = spec.map(FloeRelationSpecific::getRelisshared).orElse(false);

    float reltuples = spec.map(FloeRelationSpecific::getReltuples).orElse(0);

    int relpages = spec.map(FloeRelationSpecific::getRelpages).orElse(0);

    return new SystemObjectRow(
        new Object[] {
          oid,
          relname,
          relnamespace,
          relkind,
          relowner,
          relhasindex,
          relisshared,
          reltuples,
          relpages
        });
  }

  private static String defaultRelKind(GraphNode node) {
    if (node instanceof ViewNode) return "v";
    return "r"; // TableNode
  }
}
