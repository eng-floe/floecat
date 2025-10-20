package ai.floedb.metacat.reconciler.impl;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.connector.spi.ConnectorConfig;
import ai.floedb.metacat.connector.spi.ConnectorFactory;
import ai.floedb.metacat.connector.spi.ConnectorFormat;
import ai.floedb.metacat.connector.spi.MetacatConnector;

import static ai.floedb.metacat.reconciler.util.NameParts.split;

@ApplicationScoped
public class ReconcilerService {

  @Inject GrpcClients mc;

  public Result reconcile(ConnectorConfig cfg, boolean fullRescan) throws Exception {
    long scanned = 0, changed = 0, errors = 0;

    try (MetacatConnector c = ConnectorFactory.create(cfg)) {
      var catId = ensureCatalog(cfg.targetCatalogDisplayName());

      for (String ns : c.listNamespaces()) {
        var nsId = ensureNamespace(catId, ns);
        for (String tbl : c.listTables(ns)) {
          try {
            scanned++;
            var up = c.describe(ns, tbl);
            var tblId = ensureTable(catId, nsId, up, c.format());
            if (up.currentSnapshotId().isPresent()) {
              ensureSnapshot(tblId, up.currentSnapshotId().get(),
                  up.currentSnapshotTsMillis().orElse(System.currentTimeMillis()));
            }
            changed++;
          } catch (Exception te) {
            errors++;
          }
        }
      }
      return new Result(scanned, changed, errors, null);
    } catch (Exception e) {
      return new Result(scanned, changed, errors, e);
    }
  }

  public static final class Result {
    public final long scanned, changed, errors;
    public final Exception error;

    public Result(long scanned, long changed, long errors, Exception error) {
      this.scanned = scanned;
      this.changed = changed;
      this.errors = errors;
      this.error = error;
    }

    public boolean ok() {
      return error == null;
    }
    
    public String message() {
      return ok() ? "OK" : error.getMessage();
    }
  }

  private ResourceId ensureCatalog(String displayName) {
    try {
      var res = mc.directory().resolveCatalog(ResolveCatalogRequest.newBuilder()
          .setRef(NameRef.newBuilder().setCatalog(displayName).build())
          .build());
      return res.getResourceId();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) throw e;
    }
    var req = CreateCatalogRequest.newBuilder()
        .setSpec(CatalogSpec.newBuilder().setDisplayName(displayName).build())
        .setIdempotency(idem("CreateCatalog|" + displayName))
        .build();
    return mc.mutation().createCatalog(req).getCatalog().getResourceId();
  }

   private ResourceId ensureNamespace(ResourceId catalogId, String namespaceFq) {
    var parts = split(namespaceFq);
    try {
      var ref = NameRef.newBuilder()
          .setCatalog(lookupCatalogName(catalogId))
          .addAllPath(parts.parents)
          .setName(parts.leaf)
          .build();
      return mc.directory().resolveNamespace(
          ResolveNamespaceRequest.newBuilder().setRef(ref).build()
      ).getResourceId();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) throw e;
    }
    var spec = NamespaceSpec.newBuilder()
        .setCatalogId(catalogId).setDisplayName(parts.leaf).addAllPath(parts.parents).build();
    var req = CreateNamespaceRequest.newBuilder()
        .setSpec(spec).setIdempotency(idem("CreateNamespace|" + key(catalogId) + "|" + namespaceFq)).build();
    return mc.mutation().createNamespace(req).getNamespace().getResourceId();
  }

  private ResourceId ensureTable(ResourceId catalogId,
                                 ResourceId namespaceId,
                                 MetacatConnector.UpstreamTable up,
                                 ConnectorFormat fmt) {
    try {
      var ref = NameRef.newBuilder()
          .setCatalog(lookupCatalogName(catalogId))
          .addAllPath(split(up.namespaceFq()).parents)
          .setName(up.tableName())
          .build();
      var tid = mc.directory().resolveTable(
          ResolveTableRequest.newBuilder().setRef(ref).build()
      ).getResourceId();
      maybeBumpTableSchema(tid, up);
      return tid;
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) throw e;
    }

    var spec = TableSpec.newBuilder()
        .setCatalogId(catalogId)
        .setNamespaceId(namespaceId)
        .setDisplayName(up.tableName())
        .setRootUri(up.location())
        .setSchemaJson(up.schemaJson())
        .setFormat(toTableFormat(fmt))
        .putAllProperties(up.properties())
        .build();

    var req = CreateTableRequest.newBuilder()
        .setSpec(spec)
        .setIdempotency(idem("CreateTable|" + key(catalogId) + "|" + key(namespaceId) + "|" + up.tableName()))
        .build();
    return mc.mutation().createTable(req).getTable().getResourceId();
  }

  private void maybeBumpTableSchema(ResourceId tableId, MetacatConnector.UpstreamTable up) {
    var upd = UpdateTableSchemaRequest.newBuilder()
        .setTableId(tableId)
        .setSchemaJson(up.schemaJson())
        .addAllPartitionKeys(up.partitionKeys())
        .putAllProperties(up.properties())
        .build();
    try {
      mc.mutation().updateTableSchema(upd);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.FAILED_PRECONDITION) throw e;
    }
  }

  private void ensureSnapshot(ResourceId tableId, long snapshotId, long upstreamTsMs) {
    var spec = SnapshotSpec.newBuilder()
        .setTableId(tableId)
        .setSnapshotId(snapshotId)
        .setUpstreamCreatedAt(Timestamps.fromMillis(upstreamTsMs))
        .setIngestedAt(Timestamps.fromMillis(System.currentTimeMillis()))
        .build();
    var req = CreateSnapshotRequest.newBuilder()
        .setSpec(spec).setIdempotency(idem("CreateSnapshot|" + key(tableId) + "|" + snapshotId)).build();
    try { mc.mutation().createSnapshot(req); }
    catch (StatusRuntimeException e) {
      var c = e.getStatus().getCode();
      if (c != Status.Code.ALREADY_EXISTS && c != Status.Code.ABORTED && c != Status.Code.FAILED_PRECONDITION) throw e;
    }
  }

  private static IdempotencyKey idem(String s) {
    var key = Base64.getUrlEncoder().withoutPadding()
        .encodeToString(s.getBytes(StandardCharsets.UTF_8));
    return IdempotencyKey.newBuilder().setKey(key).build();
  }

  private static String key(ResourceId id) {
    return id.getTenantId() + ":" + id.getId(); 
  }

  private String lookupCatalogName(ResourceId catalogId) {
    return mc.directory().lookupCatalog(
        LookupCatalogRequest.newBuilder().setResourceId(catalogId).build()
    ).getDisplayName();
  }

  private static TableFormat toTableFormat(ConnectorFormat src) {
    if (src == null) return TableFormat.TF_UNSPECIFIED;

    String name = src.name();
    int i = name.indexOf('_');
    String stem = (i >= 0 && i + 1 < name.length()) ? name.substring(i + 1) : name;

    String target = "TF_" + stem;

    try {
      return TableFormat.valueOf(target);
    } catch (IllegalArgumentException ignored) {
      return TableFormat.TF_UNKNOWN;
    }
  }
}
