package ai.floedb.metacat.reconciler.impl;

import static ai.floedb.metacat.reconciler.util.NameParts.split;

import ai.floedb.metacat.catalog.rpc.CatalogSpec;
import ai.floedb.metacat.catalog.rpc.CreateCatalogRequest;
import ai.floedb.metacat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.CreateSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.CreateTableRequest;
import ai.floedb.metacat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.metacat.catalog.rpc.NamespaceSpec;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.ResolveTableRequest;
import ai.floedb.metacat.catalog.rpc.SnapshotSpec;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.catalog.rpc.TableSpec;
import ai.floedb.metacat.catalog.rpc.UpdateTableSchemaRequest;
import ai.floedb.metacat.common.rpc.IdempotencyKey;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.connector.spi.ConnectorConfig;
import ai.floedb.metacat.connector.spi.ConnectorFactory;
import ai.floedb.metacat.connector.spi.ConnectorFormat;
import ai.floedb.metacat.connector.spi.MetacatConnector;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

@ApplicationScoped
public class ReconcilerService {
  @Inject GrpcClients clients;

  public Result reconcile(ConnectorConfig cfg, boolean fullRescan) throws Exception {
    long scanned = 0;
    long changed = 0;
    long errors = 0;

    try (MetacatConnector connector = ConnectorFactory.create(cfg)) {
      var catalogId = ensureCatalog(cfg.targetCatalogDisplayName());

      for (String namespace : connector.listNamespaces()) {
        var namespaceId = ensureNamespace(catalogId, namespace);
        for (String tbl : connector.listTables(namespace)) {
          try {
            scanned++;
            var upstreamTable = connector.describe(namespace, tbl);
            var tableId = ensureTable(catalogId, namespaceId, upstreamTable, connector.format());
            if (upstreamTable.currentSnapshotId().isPresent()) {
              ensureSnapshot(
                  tableId,
                  upstreamTable.currentSnapshotId().get(),
                  upstreamTable.currentSnapshotTsMillis().orElse(System.currentTimeMillis()));
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
    public final long scanned;
    public final long changed;
    public final long errors;
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
      var response =
          clients
              .directory()
              .resolveCatalog(
                  ResolveCatalogRequest.newBuilder()
                      .setRef(NameRef.newBuilder().setCatalog(displayName).build())
                      .build());
      return response.getResourceId();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw e;
      }
    }
    var request =
        CreateCatalogRequest.newBuilder()
            .setSpec(CatalogSpec.newBuilder().setDisplayName(displayName).build())
            .setIdempotency(idem("CreateCatalog|" + displayName))
            .build();
    return clients.mutation().createCatalog(request).getCatalog().getResourceId();
  }

  private ResourceId ensureNamespace(ResourceId catalogId, String namespaceFq) {
    var parts = split(namespaceFq);
    try {
      var nameRef =
          NameRef.newBuilder()
              .setCatalog(lookupCatalogName(catalogId))
              .addAllPath(parts.parents)
              .setName(parts.leaf)
              .build();
      return clients
          .directory()
          .resolveNamespace(ResolveNamespaceRequest.newBuilder().setRef(nameRef).build())
          .getResourceId();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw e;
      }
    }
    var spec =
        NamespaceSpec.newBuilder()
            .setCatalogId(catalogId)
            .setDisplayName(parts.leaf)
            .addAllPath(parts.parents)
            .build();
    var request =
        CreateNamespaceRequest.newBuilder()
            .setSpec(spec)
            .setIdempotency(idem("CreateNamespace|" + key(catalogId) + "|" + namespaceFq))
            .build();
    return clients.mutation().createNamespace(request).getNamespace().getResourceId();
  }

  private ResourceId ensureTable(
      ResourceId catalogId,
      ResourceId namespaceId,
      MetacatConnector.UpstreamTable upstreamTable,
      ConnectorFormat format) {
    try {
      var nameRef =
          NameRef.newBuilder()
              .setCatalog(lookupCatalogName(catalogId))
              .addAllPath(split(upstreamTable.namespaceFq()).parents)
              .setName(upstreamTable.tableName())
              .build();
      var tableId =
          clients
              .directory()
              .resolveTable(ResolveTableRequest.newBuilder().setRef(nameRef).build())
              .getResourceId();
      maybeBumpTableSchema(tableId, upstreamTable);
      return tableId;
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw e;
      }
    }

    var spec =
        TableSpec.newBuilder()
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName(upstreamTable.tableName())
            .setRootUri(upstreamTable.location())
            .setSchemaJson(upstreamTable.schemaJson())
            .setFormat(toTableFormat(format))
            .putAllProperties(upstreamTable.properties())
            .build();

    var request =
        CreateTableRequest.newBuilder()
            .setSpec(spec)
            .setIdempotency(
                idem(
                    "CreateTable|"
                        + key(catalogId)
                        + "|"
                        + key(namespaceId)
                        + "|"
                        + upstreamTable.tableName()))
            .build();
    return clients.mutation().createTable(request).getTable().getResourceId();
  }

  private void maybeBumpTableSchema(
      ResourceId tableId, MetacatConnector.UpstreamTable upstreamTable) {
    var request =
        UpdateTableSchemaRequest.newBuilder()
            .setTableId(tableId)
            .setSchemaJson(upstreamTable.schemaJson())
            .addAllPartitionKeys(upstreamTable.partitionKeys())
            .putAllProperties(upstreamTable.properties())
            .build();
    try {
      clients.mutation().updateTableSchema(request);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.FAILED_PRECONDITION) {
        throw e;
      }
    }
  }

  private void ensureSnapshot(ResourceId tableId, long snapshotId, long upstreamTsMs) {
    var spec =
        SnapshotSpec.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setUpstreamCreatedAt(Timestamps.fromMillis(upstreamTsMs))
            .setIngestedAt(Timestamps.fromMillis(System.currentTimeMillis()))
            .build();
    var request =
        CreateSnapshotRequest.newBuilder()
            .setSpec(spec)
            .setIdempotency(idem("CreateSnapshot|" + key(tableId) + "|" + snapshotId))
            .build();
    try {
      clients.mutation().createSnapshot(request);
    } catch (StatusRuntimeException e) {
      var c = e.getStatus().getCode();
      if (c != Status.Code.ALREADY_EXISTS
          && c != Status.Code.ABORTED
          && c != Status.Code.FAILED_PRECONDITION) {
        throw e;
      }
    }
  }

  private static IdempotencyKey idem(String rawKey) {
    var key =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(rawKey.getBytes(StandardCharsets.UTF_8));
    return IdempotencyKey.newBuilder().setKey(key).build();
  }

  private static String key(ResourceId resourceId) {
    return resourceId.getTenantId() + ":" + resourceId.getId();
  }

  private String lookupCatalogName(ResourceId catalogId) {
    return clients
        .directory()
        .lookupCatalog(LookupCatalogRequest.newBuilder().setResourceId(catalogId).build())
        .getDisplayName();
  }

  private static TableFormat toTableFormat(ConnectorFormat format) {
    if (format == null) {
      return TableFormat.TF_UNSPECIFIED;
    }

    String name = format.name();
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
