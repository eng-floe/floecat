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

package ai.floedb.floecat.gateway.iceberg.rest.table.transaction;

import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.firstNonBlank;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.UUID;

@ApplicationScoped
public class CommitTargetResolver {
  public record ResolvedTarget(
      List<String> namespacePath,
      ResourceId namespaceId,
      String tableName,
      ResourceId tableId,
      GetTableResponse tableResponse,
      ai.floedb.floecat.catalog.rpc.Table persistedTable,
      long pointerVersion,
      boolean hadCommittedSnapshot) {}

  public ResolvedTarget resolve(
      CommitRequestContext context, TransactionCommitRequest.TableChange change) {
    var identifier = change.identifier();
    List<String> namespacePath =
        identifier.namespace() == null ? List.of() : List.copyOf(identifier.namespace());
    ResourceId namespaceId =
        context.tableSupport().resolveNamespaceId(context.catalogName(), namespacePath);
    boolean assertCreateRequested =
        CommitPlanningPredicates.requiresAssertCreate(change.requirements());

    ResourceId tableId;
    GetTableResponse tableResponse;
    try {
      tableId =
          context
              .tableSupport()
              .resolveTableId(context.catalogName(), namespacePath, identifier.name());
      tableResponse = context.tableSupport().getTableResponse(tableId);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND || !assertCreateRequested) {
        throw e;
      }
      tableId =
          atomicCreateTableId(
              context.accountId(),
              context.txId(),
              context.catalogId(),
              namespaceId,
              namespacePath,
              identifier.name());
      tableResponse = null;
    }
    if (assertCreateRequested && tableResponse != null && tableResponse.hasTable()) {
      throw new WebApplicationException(
          ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses.failure(
              "assert-create failed", "CommitFailedException", Response.Status.CONFLICT));
    }

    ai.floedb.floecat.catalog.rpc.Table persistedTable =
        tableResponse == null || !tableResponse.hasTable()
            ? newCreateTableStub(
                tableId,
                context.catalogId(),
                namespaceId,
                identifier.name(),
                context.txCreatedAtMs())
            : tableResponse.getTable();
    long pointerVersion =
        tableResponse != null && tableResponse.hasMeta()
            ? tableResponse.getMeta().getPointerVersion()
            : 0L;
    return new ResolvedTarget(
        namespacePath,
        namespaceId,
        identifier.name(),
        tableId,
        tableResponse,
        persistedTable,
        pointerVersion,
        CommitPlanningPredicates.hasCommittedSnapshot(persistedTable));
  }

  public ResourceId scopeTableIdWithAccount(ResourceId tableId, String accountId) {
    if (tableId == null) {
      return null;
    }
    String resolvedAccount = firstNonBlank(tableId.getAccountId(), accountId);
    if (resolvedAccount == null || resolvedAccount.isBlank()) {
      return tableId;
    }
    if (resolvedAccount.equals(tableId.getAccountId())) {
      return tableId;
    }
    return tableId.toBuilder().setAccountId(resolvedAccount).build();
  }

  private ResourceId atomicCreateTableId(
      String accountId,
      String txId,
      ResourceId catalogId,
      ResourceId namespaceId,
      List<String> namespacePath,
      String tableName) {
    String catalogPart = catalogId == null ? "<catalog>" : catalogId.getId();
    String namespacePart =
        namespacePath == null || namespacePath.isEmpty()
            ? (namespaceId == null ? "<namespace>" : namespaceId.getId())
            : String.join(".", namespacePath);
    String seed =
        (txId == null ? "" : txId)
            + "|"
            + catalogPart
            + "|"
            + namespacePart
            + "|"
            + (tableName == null ? "" : tableName);
    UUID deterministicId =
        UUID.nameUUIDFromBytes(seed.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    return ResourceId.newBuilder()
        .setAccountId(accountId == null ? "" : accountId)
        .setId("tbl-" + deterministicId)
        .setKind(ResourceKind.RK_TABLE)
        .build();
  }

  private ai.floedb.floecat.catalog.rpc.Table newCreateTableStub(
      ResourceId tableId,
      ResourceId catalogId,
      ResourceId namespaceId,
      String tableName,
      long createdAtMs) {
    ai.floedb.floecat.catalog.rpc.Table.Builder builder =
        ai.floedb.floecat.catalog.rpc.Table.newBuilder();
    if (tableId != null) {
      builder.setResourceId(tableId);
    }
    if (catalogId != null) {
      builder.setCatalogId(catalogId);
    }
    if (namespaceId != null) {
      builder.setNamespaceId(namespaceId);
    }
    if (tableName != null && !tableName.isBlank()) {
      builder.setDisplayName(tableName);
    }
    builder.setCreatedAt(Timestamps.fromMillis(Math.max(0L, createdAtMs)));
    builder.setUpstream(
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_ICEBERG)
            .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_FIELD_ID)
            .build());
    return builder.build();
  }
}
