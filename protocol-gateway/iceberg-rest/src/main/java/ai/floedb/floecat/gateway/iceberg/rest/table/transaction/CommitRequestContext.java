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

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableMetadata;

record CommitRequestContext(
    String accountId,
    String txId,
    String prefix,
    String catalogName,
    ResourceId catalogId,
    String idempotencyBase,
    String requestHash,
    long txCreatedAtMs,
    TransactionState currentState,
    TableGatewaySupport tableSupport,
    List<ValidatedTableChange> changes,
    boolean preMaterializeAssertCreate,
    List<PlannedTableChange> plannedChanges) {

  CommitRequestContext withPlannedChanges(List<PlannedTableChange> plannedChanges) {
    return new CommitRequestContext(
        accountId,
        txId,
        prefix,
        catalogName,
        catalogId,
        idempotencyBase,
        requestHash,
        txCreatedAtMs,
        currentState,
        tableSupport,
        changes,
        preMaterializeAssertCreate,
        plannedChanges == null ? List.of() : List.copyOf(plannedChanges));
  }
}

record ValidatedTableChange(
    TransactionCommitRequest.TableChange rawChange, ParsedCommit parsedCommit) {
  TableIdentifierDto identifier() {
    return rawChange.identifier();
  }

  List<Map<String, Object>> requirements() {
    return parsedCommit.requirements();
  }

  List<Map<String, Object>> updates() {
    return parsedCommit.updates();
  }
}

record PlannedTableChange(
    ValidatedTableChange requestedChange,
    CommitTargetResolver.ResolvedTarget target,
    ParsedCommit normalizedCommit,
    CommitUpdateCompiler.CompiledTablePatch compiledPatch,
    ai.floedb.floecat.catalog.rpc.Table updatedTable,
    TableMetadata committedMetadata) {
  TableIdentifierDto identifier() {
    return requestedChange.identifier();
  }

  TableRequests.Commit commitRequest() {
    return normalizedCommit.toCommitRequest();
  }
}
