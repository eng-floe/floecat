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

import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.support.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.support.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.table.StagedTableEntry;
import ai.floedb.floecat.gateway.iceberg.rest.table.StagedTableRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class CreateCommitNormalizer {
  @Inject StagedTableRepository stagedTableRepository;

  public TableRequests.Commit normalizeFirstWriteCommit(
      String accountId,
      String catalogName,
      List<String> namespacePath,
      String tableName,
      boolean tableExists,
      ai.floedb.floecat.catalog.rpc.Table persistedTable,
      TableRequests.Commit commit,
      StagedTableEntry stagedEntry) {
    if (commit == null || CommitPlanningPredicates.hasCommittedSnapshot(persistedTable)) {
      return commit;
    }
    StagedTableEntry effectiveStage = stagedEntry;
    if (effectiveStage == null
        && stagedTableRepository != null
        && accountId != null
        && !accountId.isBlank()
        && catalogName != null
        && !catalogName.isBlank()
        && tableName != null
        && !tableName.isBlank()) {
      effectiveStage =
          stagedTableRepository
              .findSingleStage(accountId, catalogName, namespacePath, tableName)
              .orElse(null);
    }
    if (effectiveStage == null
        || effectiveStage.request() == null
        || effectiveStage.spec() == null) {
      return commit;
    }
    String stagedMetadataLocation =
        MetadataLocationUtil.metadataLocation(effectiveStage.request().properties());
    if (callerProvidesCreateInitialization(commit)) {
      return injectMetadataLocation(commit, stagedMetadataLocation);
    }
    List<Map<String, Object>> updates =
        new ArrayList<>(
            CreateUpdateFactory.fromCreateRequest(effectiveStage.request(), effectiveStage.spec()));
    if (commit.updates() != null && !commit.updates().isEmpty()) {
      updates.addAll(commit.updates());
    }
    List<Map<String, Object>> requirements =
        commit.requirements() == null ? new ArrayList<>() : new ArrayList<>(commit.requirements());
    if (!tableExists) {
      requirements.addAll(0, CommitUpdateInspector.assertCreateRequirements());
    }
    return injectMetadataLocation(
        new TableRequests.Commit(List.copyOf(requirements), List.copyOf(updates)),
        stagedMetadataLocation);
  }

  TableRequests.Commit enrichPendingCreateBootstrap(
      String accountId,
      String catalogName,
      List<String> namespacePath,
      String tableName,
      ai.floedb.floecat.catalog.rpc.Table persistedTable,
      TableRequests.Commit commit) {
    return normalizeFirstWriteCommit(
        accountId, catalogName, namespacePath, tableName, true, persistedTable, commit, null);
  }

  private TableRequests.Commit injectMetadataLocation(
      TableRequests.Commit commit, String metadataLocation) {
    if (commit == null
        || metadataLocation == null
        || metadataLocation.isBlank()
        || CommitUpdateInspector.inspect(commit).requestedMetadataLocation() != null) {
      return commit;
    }
    List<Map<String, Object>> updates =
        commit.updates() == null ? new ArrayList<>() : new ArrayList<>(commit.updates());
    updates.add(
        Map.of(
            "action",
            CommitUpdateInspector.ACTION_SET_PROPERTIES,
            "updates",
            Map.of(MetadataLocationUtil.PRIMARY_KEY, metadataLocation)));
    return new TableRequests.Commit(commit.requirements(), List.copyOf(updates));
  }

  private boolean callerProvidesCreateInitialization(TableRequests.Commit commit) {
    if (commit == null || commit.updates() == null || commit.updates().isEmpty()) {
      return false;
    }
    for (Map<String, Object> update : commit.updates()) {
      if (CommitUpdateInspector.isCreateInitializationAction(
          CommitUpdateInspector.actionOf(update))) {
        return true;
      }
    }
    return false;
  }
}
