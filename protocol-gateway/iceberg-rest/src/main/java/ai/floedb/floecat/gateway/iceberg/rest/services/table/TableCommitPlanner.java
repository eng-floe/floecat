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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.common.rpc.ResourceId;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

@ApplicationScoped
public class TableCommitPlanner {

  @Inject TableUpdatePlanner tableUpdatePlanner;

  public record PlanResult(Table table, Response error) {
    public boolean hasError() {
      return error != null;
    }
  }

  public PlanResult plan(
      TableCommitService.CommitCommand command,
      Supplier<Table> tableSupplier,
      Supplier<Table> requirementTableSupplier,
      ResourceId tableId) {
    var updatePlan =
        tableUpdatePlanner.planTransactionUpdates(
            command, tableSupplier, requirementTableSupplier, tableId);
    if (updatePlan.hasError()) {
      return new PlanResult(null, updatePlan.error());
    }
    Table current = tableSupplier.get();
    Table next = applySpec(current, updatePlan.spec(), updatePlan.mask());
    return new PlanResult(next, null);
  }

  public PlanResult plan(
      TableCommitService.CommitCommand command, Supplier<Table> tableSupplier, ResourceId tableId) {
    return plan(command, tableSupplier, tableSupplier, tableId);
  }

  private Table applySpec(Table current, TableSpec.Builder spec, FieldMask.Builder mask) {
    if (mask == null || mask.getPathsCount() == 0) {
      return current;
    }
    Table.Builder out = current.toBuilder();
    Set<String> paths = new HashSet<>(mask.getPathsList());
    if (paths.contains("display_name") && spec.hasDisplayName()) {
      out.setDisplayName(spec.getDisplayName());
    }
    if (paths.contains("description") && spec.hasDescription()) {
      out.setDescription(spec.getDescription());
    }
    if (paths.contains("schema_json") && spec.hasSchemaJson()) {
      out.setSchemaJson(spec.getSchemaJson());
    }
    if (paths.contains("catalog_id") && spec.hasCatalogId()) {
      out.setCatalogId(spec.getCatalogId());
    }
    if (paths.contains("namespace_id") && spec.hasNamespaceId()) {
      out.setNamespaceId(spec.getNamespaceId());
    }
    if ((paths.contains("upstream") || paths.contains("upstream.uri")) && spec.hasUpstream()) {
      out.setUpstream(spec.getUpstream());
    }
    if (paths.contains("properties")) {
      out.clearProperties().putAllProperties(spec.getPropertiesMap());
    }
    return out.build();
  }
}
