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

import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.asObjectMap;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.support.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.support.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.table.SnapshotUpdateService;
import ai.floedb.floecat.gateway.iceberg.rest.table.TablePropertyService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@ApplicationScoped
public class CommitUpdateCompiler {
  public record CompiledTablePatch(TableSpec spec, FieldMask mask) {}

  public record CompileResult(CompiledTablePatch patch, Response error) {
    static CompileResult success(CompiledTablePatch patch) {
      return new CompileResult(patch, null);
    }

    static CompileResult failure(Response error) {
      return new CompileResult(null, error);
    }

    public boolean hasError() {
      return error != null;
    }
  }

  @Inject TablePropertyService tablePropertyService;
  @Inject SnapshotUpdateService snapshotUpdateService;
  @Inject ObjectMapper mapper;

  public CompileResult compile(
      TableRequests.Commit request,
      Supplier<Table> tableSupplier,
      boolean validateSnapshotUpdates) {
    if (request == null) {
      return CompileResult.failure(IcebergErrorResponses.validation("Request body is required"));
    }
    if (request.updates() == null) {
      return CompileResult.failure(IcebergErrorResponses.validation("updates are required"));
    }
    String unsupported = unsupportedUpdateAction(request);
    if (unsupported != null) {
      return CompileResult.failure(
          IcebergErrorResponses.validation("unsupported commit update action: " + unsupported));
    }
    if (validateSnapshotUpdates) {
      Response snapshotValidation =
          snapshotUpdateService.validateSnapshotUpdates(request.updates());
      if (snapshotValidation != null) {
        return CompileResult.failure(snapshotValidation);
      }
    }

    TableSpec.Builder spec = TableSpec.newBuilder();
    FieldMask.Builder mask = FieldMask.newBuilder();
    Response locationError =
        tablePropertyService.applyLocationUpdate(spec, mask, tableSupplier, request.updates());
    if (locationError != null) {
      return CompileResult.failure(locationError);
    }
    var propertyResult =
        tablePropertyService.applyCommitPropertyUpdates(tableSupplier, null, request.updates());
    if (propertyResult.hasError()) {
      return CompileResult.failure(propertyResult.error());
    }
    Map<String, String> mergedProps = stripFileIoProperties(propertyResult.properties());
    applyTableDefinitionSpecUpdates(spec, mask, request.updates());
    if (mergedProps != null) {
      spec.clearProperties().putAllProperties(mergedProps);
      mask.addPaths("properties");
    }
    return CompileResult.success(new CompiledTablePatch(spec.build(), mask.build()));
  }

  private String unsupportedUpdateAction(TableRequests.Commit req) {
    if (req == null || req.updates() == null) {
      return null;
    }
    for (Map<String, Object> update : req.updates()) {
      if (update == null) {
        return "<missing>";
      }
      String action = CommitUpdateInspector.actionOf(update);
      if (action == null || action.isBlank()) {
        return "<missing>";
      }
      if (!CommitUpdateInspector.isSupportedUpdateAction(action)) {
        return action;
      }
    }
    return null;
  }

  private void applyTableDefinitionSpecUpdates(
      TableSpec.Builder spec, FieldMask.Builder mask, List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return;
    }
    for (Map<String, Object> update : updates) {
      if (update == null) {
        continue;
      }
      String action = CommitUpdateInspector.actionOf(update);
      if (!CommitUpdateInspector.ACTION_ADD_SCHEMA.equals(action)) {
        continue;
      }
      Map<String, Object> schema = asObjectMap(update.get("schema"));
      if (schema == null || schema.isEmpty()) {
        continue;
      }
      try {
        String schemaJson = mapper.writeValueAsString(schema);
        if (!schemaJson.isBlank()) {
          spec.setSchemaJson(schemaJson);
          mask.addPaths("schema_json");
        }
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Invalid add-schema payload", e);
      }
    }
  }

  private Map<String, String> stripFileIoProperties(Map<String, String> mergedProps) {
    if (mergedProps == null || mergedProps.isEmpty()) {
      return mergedProps;
    }
    mergedProps.keySet().removeIf(FileIoFactory::isFileIoProperty);
    return mergedProps;
  }
}
