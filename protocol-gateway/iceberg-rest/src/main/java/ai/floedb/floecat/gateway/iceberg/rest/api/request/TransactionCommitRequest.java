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

package ai.floedb.floecat.gateway.iceberg.rest.api.request;

import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record TransactionCommitRequest(
    @JsonProperty("table-changes") List<@Valid TableChange> tableChanges,
    @JsonProperty("staged-ref-updates") List<@Valid StagedRefUpdate> stagedRefUpdates,
    @JsonProperty("requirements") List<Map<String, Object>> requirements,
    @JsonProperty("updates") List<Map<String, Object>> updates) {

  public List<TableChange> resolvedTableChanges() {
    if (tableChanges != null && !tableChanges.isEmpty()) {
      return tableChanges;
    }
    if (stagedRefUpdates == null || stagedRefUpdates.isEmpty()) {
      return List.of();
    }
    List<TableChange> converted = new ArrayList<>();
    for (StagedRefUpdate staged : stagedRefUpdates) {
      converted.add(
          new TableChange(
              staged.table(), staged.stageId(), staged.requirements(), staged.updates()));
    }
    return converted;
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record TableChange(
      @JsonProperty("identifier") @JsonAlias("table") @NotNull @Valid TableIdentifierDto identifier,
      @JsonProperty("stage-id") String stageId,
      @JsonProperty("requirements") List<Map<String, Object>> requirements,
      @JsonProperty("updates") List<Map<String, Object>> updates) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record StagedRefUpdate(
      @JsonProperty("table") @NotNull @Valid TableIdentifierDto table,
      @JsonProperty("stage-id") String stageId,
      @JsonProperty("requirements") List<Map<String, Object>> requirements,
      @JsonProperty("updates") List<Map<String, Object>> updates) {}
}
