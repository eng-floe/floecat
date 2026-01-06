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

package ai.floedb.floecat.gateway.iceberg.rest.api.metadata;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ViewMetadataView(
    @JsonProperty("view-uuid") String viewUuid,
    @JsonProperty("format-version") Integer formatVersion,
    @JsonProperty("location") String location,
    @JsonProperty("current-version-id") Integer currentVersionId,
    @JsonProperty("versions") List<ViewVersion> versions,
    @JsonProperty("version-log") List<ViewHistoryEntry> versionLog,
    @JsonProperty("schemas") List<SchemaSummary> schemas,
    @JsonProperty("properties") Map<String, String> properties) {

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record ViewVersion(
      @JsonProperty("version-id") Integer versionId,
      @JsonProperty("timestamp-ms") Long timestampMs,
      @JsonProperty("schema-id") Integer schemaId,
      @JsonProperty("summary") Map<String, String> summary,
      @JsonProperty("representations") List<ViewRepresentation> representations,
      @JsonProperty("default-namespace") List<String> defaultNamespace,
      @JsonProperty("default-catalog") String defaultCatalog) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record ViewHistoryEntry(
      @JsonProperty("version-id") Integer versionId,
      @JsonProperty("timestamp-ms") Long timestampMs) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record ViewRepresentation(String type, String sql, String dialect) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record SchemaSummary(
      @JsonProperty("schema-id") Integer schemaId,
      String type,
      List<Map<String, Object>> fields,
      @JsonProperty("identifier-field-ids") List<Integer> identifierFieldIds) {}
}
