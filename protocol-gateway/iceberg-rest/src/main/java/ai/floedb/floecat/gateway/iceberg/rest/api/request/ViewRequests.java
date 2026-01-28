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

import ai.floedb.floecat.gateway.iceberg.rest.common.NamespaceListDeserializer;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import java.util.Map;

public final class ViewRequests {
  private ViewRequests() {}

  public record Create(
      String name,
      String location,
      JsonNode schema,
      @JsonProperty("view-version") ViewVersion viewVersion,
      Map<String, String> properties) {}

  public record ViewVersion(
      @JsonProperty("version-id") Integer versionId,
      @JsonProperty("timestamp-ms") Long timestampMs,
      @JsonProperty("schema-id") Integer schemaId,
      Map<String, String> summary,
      List<ViewRepresentation> representations,
      @JsonProperty("default-namespace") List<String> defaultNamespace,
      @JsonProperty("default-catalog") String defaultCatalog) {}

  public record ViewRepresentation(String type, String sql, String dialect) {}

  public record Update(
      String name,
      @JsonDeserialize(using = NamespaceListDeserializer.class) List<String> namespace,
      String sql,
      Map<String, String> properties) {}

  public record Register(String name, @JsonProperty("metadata-location") String metadataLocation) {}

  public record Commit(List<JsonNode> requirements, List<JsonNode> updates) {}
}
