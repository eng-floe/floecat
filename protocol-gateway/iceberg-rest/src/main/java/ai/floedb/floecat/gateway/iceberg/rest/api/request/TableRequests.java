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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;

public final class TableRequests {
  private TableRequests() {}

  public record Create(
      String name,
      @JsonProperty("schema") JsonNode schema,
      @JsonProperty("location") String location,
      Map<String, String> properties,
      @JsonProperty("partition-spec") JsonNode partitionSpec,
      @JsonProperty("write-order") JsonNode writeOrder,
      @JsonProperty("stage-create") Boolean stageCreate) {}

  public record Commit(
      @JsonProperty("requirements") List<Map<String, Object>> requirements,
      @JsonProperty("updates") List<Map<String, Object>> updates) {}

  public record Register(
      String name, @JsonProperty("metadata-location") String metadataLocation, Boolean overwrite) {}
}
