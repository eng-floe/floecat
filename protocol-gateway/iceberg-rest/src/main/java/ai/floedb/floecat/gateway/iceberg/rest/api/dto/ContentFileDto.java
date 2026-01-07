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

package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ContentFileDto(
    String content,
    @JsonProperty("file-path") String filePath,
    @JsonProperty("file-format") String fileFormat,
    @JsonProperty("spec-id") Integer specId,
    @JsonProperty("partition") List<Object> partition,
    @JsonProperty("file-size-in-bytes") Long fileSizeInBytes,
    @JsonProperty("record-count") Long recordCount,
    @JsonProperty("key-metadata") String keyMetadata,
    @JsonProperty("split-offsets") List<Long> splitOffsets,
    @JsonProperty("sort-order-id") Integer sortOrderId,
    @JsonProperty("equality-ids") List<Integer> equalityIds) {}
