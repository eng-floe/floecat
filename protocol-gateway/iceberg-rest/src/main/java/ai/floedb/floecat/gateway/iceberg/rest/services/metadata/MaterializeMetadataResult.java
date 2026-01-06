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

package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import jakarta.ws.rs.core.Response;

public record MaterializeMetadataResult(
    Response error, TableMetadataView metadata, String metadataLocation) {
  public static MaterializeMetadataResult success(TableMetadataView metadata, String location) {
    return new MaterializeMetadataResult(null, metadata, location);
  }

  public static MaterializeMetadataResult failure(Response error) {
    return new MaterializeMetadataResult(error, null, null);
  }
}
