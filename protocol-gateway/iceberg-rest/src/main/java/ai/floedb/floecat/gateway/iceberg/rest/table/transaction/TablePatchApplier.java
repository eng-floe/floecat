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

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.HashSet;
import java.util.Set;

@ApplicationScoped
public class TablePatchApplier {
  public Table apply(Table current, TableSpec spec, FieldMask mask) {
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
