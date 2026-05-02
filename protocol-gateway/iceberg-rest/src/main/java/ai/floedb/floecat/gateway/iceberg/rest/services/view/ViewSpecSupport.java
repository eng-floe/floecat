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

package ai.floedb.floecat.gateway.iceberg.rest.services.view;

import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.NamespaceRef;
import ai.floedb.floecat.gateway.iceberg.rest.services.view.ViewMetadataService.MetadataContext;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class ViewSpecSupport {
  @Inject ViewMetadataService viewMetadataService;

  public ViewSpec createSpec(
      NamespaceRef namespaceContext, String viewName, MetadataContext metadataContext) {
    ViewSpec.Builder spec =
        ViewSpec.newBuilder()
            .setCatalogId(namespaceContext.catalogId())
            .setNamespaceId(namespaceContext.namespaceId())
            .setDisplayName(viewName);
    populateCommonFields(spec, metadataContext);
    return spec.build();
  }

  public UpdateSpec updateSpec(MetadataContext metadataContext) {
    ViewSpec.Builder spec = ViewSpec.newBuilder();
    FieldMask.Builder mask =
        FieldMask.newBuilder().addPaths("sql_definitions").addPaths("properties");
    populateCommonFields(spec, metadataContext);
    var outputColumns = viewMetadataService.extractOutputColumns(metadataContext);
    if (!outputColumns.isEmpty()) {
      mask.addPaths("output_columns");
    }
    mask.addPaths("creation_search_path");
    return new UpdateSpec(spec.build(), mask.build());
  }

  private void populateCommonFields(ViewSpec.Builder spec, MetadataContext metadataContext) {
    spec.putAllProperties(viewMetadataService.buildPropertyMap(metadataContext));
    viewMetadataService.extractSqlDefinitions(metadataContext).forEach(spec::addSqlDefinitions);
    spec.addAllCreationSearchPath(viewMetadataService.extractCreationSearchPath(metadataContext));
    spec.addAllOutputColumns(viewMetadataService.extractOutputColumns(metadataContext));
  }

  public record UpdateSpec(ViewSpec spec, FieldMask updateMask) {}
}
