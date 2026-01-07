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

package ai.floedb.floecat.gateway.iceberg.rest.common;

import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadViewResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.ViewMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NamespacePaths;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ViewResponseMapper {
  private ViewResponseMapper() {}

  public static LoadViewResultDto toLoadResult(
      String namespacePath, String viewName, View view, ViewMetadataView storedMetadata) {
    Map<String, String> props = new LinkedHashMap<>(view.getPropertiesMap());
    String metadataLocation = props.get("metadata-location");

    ViewMetadataView metadata = storedMetadata;
    if (metadata == null) {
      metadata = synthesizeMetadata(namespacePath, viewName, view, props);
      metadataLocation = metadata.location();
    } else if (metadataLocation == null || metadataLocation.isBlank()) {
      metadataLocation = metadata.location();
    } else if (metadata.location() == null || metadata.location().isBlank()) {
      metadata =
          new ViewMetadataView(
              metadata.viewUuid(),
              metadata.formatVersion(),
              metadataLocation,
              metadata.currentVersionId(),
              metadata.versions(),
              metadata.versionLog(),
              metadata.schemas(),
              metadata.properties());
    }
    if (metadataLocation == null || metadataLocation.isBlank()) {
      metadataLocation = "floecat://" + viewName;
    }
    return new LoadViewResultDto(metadataLocation, metadata, Map.of());
  }

  private static ViewMetadataView synthesizeMetadata(
      String namespacePath, String viewName, View view, Map<String, String> props) {
    String metadataLocation =
        props.getOrDefault(
            "metadata-location", props.getOrDefault("location", "floecat://" + viewName));
    Long timestamp =
        view.hasCreatedAt()
            ? view.getCreatedAt().getSeconds() * 1000 + view.getCreatedAt().getNanos() / 1_000_000
            : Instant.now().toEpochMilli();
    List<String> namespace = NamespacePaths.split(namespacePath);
    ViewMetadataView.ViewRepresentation representation =
        new ViewMetadataView.ViewRepresentation(
            "sql", view.getSql(), props.getOrDefault("dialect", "ansi"));
    ViewMetadataView.ViewVersion version =
        new ViewMetadataView.ViewVersion(
            0,
            timestamp,
            0,
            Map.of("operation", "sql"),
            List.of(representation),
            namespace,
            props.get("default-catalog"));
    ViewMetadataView.ViewHistoryEntry history = new ViewMetadataView.ViewHistoryEntry(0, timestamp);
    ViewMetadataView.SchemaSummary schema =
        new ViewMetadataView.SchemaSummary(0, "struct", List.of(), List.of());
    return new ViewMetadataView(
        view.hasResourceId() ? view.getResourceId().getId() : viewName,
        1,
        metadataLocation,
        0,
        List.of(version),
        List.of(history),
        List.of(schema),
        props);
  }
}
