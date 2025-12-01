package ai.floedb.metacat.gateway.iceberg.rest;

import ai.floedb.metacat.catalog.rpc.View;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class ViewResponseMapper {
  private ViewResponseMapper() {}

  static LoadViewResultDto toLoadResult(String namespacePath, String viewName, View view) {
    Map<String, String> props = new LinkedHashMap<>(view.getPropertiesMap());
    String metadataLocation =
        props.getOrDefault(
            "metadata-location", props.getOrDefault("location", "metacat://" + viewName));
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
            0, timestamp, 0, Map.of("operation", "sql"), List.of(representation), namespace);
    ViewMetadataView.ViewHistoryEntry history = new ViewMetadataView.ViewHistoryEntry(0, timestamp);
    ViewMetadataView.SchemaSummary schema =
        new ViewMetadataView.SchemaSummary(0, "struct", List.of(), List.of());
    ViewMetadataView metadata =
        new ViewMetadataView(
            view.hasResourceId() ? view.getResourceId().getId() : viewName,
            1,
            metadataLocation,
            0,
            List.of(version),
            List.of(history),
            List.of(schema),
            props);
    return new LoadViewResultDto(metadataLocation, metadata, Map.of());
  }
}
