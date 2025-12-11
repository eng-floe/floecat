package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

import java.util.List;

public record NamespaceListResponse(List<List<String>> namespaces, String nextPageToken) {}
