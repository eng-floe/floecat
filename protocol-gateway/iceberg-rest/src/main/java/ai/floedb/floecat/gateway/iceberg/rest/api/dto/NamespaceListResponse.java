package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record NamespaceListResponse(
    List<List<String>> namespaces, @JsonProperty("next-page-token") String nextPageToken) {}
