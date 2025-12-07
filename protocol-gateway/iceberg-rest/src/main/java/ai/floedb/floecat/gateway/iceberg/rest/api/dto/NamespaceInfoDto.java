package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

import java.util.List;
import java.util.Map;

public record NamespaceInfoDto(List<String> namespace, Map<String, String> properties) {}
