package ai.floedb.metacat.gateway.iceberg.rest.api.dto;

import java.util.List;

public record TableIdentifierDto(List<String> namespace, String name) {}
