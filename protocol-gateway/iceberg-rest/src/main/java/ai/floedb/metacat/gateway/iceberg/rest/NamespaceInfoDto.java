package ai.floedb.metacat.gateway.iceberg.rest;

import java.util.List;
import java.util.Map;

public record NamespaceInfoDto(List<String> namespace, Map<String, String> properties) {}
