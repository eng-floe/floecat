package ai.floedb.metacat.gateway.iceberg.rest.api.request;

import java.util.List;
import java.util.Map;

public record NamespacePropertiesRequest(List<String> removals, Map<String, String> updates) {}
