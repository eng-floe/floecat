package ai.floedb.metacat.gateway.iceberg.rest;

import java.util.List;

public record NamespacesResponse(List<NamespaceDto> namespaces, PageDto page) {}
