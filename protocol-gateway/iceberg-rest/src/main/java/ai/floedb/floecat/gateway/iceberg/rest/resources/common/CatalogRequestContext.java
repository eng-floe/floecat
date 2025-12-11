package ai.floedb.floecat.gateway.iceberg.rest.resources.common;

import ai.floedb.floecat.common.rpc.ResourceId;

public record CatalogRequestContext(String prefix, String catalogName, ResourceId catalogId) {}
