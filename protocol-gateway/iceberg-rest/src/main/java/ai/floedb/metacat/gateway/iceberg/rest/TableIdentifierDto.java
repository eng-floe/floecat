package ai.floedb.metacat.gateway.iceberg.rest;

import java.util.List;

/** Minimal Iceberg-friendly table identifier. */
public record TableIdentifierDto(List<String> namespace, String name) {}
