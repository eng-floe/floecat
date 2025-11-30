package ai.floedb.metacat.gateway.iceberg.rest;

public record IcebergError(String message, String type, int code) {}
