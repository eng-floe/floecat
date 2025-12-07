package ai.floedb.floecat.gateway.iceberg.rest.api.error;

public record IcebergError(String message, String type, int code) {}
