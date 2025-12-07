package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import jakarta.ws.rs.core.Response;

public record MaterializeMetadataResult(
    Response error, TableMetadataView metadata, String metadataLocation) {
  public static MaterializeMetadataResult success(TableMetadataView metadata, String location) {
    return new MaterializeMetadataResult(null, metadata, location);
  }

  public static MaterializeMetadataResult failure(Response error) {
    return new MaterializeMetadataResult(error, null, null);
  }
}
