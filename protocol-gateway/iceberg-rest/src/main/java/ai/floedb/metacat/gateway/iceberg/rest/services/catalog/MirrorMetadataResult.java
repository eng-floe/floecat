package ai.floedb.metacat.gateway.iceberg.rest.services.catalog;

import ai.floedb.metacat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import jakarta.ws.rs.core.Response;

public record MirrorMetadataResult(
    Response error, TableMetadataView metadata, String metadataLocation) {
  public static MirrorMetadataResult success(TableMetadataView metadata, String location) {
    return new MirrorMetadataResult(null, metadata, location);
  }

  public static MirrorMetadataResult failure(Response error) {
    return new MirrorMetadataResult(error, null, null);
  }
}
