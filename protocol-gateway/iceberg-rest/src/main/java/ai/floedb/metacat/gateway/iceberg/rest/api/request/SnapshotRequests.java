package ai.floedb.metacat.gateway.iceberg.rest.api.request;

public final class SnapshotRequests {
  private SnapshotRequests() {}

  public record Create(
      Long snapshotId,
      Long parentSnapshotId,
      String upstreamCreatedAt,
      String ingestedAt,
      String schemaJson) {}
}
