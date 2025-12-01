package ai.floedb.metacat.gateway.iceberg.rest;

/** Minimal DTOs for snapshot operations. */
public final class SnapshotRequests {
  private SnapshotRequests() {}

  public record Create(
      Long snapshotId,
      Long parentSnapshotId,
      String upstreamCreatedAt,
      String ingestedAt,
      String schemaJson) {}
}
