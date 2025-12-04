package ai.floedb.metacat.gateway.iceberg.rest.services.staging;

/** Lifecycle flags for staged table payloads. */
public enum StageState {
  STAGED,
  ABORTED
}
