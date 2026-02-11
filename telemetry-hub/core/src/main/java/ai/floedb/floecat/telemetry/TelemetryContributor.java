package ai.floedb.floecat.telemetry;

/** Allows modules to register metric definitions into the shared {@link TelemetryRegistry}. */
@FunctionalInterface
public interface TelemetryContributor {

  /** Register metrics into the provided registry. */
  void contribute(TelemetryRegistry registry);
}
