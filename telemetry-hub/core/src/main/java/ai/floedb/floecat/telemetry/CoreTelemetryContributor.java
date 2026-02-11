package ai.floedb.floecat.telemetry;

/** Registers the core metric definitions into {@link TelemetryRegistry}. */
public final class CoreTelemetryContributor implements TelemetryContributor {

  @Override
  public void contribute(TelemetryRegistry registry) {
    Telemetry.Metrics.definitions().values().forEach(registry::register);
  }
}
