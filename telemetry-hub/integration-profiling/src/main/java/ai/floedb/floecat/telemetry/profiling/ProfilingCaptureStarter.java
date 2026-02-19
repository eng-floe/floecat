package ai.floedb.floecat.telemetry.profiling;

import java.time.Duration;

/** Allows callers to request a profiling capture with optional policy details. */
public interface ProfilingCaptureStarter {
  CaptureMetadata startCapture(
      String trigger,
      Duration requestedDuration,
      String mode,
      String scope,
      String requestedBy,
      ProfilingPolicyTrigger policyTrigger);
}
