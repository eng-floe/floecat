package ai.floedb.floecat.service.telemetry.policy;

import ai.floedb.floecat.telemetry.profiling.ProfilingCaptureStarter;
import ai.floedb.floecat.telemetry.profiling.ProfilingConfig;
import ai.floedb.floecat.telemetry.profiling.ProfilingPolicyTrigger;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class ProfilingPolicyScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(ProfilingPolicyScheduler.class);
  private final ProfilingConfig config;
  private final ProfilingCaptureStarter captureStarter;
  private final MeterRegistry registry;
  private Instant lastLatency = Instant.EPOCH;
  private Instant lastQueue = Instant.EPOCH;
  private Instant lastGc = Instant.EPOCH;
  private int consecutiveLatencyViolations = 0;
  private static final int LATENCY_VIOLATION_TARGET = 2;

  @Inject
  public ProfilingPolicyScheduler(
      ProfilingConfig config, ProfilingCaptureStarter captureStarter, MeterRegistry registry) {
    this.config = config;
    this.captureStarter = captureStarter;
    this.registry = registry;
  }

  @Scheduled(every = "${floecat.profiling.policy-poll-interval:PT30S}")
  void evaluatePolicies() {
    if (!config.enabled() || !config.policy().enabled()) {
      return;
    }
    checkLatencyPolicy();
    checkQueuePolicy();
    checkGcPolicy();
  }

  private void checkLatencyPolicy() {
    ProfilingConfig.LatencyPolicy policy = config.policy().latency();
    if (!policy.enabled()) {
      return;
    }
    if (!cooldownPassed(lastLatency, policy.cooldown())) {
      return;
    }
    Timer timer = registry.find("floecat.core.rpc.latency").timer();
    if (timer == null) {
      return;
    }
    double observedMs = observedLatencyMs(timer);
    double thresholdMs = policy.threshold().toMillis();
    if (observedMs <= thresholdMs) {
      consecutiveLatencyViolations = 0;
      return;
    }
    consecutiveLatencyViolations++;
    if (consecutiveLatencyViolations < LATENCY_VIOLATION_TARGET) {
      return;
    }
    triggerCapture(
        "latency_threshold", "rpc_latency_ms", observedMs, thresholdMs, policy.window(), policy);
    lastLatency = Instant.now();
    consecutiveLatencyViolations = 0;
  }

  private void checkQueuePolicy() {
    ProfilingConfig.ExecutorQueuePolicy policy = config.policy().executorQueue();
    if (!policy.enabled()) {
      return;
    }
    if (!cooldownPassed(lastQueue, policy.cooldown())) {
      return;
    }
    Gauge gauge =
        registry.find("floecat.core.exec.queue.depth").tag("pool", policy.poolName()).gauge();
    if (gauge == null) {
      return;
    }
    double depth = gauge.value();
    if (depth <= policy.threshold()) {
      return;
    }
    triggerCapture(
        "queue_depth", "executor_queue", depth, policy.threshold(), policy.window(), policy);
    lastQueue = Instant.now();
  }

  private void checkGcPolicy() {
    ProfilingConfig.GcPolicy policy = config.policy().gc();
    if (!policy.enabled()) {
      return;
    }
    if (!cooldownPassed(lastGc, policy.cooldown())) {
      return;
    }
    Gauge gauge =
        registry.find("floecat.jvm.gc.live.data.bytes").tag("gc", policy.gcName()).gauge();
    if (gauge == null) {
      return;
    }
    double used = gauge.value();
    if (used <= policy.thresholdBytes()) {
      return;
    }
    triggerCapture(
        "gc_pressure", "gc_live_bytes", used, policy.thresholdBytes(), policy.window(), policy);
    lastGc = Instant.now();
  }

  private boolean cooldownPassed(Instant last, Duration cooldown) {
    return Instant.now().isAfter(last.plus(cooldown));
  }

  private double observedLatencyMs(Timer timer) {
    HistogramSnapshot snapshot = timer.takeSnapshot();
    ValueAtPercentile[] values = snapshot.percentileValues();
    if (values != null) {
      double target = 0.95;
      ValueAtPercentile closest = null;
      double bestDiff = Double.MAX_VALUE;
      for (ValueAtPercentile value : values) {
        double diff = Math.abs(value.percentile() - target);
        if (diff < bestDiff) {
          bestDiff = diff;
          closest = value;
        }
        if (diff == 0) {
          break;
        }
      }
      if (closest != null) {
        return closest.value(TimeUnit.MILLISECONDS);
      }
    }
    return timer.max(TimeUnit.MILLISECONDS);
  }

  private void triggerCapture(
      String policyName,
      String signal,
      double value,
      double threshold,
      Duration window,
      ProfilingConfig.PolicySpec policy) {
    try {
      ProfilingPolicyTrigger trigger =
          new ProfilingPolicyTrigger(policyName, signal, value, threshold, window);
      captureStarter.startCapture(
          "policy", config.captureDuration(), "jfr", "policy", "policy/" + policyName, trigger);
    } catch (Exception e) {
      LOG.warn("policy {} could not start capture", policyName, e);
    }
  }
}
